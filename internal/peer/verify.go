// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

func uniqueDescriptors(im *image.Image) []v1.Descriptor {
	seen := map[digest.Digest]bool{}
	var result []v1.Descriptor
	for _, d := range im.Descriptors {
		if !seen[d.Digest] {
			result = append(result, d)
			seen[d.Digest] = true
		}
	}
	// Start the largest blobs first; one large final layer otherwise hides the
	// available concurrency. This does not split a blob across connections.
	sort.SliceStable(result, func(i, j int) bool { return result[i].Size > result[j].Size })
	return result
}

// Verify downloads and hashes each unique descriptor using the same bounded
// receiver cache as Docker import. It never connects to a receiver Docker daemon.
func Verify(ctx context.Context, c *Client, o PullOptions) (result Result, err error) {
	started := time.Now()
	result = Result{Version: ProtocolVersion, Transfer: c.Credentials.Transfer, Peer: c.Credentials.Peer, State: "FAILED", ImportMethod: "none"}
	defer func() {
		result.TransferSeconds = time.Since(started).Seconds()
		result.Paths = c.PathMetrics()
		if err != nil {
			result.Error = err.Error()
			if ctx.Err() != nil {
				result.State = "CANCELLED"
			}
		}
	}()
	if o.Memory == 0 {
		o.Memory = 128 << 20
	}
	if o.Parallel == 0 {
		o.Parallel = 4
	}
	if o.Retries < 0 || o.Retries > 10 {
		return result, errors.New("invalid retry count")
	}
	im, err := c.Image(ctx)
	if err != nil {
		return result, err
	}
	result.Manifest = string(im.Digest)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cache, err := transfer.New(workCtx, c, o.Memory, o.Parallel)
	if err != nil {
		return result, err
	}
	defer func() { cache.Close(); result.Metrics = cache.Metrics() }()
	descriptors := uniqueDescriptors(im)
	jobs := make(chan v1.Descriptor, len(descriptors))
	for _, d := range descriptors {
		jobs <- d
	}
	close(jobs)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range min(o.Parallel, len(descriptors)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				var failure error
				for attempt := 0; attempt <= o.Retries; attempt++ {
					reader, openErr := cache.Open(workCtx, d)
					failure = openErr
					if failure == nil {
						_, failure = io.Copy(io.Discard, reader)
						_ = reader.Close()
					}
					var network *pathError
					if failure == nil || workCtx.Err() != nil || (!errors.As(failure, &network) && !errors.Is(failure, transfer.ErrLagged)) {
						break
					}
				}
				if failure != nil {
					once.Do(func() { firstErr = failure; cancel() })
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return result, firstErr
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	result.State = "VERIFIED"
	return result, nil
}

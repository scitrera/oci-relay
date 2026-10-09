// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
)

var errDecodeBudget = errors.New("receiver decoder scratch budget exhausted")
var errDecodeUnavailable = errors.New("receiver decoder unavailable")

type DecodeOptions struct {
	Mode         string
	Helper       string
	Directory    string
	Workers      int
	MaxBytes     int64
	ReserveBytes int64
}

func (o DecodeOptions) validate() error {
	if o.Mode == "" || o.Mode == "none" {
		return nil
	}
	if o.Mode != "auto" && o.Mode != "unpigz" {
		return errors.New("decoder must be none, auto or unpigz")
	}
	if !filepath.IsAbs(o.Helper) || !filepath.IsAbs(o.Directory) || o.Workers < 1 || o.Workers > 16 || o.MaxBytes < 4<<20 || o.MaxBytes > 1<<50 || o.ReserveBytes < 0 || o.ReserveBytes > 1<<50 {
		return errors.New("decoder requires absolute helper/spool paths, 1..16 workers and explicit 4 MiB..1 PiB scratch budget")
	}
	return nil
}

type DecodeMetrics struct {
	Seconds  float64 `json:"seconds"`
	Bytes    int64   `json:"bytes"`
	Layers   int     `json:"layers"`
	Fallback string  `json:"fallback,omitempty"`
}

type decodedLayers struct {
	image        *image.Image
	files        map[digest.Digest]string
	availability []string
	root         string
	metrics      DecodeMetrics
}

func (d *decodedLayers) close() error {
	if d.root == "" {
		return nil
	}
	return os.RemoveAll(d.root)
}

type decodeWriter struct {
	ctx        context.Context
	file       *os.File
	used       *atomic.Int64
	options    DecodeOptions
	sinceCheck int64
	err        error
}

func (w *decodeWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	used := w.used.Add(int64(len(p)))
	if used > w.options.MaxBytes {
		w.used.Add(-int64(len(p)))
		w.err = errDecodeBudget
		return 0, w.err
	}
	w.sinceCheck += int64(len(p))
	if w.sinceCheck >= 64<<20 {
		w.sinceCheck = 0
		free, err := availableDisk(w.options.Directory)
		if err != nil {
			w.used.Add(-int64(len(p)))
			w.err = err
			return 0, err
		}
		// Keep room for Docker's download copy and unpacked data, besides scratch.
		if free < w.options.ReserveBytes+2*used {
			w.used.Add(-int64(len(p)))
			w.err = errDecodeBudget
			return 0, w.err
		}
	}
	n, err := w.file.Write(p)
	w.used.Add(int64(n - len(p)))
	w.err = err
	return n, err
}

func prepareDecoded(ctx context.Context, im *image.Image, cache *transfer.Cache, availability []string, o DecodeOptions) (out *decodedLayers, err error) {
	out = &decodedLayers{image: im, files: map[digest.Digest]string{}, availability: append([]string(nil), availability...)}
	if err = o.validate(); err != nil {
		return
	}
	if o.Mode == "" || o.Mode == "none" {
		return
	}
	start := time.Now()
	defer func() { out.metrics.Seconds = time.Since(start).Seconds() }()
	// Fallback is limited to resource/availability failures. Integrity failures
	// are always fatal, even when automatic selection was requested.
	defer func() {
		if err != nil {
			cleanupErr := out.close()
			if o.Mode == "auto" && ctx.Err() == nil && cleanupErr == nil && (errors.Is(err, errDecodeBudget) || errors.Is(err, errDecodeUnavailable)) {
				out.metrics.Fallback = err.Error()
				out.image, out.files, out.root = im, map[digest.Digest]string{}, ""
				out.availability = append([]string(nil), availability...)
				err = nil
			} else {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	if err = im.Validate(); err != nil {
		return
	}
	var manifest v1.Manifest
	var config v1.Image
	_ = json.Unmarshal(im.Manifest, &manifest)
	_ = json.Unmarshal(im.Config, &config)
	if len(availability) != len(manifest.Layers) {
		err = errors.New("decoder availability count mismatch")
		return
	}
	type job struct {
		descriptor v1.Descriptor
		diff       digest.Digest
		indexes    []int
		size       int64
		path       string
	}
	jobs := []*job{}
	seen := map[digest.Digest]*job{}
	for i, d := range manifest.Layers {
		if availability[i] != layerMissing || (d.MediaType != v1.MediaTypeImageLayerGzip && d.MediaType != "application/vnd.docker.image.rootfs.diff.tar.gzip") {
			continue
		}
		if old := seen[d.Digest]; old != nil {
			if old.diff != config.RootFS.DiffIDs[i] {
				err = errors.New("conflicting gzip DiffIDs")
				return
			}
			old.indexes = append(old.indexes, i)
		} else {
			j := &job{descriptor: d, diff: config.RootFS.DiffIDs[i], indexes: []int{i}}
			jobs = append(jobs, j)
			seen[d.Digest] = j
		}
	}
	if len(jobs) == 0 {
		return
	}
	helper, e := os.Stat(o.Helper)
	if e != nil || !helper.Mode().IsRegular() || helper.Mode().Perm()&0111 == 0 {
		err = fmt.Errorf("%w: executable helper", errDecodeUnavailable)
		return
	}
	free, e := availableDisk(o.Directory)
	if e != nil {
		err = fmt.Errorf("%w: disk headroom: %v", errDecodeUnavailable, e)
		return
	}
	if free < 3*o.MaxBytes+o.ReserveBytes {
		err = errDecodeBudget
		return
	}
	out.root, err = os.MkdirTemp(o.Directory, "decode-")
	if err != nil {
		return
	}
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	var used atomic.Int64
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	queue := make(chan *job)
	for n := 0; n < min(o.Workers, len(jobs)); n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range queue {
				if work.Err() != nil {
					continue
				}
				size, e := decodeLayer(work, cache, j.descriptor, j.diff, out.root, o, &used)
				if e != nil {
					once.Do(func() { first = e; cancel() })
					continue
				}
				j.size, j.path = size, filepath.Join(out.root, j.diff.Encoded())
			}
		}()
	}
	for _, j := range jobs {
		select {
		case queue <- j:
		case <-work.Done():
		}
	}
	close(queue)
	wg.Wait()
	out.metrics.Bytes = used.Load()
	if first != nil {
		err = first
		return
	}
	if err = ctx.Err(); err != nil {
		return
	}
	for _, j := range jobs {
		out.files[j.diff] = j.path
		for _, i := range j.indexes {
			manifest.Layers[i] = v1.Descriptor{MediaType: v1.MediaTypeImageLayer, Digest: j.diff, Size: j.size}
			out.availability[i] = layerBlob
			out.metrics.Layers++
		}
	}
	manifest.MediaType = v1.MediaTypeImageManifest
	raw, e := json.Marshal(manifest)
	if e != nil {
		err = e
		return
	}
	out.image, err = image.Parse(raw, im.Config, "", im.Platform)
	return
}

func decodeLayer(ctx context.Context, cache *transfer.Cache, d v1.Descriptor, diff digest.Digest, root string, o DecodeOptions, used *atomic.Int64) (int64, error) {
	reader, err := cache.Open(ctx, d)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	f, err := os.CreateTemp(root, "partial-")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	defer os.Remove(f.Name())
	hash := digest.SHA256.Digester()
	writer := &decodeWriter{ctx: ctx, file: f, used: used, options: o}
	cmd := exec.CommandContext(ctx, o.Helper, "-d", "-c")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GZIP=") && !strings.HasPrefix(entry, "PIGZ=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Stdin = reader
	cmd.Stdout = io.MultiWriter(writer, hash.Hash())
	// Decoder diagnostics are not payload data; discard rather than retaining
	// unbounded stderr or exposing content supplied by a failed helper.
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	if err = cmd.Run(); err != nil {
		if writer.err != nil {
			return 0, writer.err
		}
		return 0, fmt.Errorf("decode %s: %w", d.Digest, err)
	}
	// Observe the cache's compressed digest verification even when gzip reaches
	// its logical end before the subprocess stdin pump sees the final error.
	if n, e := io.Copy(io.Discard, reader); e != nil || n != 0 {
		return 0, errors.Join(e, fmt.Errorf("unexpected compressed input tail: %d", n))
	}
	if hash.Digest() != diff {
		return 0, fmt.Errorf("decoded DiffID mismatch for %s", d.Digest)
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if err = f.Close(); err != nil {
		return 0, err
	}
	if err = os.Rename(f.Name(), filepath.Join(root, diff.Encoded())); err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Registry uses these private, already-verified files without another relay
// hash pass. Docker still verifies the raw descriptor and ordered DiffID.
func (d *decodedLayers) registry(cache *transfer.Cache, repository string) http.Handler {
	reg := NewRegistry(d.image, cache, repository)
	reg.files = d.files
	return reg
}

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/fileio"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/transfer"
)

type RegistryOptions struct {
	Reference           string
	Platform            v1.Platform
	ConfigPath          string
	PlainHTTP           bool  // Explicit opt-in for a trusted plain-HTTP registry; never disables HTTPS verification.
	MaxCacheBytes       int64 // Optional retained compressed blobs, separate from transfer cache memory.
	SpoolDir            string
	Transport           *http.Transport      // Optional verified CA/proxy customization for embedded callers.
	Credentials         *RegistryCredentials // Optional in-memory credentials; nil loads Docker config on this host.
	RangeConcurrency    int                  // 0 defaults to 4 requests per blob; 1 disables ranges.
	RangeChunkBytes     int64                // 0 defaults to 16 MiB.
	RangeThresholdBytes int64                // 0 defaults to 256 MiB.
	RangeBufferBytes    int64                // Shared range read-ahead budget; 0 defaults to 128 MiB.
}

type RegistryMetrics struct {
	UpstreamBytes          int64 `json:"upstream_blob_bytes"`
	Downloads              int64 `json:"blob_downloads"`
	CacheHits              int64 `json:"disk_cache_hits"`
	CacheBypasses          int64 `json:"disk_cache_bypasses"`
	CacheWriteErrors       int64 `json:"disk_cache_write_errors"`
	CacheReservedBytes     int64 `json:"disk_cache_reserved_bytes"`
	PeakCacheReservedBytes int64 `json:"peak_disk_cache_reserved_bytes"`
	MetadataBytes          int64 `json:"metadata_bytes"`
	SharedVerifications    int64 `json:"shared_sha256_verifications"`
	LocalVerifications     int64 `json:"local_sha256_verifications"`
	RangeDownloads         int64 `json:"range_downloads"`
	RangeRequests          int64 `json:"range_requests"`
	RangeFallbacks         int64 `json:"range_fallbacks"`
	ActiveRangeRequests    int64 `json:"active_range_requests"`
	PeakRangeRequests      int64 `json:"peak_range_requests"`
	RangeBufferBytes       int64 `json:"range_buffer_bytes"`
	PeakRangeBufferBytes   int64 `json:"peak_range_buffer_bytes"`
}

type registryBlob struct {
	done        chan struct{}
	path        string
	err         error
	cacheFailed bool
}

type Registry struct {
	Image                                         *image.Image
	RootDigest                                    digest.Digest
	PreparationSeconds                            float64
	http                                          *registryHTTP
	transport                                     *http.Transport
	ctx                                           context.Context
	cancel                                        context.CancelFunc
	mu                                            sync.Mutex
	closed                                        bool
	wg                                            sync.WaitGroup
	blobs                                         map[digest.Digest]*registryBlob
	dir                                           string
	budget, reserved, peak                        int64
	upstream, downloads, hits, bypasses, metadata atomic.Int64
	cacheErrors                                   atomic.Int64
	sharedVerifications, localVerifications       atomic.Int64
	ranges                                        registryRanges
}

func NewRegistry(parent context.Context, o RegistryOptions) (_ *Registry, returnErr error) {
	start := time.Now()
	if o.MaxCacheBytes < 0 || o.MaxCacheBytes > 1<<50 {
		return nil, errors.New("invalid registry disk cache budget")
	}
	ref, err := parseRegistryReference(o.Reference, o.PlainHTTP)
	if err != nil {
		return nil, errors.New("invalid registry image reference")
	}
	if strings.Contains(ref.Reference, ":") && !strings.HasPrefix(ref.Reference, "sha256:") {
		return nil, errors.New("registry manifest requires SHA-256")
	}
	if o.Platform.OS == "" {
		o.Platform = v1.Platform{OS: "linux", Architecture: runtime.GOARCH}
	}
	ctx, cancel := context.WithCancel(parent)
	r := &Registry{ctx: ctx, cancel: cancel, budget: o.MaxCacheBytes, blobs: map[digest.Digest]*registryBlob{}}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, r.Close())
		}
	}()
	if err = r.ranges.configure(o); err != nil {
		return nil, err
	}
	var creds RegistryCredentials
	if o.Credentials != nil {
		creds = *o.Credentials
	} else {
		creds, err = DockerRegistryCredentials(ctx, ref.Registry, o.ConfigPath)
		if err != nil {
			return nil, err
		}
	}
	r.transport = registryTransport()
	if o.Transport != nil {
		r.transport = o.Transport.Clone()
		r.transport.DisableCompression = true
	}
	// The source never accepts a transport that weakens server identity checks.
	if r.transport.TLSClientConfig != nil && r.transport.TLSClientConfig.InsecureSkipVerify {
		return nil, errors.New("registry TLS verification is required")
	}
	client := &http.Client{Transport: r.transport, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("registry redirect limit exceeded")
		}
		if req.URL.User != nil || (req.URL.Scheme != "https" && !(o.PlainHTTP && req.URL.Scheme == "http")) {
			return errors.New("unsafe registry redirect")
		}
		if !sameOrigin(req.URL, via[0].URL) {
			req.Header.Del("Authorization")
			req.Header.Del("Cookie")
		}
		return nil
	}}
	r.http = &registryHTTP{ref: ref, client: client, credentials: creds, plainHTTP: o.PlainHTTP, authGate: make(chan struct{}, 1)}
	if creds.RegistryToken != "" {
		r.http.authorization = "Bearer " + creds.RegistryToken
		r.http.expires = time.Now().Add(time.Hour)
	}
	if r.budget > 0 {
		r.dir, err = os.MkdirTemp(o.SpoolDir, "oci-relay-registry-")
		if err != nil {
			r.budget = 0
			r.cacheErrors.Add(1)
		}
	}
	var expected digest.Digest
	if strings.HasPrefix(ref.Reference, "sha256:") {
		expected = digest.Digest(ref.Reference)
	}
	raw, root, err := r.manifest(ctx, ref.Reference, expected, -1)
	if err != nil {
		return nil, err
	}
	r.RootDigest = root
	selected, selectedDigest, err := r.resolve(ctx, raw, root, o.Platform, 0, map[digest.Digest]bool{}, new(int))
	if err != nil {
		return nil, err
	}
	var manifest v1.Manifest
	if json.Unmarshal(selected, &manifest) != nil || manifest.Config.Size < 0 || manifest.Config.Size > image.MaxMetadata {
		return nil, errors.New("invalid registry image manifest/config size")
	}
	if err = image.ValidateDescriptor(manifest.Config); err != nil {
		return nil, err
	}
	configResp, err := r.http.get(ctx, ref.BlobURL(manifest.Config.Digest.String()), "")
	if err != nil {
		return nil, fmt.Errorf("registry config: %w", err)
	}
	config, err := readRegistryMetadata(configResp, image.MaxMetadata)
	if err != nil {
		return nil, err
	}
	r.metadata.Add(int64(len(config)))
	r.Image, err = image.Parse(selected, config, selectedDigest, o.Platform)
	if err != nil {
		return nil, err
	}
	eligible := false
	for _, layer := range r.Image.Descriptors[1:] {
		eligible = eligible || (layer.Size >= r.ranges.threshold && layer.Size > r.ranges.chunk)
	}
	if !eligible {
		// Small-image operations need no read-ahead reservation at all.
		r.ranges.streams, r.ranges.budget = 1, 0
	}
	r.PreparationSeconds = time.Since(start).Seconds()
	return r, nil
}

const registryManifestAccept = v1.MediaTypeImageIndex + ", " + image.DockerIndex + ", " + v1.MediaTypeImageManifest + ", " + image.DockerManifest

func (r *Registry) manifest(ctx context.Context, reference string, expected digest.Digest, size int64) ([]byte, digest.Digest, error) {
	ref := *r.http.ref
	ref.Reference = reference
	resp, err := r.http.get(ctx, ref.ManifestURL(), registryManifestAccept)
	if err != nil {
		return nil, "", err
	}
	header := resp.Header.Get("Docker-Content-Digest")
	raw, err := readRegistryMetadata(resp, image.MaxMetadata)
	if err != nil {
		return nil, "", err
	}
	r.metadata.Add(int64(len(raw)))
	actual := digest.FromBytes(raw)
	if (expected != "" && actual != expected) || (header != "" && header != actual.String()) || (size >= 0 && int64(len(raw)) != size) {
		return nil, "", errors.New("registry manifest digest or size mismatch")
	}
	return raw, actual, nil
}

func (r *Registry) resolve(ctx context.Context, raw []byte, dg digest.Digest, platform v1.Platform, depth int, seen map[digest.Digest]bool, count *int) ([]byte, digest.Digest, error) {
	if depth > 8 || *count >= image.MaxDescriptors {
		return nil, "", errors.New("registry index resolution exceeds limits")
	}
	if seen[dg] {
		return nil, "", errors.New("registry index repeats a manifest")
	}
	seen[dg] = true
	*count++
	var header struct {
		SchemaVersion int             `json:"schemaVersion"`
		MediaType     string          `json:"mediaType"`
		Subject       json.RawMessage `json:"subject"`
		ArtifactType  string          `json:"artifactType"`
	}
	if json.Unmarshal(raw, &header) != nil || header.SchemaVersion != 2 || (len(header.Subject) > 0 && string(header.Subject) != "null") || header.ArtifactType != "" {
		return nil, "", errors.New("expected registry image manifest or index")
	}
	if header.MediaType == v1.MediaTypeImageManifest || header.MediaType == image.DockerManifest {
		return raw, dg, nil
	}
	if header.MediaType != v1.MediaTypeImageIndex && header.MediaType != image.DockerIndex {
		return nil, "", errors.New("unsupported registry manifest media type")
	}
	var idx v1.Index
	if json.Unmarshal(raw, &idx) != nil || len(idx.Manifests) > image.MaxDescriptors {
		return nil, "", errors.New("invalid or oversized registry index")
	}
	var candidates []v1.Descriptor
	for _, d := range idx.Manifests {
		if err := image.ValidateDescriptor(d); err != nil {
			return nil, "", err
		}
		if d.Size > image.MaxMetadata {
			return nil, "", errors.New("registry child metadata exceeds limit")
		}
		if d.Platform != nil && !image.Matches(platform, *d.Platform) {
			continue
		}
		if d.MediaType != v1.MediaTypeImageManifest && d.MediaType != image.DockerManifest && d.MediaType != v1.MediaTypeImageIndex && d.MediaType != image.DockerIndex {
			continue
		}
		candidates = append(candidates, d)
	}
	if len(candidates) != 1 {
		return nil, "", fmt.Errorf("registry index requires one matching platform descriptor, got %d", len(candidates))
	}
	d := candidates[0]
	child, actual, err := r.manifest(ctx, d.Digest.String(), d.Digest, d.Size)
	if err != nil {
		return nil, "", err
	}
	return r.resolve(ctx, child, actual, platform, depth+1, seen, count)
}

func (r *Registry) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	known, ok := r.Image.Descriptor(d.Digest)
	if !ok || known.Size != d.Size {
		return errors.New("descriptor is not in the pinned registry image")
	}
	// Capture ownership before optional disk retention wraps the writer. Only
	// transfer's private producer/pipeline writers can supply this barrier.
	verify := transfer.Verifier(w, d)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("registry source closed")
	}
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Digest == r.Image.Descriptors[0].Digest {
		_, err := w.Write(r.Image.Config)
		return err
	}
	r.mu.Lock()
	if blob, ok := r.blobs[d.Digest]; ok {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blob.done:
		}
		if blob.err != nil {
			return blob.err
		}
		if blob.cacheFailed {
			r.bypasses.Add(1)
			return r.download(ctx, d, w, verify)
		}
		f, err := os.Open(blob.path)
		if err != nil {
			return err
		}
		reader := fileio.ReadFile(f)
		defer reader.Close()
		r.hits.Add(1)
		dst, finish := r.verifyingWriter(d, w, verify)
		n, err := io.CopyBuffer(&contextWriter{ctx: ctx, w: dst}, io.LimitReader(reader, d.Size+1), make([]byte, 64<<10))
		if err != nil {
			return err
		}
		return finish(n)
	}
	var blob *registryBlob
	if r.dir != "" && r.budget > 0 && d.Size <= r.budget-r.reserved {
		blob = &registryBlob{done: make(chan struct{}), path: filepath.Join(r.dir, d.Digest.Encoded())}
		r.blobs[d.Digest] = blob
		r.reserved += d.Size
		r.peak = max(r.peak, r.reserved)
	} else {
		r.bypasses.Add(1)
	}
	r.mu.Unlock()
	if blob == nil {
		return r.download(ctx, d, w, verify)
	}
	f, cacheErr := os.OpenFile(blob.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	// Retention is an optimization: disk exhaustion must not break a healthy
	// upstream stream. Receiver errors and upstream integrity failures still fail.
	sink := &optionalCacheWriter{file: f, err: cacheErr}
	err := r.download(ctx, d, io.MultiWriter(w, sink), verify)
	if f != nil {
		sink.err = errors.Join(sink.err, f.Close())
	}
	r.mu.Lock()
	if err != nil || sink.err != nil {
		_ = os.Remove(blob.path)
		delete(r.blobs, d.Digest)
		r.reserved -= d.Size
	}
	if sink.err != nil {
		r.cacheErrors.Add(1)
		blob.cacheFailed = true
		// Stop trying to fill a filesystem that cannot accept cache writes.
		r.budget = 0
	}
	blob.err = err
	close(blob.done)
	r.mu.Unlock()
	return err
}

type optionalCacheWriter struct {
	file io.Writer
	err  error
}

func (w *optionalCacheWriter) Write(p []byte) (int, error) {
	if w.err == nil {
		var n int
		n, w.err = w.file.Write(p)
		if n != len(p) && w.err == nil {
			w.err = io.ErrShortWrite
		}
	}
	return len(p), nil
}

// verifyingWriter hashes locally for standalone Fetch callers (including the
// uncompressed exporter), or waits for the cache's existing SHA pass. Both paths
// verify disk replays too: successful original publication cannot certify a
// later read from a damaged file.
func (r *Registry) verifyingWriter(d v1.Descriptor, w io.Writer, verify func() error) (io.Writer, func(int64) error) {
	if verify == nil {
		h := digest.SHA256.Digester()
		w = io.MultiWriter(w, h.Hash())
		verify = func() error {
			if h.Digest() != d.Digest {
				return errors.New("registry blob digest mismatch")
			}
			r.localVerifications.Add(1)
			return nil
		}
	} else {
		barrier := verify
		verify = func() error {
			if err := barrier(); err != nil {
				return err
			}
			r.sharedVerifications.Add(1)
			return nil
		}
	}
	return w, func(n int64) error {
		if n != d.Size {
			return errors.New("registry blob size mismatch")
		}
		return verify()
	}
}

func (r *Registry) download(ctx context.Context, d v1.Descriptor, w io.Writer, verify func() error) error {
	if r.ranges.streams > 1 && d.Size >= r.ranges.threshold && d.Size > r.ranges.chunk {
		return r.downloadRanges(ctx, d, w, verify)
	}
	resp, err := r.http.get(ctx, r.http.ref.BlobURL(d.Digest.String()), "")
	if err != nil {
		return err
	}
	r.downloads.Add(1)
	return r.downloadResponse(ctx, d, w, verify, resp)
}

func (r *Registry) downloadResponse(ctx context.Context, d v1.Descriptor, w io.Writer, verify func() error, resp *http.Response) error {
	defer resp.Body.Close()
	if resp.ContentLength >= 0 && resp.ContentLength != d.Size {
		return errors.New("registry blob length mismatch")
	}
	dst, finish := r.verifyingWriter(d, w, verify)
	reader := &countRegistryReader{r: io.LimitReader(resp.Body, d.Size), count: &r.upstream}
	// A source stream may write unverified prefixes; transfer.Cache withholds its
	// last frame until Fetch returns successfully. The barrier drains queued
	// writes and verifies their SHA before this download can publish a disk entry.
	n, err := io.CopyBuffer(&contextWriter{ctx: ctx, w: dst}, reader, make([]byte, 64<<10))
	if err != nil {
		return safeRequestError(ctx, err)
	}
	var extra [1]byte
	extraN, extraErr := io.ReadFull(resp.Body, extra[:])
	r.upstream.Add(int64(extraN))
	if extraN != 0 || extraErr != io.EOF {
		return errors.New("registry blob has excess bytes or no verified end")
	}
	return finish(n)
}

type countRegistryReader struct {
	r     io.Reader
	count *atomic.Int64
}

func (r *countRegistryReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.count.Add(int64(n))
	return n, err
}
func (r *Registry) Metrics() RegistryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RegistryMetrics{UpstreamBytes: r.upstream.Load(), Downloads: r.downloads.Load(), CacheHits: r.hits.Load(), CacheBypasses: r.bypasses.Load(), CacheWriteErrors: r.cacheErrors.Load(), CacheReservedBytes: r.reserved, PeakCacheReservedBytes: r.peak, MetadataBytes: r.metadata.Load(), SharedVerifications: r.sharedVerifications.Load(), LocalVerifications: r.localVerifications.Load(),
		RangeDownloads: r.ranges.downloads.Load(), RangeRequests: r.ranges.requests.Load(), RangeFallbacks: r.ranges.fallbacks.Load(),
		ActiveRangeRequests: r.ranges.active.Load(), PeakRangeRequests: r.ranges.peakActive.Load(),
		RangeBufferBytes: r.ranges.used.Load(), PeakRangeBufferBytes: r.ranges.peakUsed.Load()}
}
func (r *Registry) Close() error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.mu.Unlock()
	r.wg.Wait()
	if r.transport != nil {
		r.transport.CloseIdleConnections()
	}
	if r.dir != "" {
		return os.RemoveAll(r.dir)
	}
	return nil
}

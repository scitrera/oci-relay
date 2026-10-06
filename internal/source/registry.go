// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

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
	"github.com/scitrera/oci-relay/internal/image"
)

type RegistryOptions struct {
	Reference     string
	Platform      v1.Platform
	ConfigPath    string
	PlainHTTP     bool  // Explicit opt-in for a trusted plain-HTTP registry; never disables HTTPS verification.
	MaxCacheBytes int64 // Optional retained compressed blobs, separate from transfer cache memory.
	SpoolDir      string
	Transport     *http.Transport      // Optional verified CA/proxy customization for embedded callers.
	Credentials   *RegistryCredentials // Optional in-memory credentials; nil loads Docker config on this host.
}

type RegistryMetrics struct {
	UpstreamBytes          int64 `json:"upstream_blob_bytes"`
	Downloads              int64 `json:"blob_downloads"`
	CacheHits              int64 `json:"disk_cache_hits"`
	CacheBypasses          int64 `json:"disk_cache_bypasses"`
	CacheReservedBytes     int64 `json:"disk_cache_reserved_bytes"`
	PeakCacheReservedBytes int64 `json:"peak_disk_cache_reserved_bytes"`
	MetadataBytes          int64 `json:"metadata_bytes"`
}

type registryBlob struct {
	done chan struct{}
	path string
	err  error
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
		o.Platform = v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
	}
	ctx, cancel := context.WithCancel(parent)
	r := &Registry{ctx: ctx, cancel: cancel, budget: o.MaxCacheBytes, blobs: map[digest.Digest]*registryBlob{}}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, r.Close())
		}
	}()
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
			return nil, err
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
		f, err := os.Open(blob.path)
		if err != nil {
			return err
		}
		defer f.Close()
		r.hits.Add(1)
		_, err = io.CopyBuffer(&contextWriter{ctx: ctx, w: w}, f, make([]byte, 64<<10))
		return err
	}
	var blob *registryBlob
	if r.dir != "" && d.Size <= r.budget-r.reserved {
		blob = &registryBlob{done: make(chan struct{}), path: filepath.Join(r.dir, d.Digest.Encoded())}
		r.blobs[d.Digest] = blob
		r.reserved += d.Size
		r.peak = max(r.peak, r.reserved)
	} else {
		r.bypasses.Add(1)
	}
	r.mu.Unlock()
	if blob == nil {
		return r.download(ctx, d, w)
	}
	f, err := os.OpenFile(blob.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		err = r.download(ctx, d, io.MultiWriter(w, f))
		err = errors.Join(err, f.Close())
	}
	r.mu.Lock()
	if err != nil {
		_ = os.Remove(blob.path)
		delete(r.blobs, d.Digest)
		r.reserved -= d.Size
	}
	blob.err = err
	close(blob.done)
	r.mu.Unlock()
	return err
}
func (r *Registry) download(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	resp, err := r.http.get(ctx, r.http.ref.BlobURL(d.Digest.String()), "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.ContentLength >= 0 && resp.ContentLength != d.Size {
		return errors.New("registry blob length mismatch")
	}
	r.downloads.Add(1)
	hash := digest.SHA256.Digester()
	reader := &countRegistryReader{r: io.LimitReader(resp.Body, d.Size), count: &r.upstream}
	// A source stream may write unverified prefixes; transfer.Cache withholds its
	// last frame until Fetch returns successfully and its independent hash agrees.
	n, err := io.CopyBuffer(io.MultiWriter(&contextWriter{ctx: ctx, w: w}, hash.Hash()), reader, make([]byte, 64<<10))
	if err != nil {
		return safeRequestError(ctx, err)
	}
	var extra [1]byte
	extraN, extraErr := io.ReadFull(resp.Body, extra[:])
	r.upstream.Add(int64(extraN))
	if extraN != 0 || extraErr != io.EOF {
		return errors.New("registry blob has excess bytes or no verified end")
	}
	if n != d.Size || hash.Digest() != d.Digest {
		return errors.New("registry blob digest or size mismatch")
	}
	return nil
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
	return RegistryMetrics{UpstreamBytes: r.upstream.Load(), Downloads: r.downloads.Load(), CacheHits: r.hits.Load(), CacheBypasses: r.bypasses.Load(), CacheReservedBytes: r.reserved, PeakCacheReservedBytes: r.peak, MetadataBytes: r.metadata.Load()}
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

// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"context"
	"errors"
	"fmt"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/fileio"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type Registry struct {
	Image      *image.Image
	Cache      *transfer.Cache
	Repository string
	streams    chan struct{}
	files      map[digest.Digest]string
}

func NewRegistry(im *image.Image, c *transfer.Cache, repository string) *Registry {
	return &Registry{Image: im, Cache: c, Repository: repository, streams: make(chan struct{}, 16)}
}
func (reg *Registry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(405)
		return
	}
	if r.URL.Path == "/v2/" {
		w.WriteHeader(200)
		return
	}
	prefix := "/v2/" + reg.Repository + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if strings.HasPrefix(path, "manifests/") {
		ref := strings.TrimPrefix(path, "manifests/")
		if ref != "transfer" && ref != string(reg.Image.Digest) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", reg.Image.MediaType)
		w.Header().Set("Content-Length", formatSize(int64(len(reg.Image.Manifest))))
		w.Header().Set("Docker-Content-Digest", string(reg.Image.Digest))
		if r.Method == http.MethodGet {
			_, _ = w.Write(reg.Image.Manifest)
		}
		return
	}
	if !strings.HasPrefix(path, "blobs/") {
		http.NotFound(w, r)
		return
	}
	dg := digest.Digest(strings.TrimPrefix(path, "blobs/"))
	desc, ok := reg.Image.Descriptor(dg)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", formatSize(desc.Size))
	w.Header().Set("Docker-Content-Digest", string(dg))
	if r.Method == http.MethodHead {
		return
	}
	// Deliberately ignore Range until resume is supported; this is a full 200 body.
	if dg == reg.Image.Descriptors[0].Digest {
		_, _ = w.Write(reg.Image.Config)
		return
	}
	// Docker/containerd can open more blob requests than our active response
	// limit. Queue them under the request/operation context: containerd treats
	// a local 429 as a failed pull rather than reliably retrying it. Waiting
	// requests allocate no payload buffers; Cache still bounds acquisitions.
	select {
	case reg.streams <- struct{}{}:
		defer func() { <-reg.streams }()
	case <-r.Context().Done():
		w.Header().Del("Content-Length")
		http.Error(w, r.Context().Err().Error(), http.StatusServiceUnavailable)
		return
	}
	if path := reg.files[dg]; path != "" {
		f, err := os.Open(path)
		if err != nil {
			w.Header().Del("Content-Length")
			http.Error(w, "decoded layer unavailable", 500)
			return
		}
		reader := fileio.ReadFile(f)
		defer reader.Close()
		if err = CopyResponse(r.Context(), w, reader); err != nil {
			panic(http.ErrAbortHandler)
		}
		return
	}
	reader, err := reg.Cache.Open(r.Context(), desc)
	if err != nil {
		w.Header().Del("Content-Length")
		http.Error(w, err.Error(), 503)
		return
	}
	defer reader.Close()
	if err = CopyResponse(r.Context(), w, reader); err != nil {
		panic(http.ErrAbortHandler)
	}
}

type PullOptions struct {
	Decode        DecodeOptions
	SkipPresent   bool
	DockerHost    string
	Tag           string
	ReplaceTag    bool
	Memory        int64
	Parallel      int
	Retries       int
	Import        string
	MaxImport     int64
	NativeStore   string
	NativeBase    string
	NativeRoot    string
	EngineVersion string
	Inventory     *engine.Inventory
}

func Pull(ctx context.Context, c *Client, o PullOptions) (result Result, err error) {
	result = Result{Version: ProtocolVersion, Transfer: c.Credentials.Transfer, Peer: c.Credentials.Peer, State: "FAILED", Tag: o.Tag}
	progress := newReceiverProgress(ctx, c)
	defer func() {
		progress.close()
		observation := progress.snapshot()
		result.Progress = &observation
		result.Paths = c.PathMetrics()
		if err != nil {
			result.Error = err.Error()
			if ctx.Err() != nil {
				result.State = "CANCELLED"
			}
		}
	}()
	if err = engine.ValidateTag(o.Tag); err != nil {
		return result, err
	}
	if o.Memory == 0 {
		o.Memory = 128 << 20
	}
	if o.Parallel == 0 {
		o.Parallel = 4
	}
	if o.Retries < 0 || o.Retries > 10 {
		return result, errors.New("invalid retry count")
	}
	if o.Import == "" {
		o.Import = "pull"
	}
	if o.Import != "pull" && o.Import != "load" && o.Import != "load-cached" {
		return result, errors.New("unknown receiver import mode")
	}
	if o.Import != "pull" && (o.MaxImport < 4<<20 || o.MaxImport > 1<<50) {
		return result, errors.New("load import requires an explicit max-import-bytes budget (4 MiB to 1 PiB)")
	}
	if err = o.Decode.validate(); err != nil {
		return result, err
	}
	if o.Decode.Mode == "unpigz" && o.Import != "pull" {
		return result, errors.New("unpigz decoder requires pull import")
	}
	im, err := c.Image(ctx)
	if err != nil {
		return result, err
	}
	result.Manifest = string(im.Digest)
	e, err := engine.New(o.DockerHost)
	if err != nil {
		return result, err
	}
	defer e.Close()
	target := string(im.Descriptors[0].Digest)
	result.ConfigDigest = target
	if o.Inventory != nil {
		if err = validateInventory(o.Inventory, im); err != nil {
			return result, err
		}
		inventoryMetrics(&result, *o.Inventory)
	}
	result.Store, err = qualifyReceiver(ctx, e, o)
	if err != nil {
		return result, err
	}
	matches := func(ref string) (bool, string, error) {
		info, inspectErr := e.InspectPlatform(ctx, ref, im.Platform)
		if inspectErr != nil {
			return false, "", inspectErr
		}
		if engine.MatchesImage(info, im) {
			return true, info.ID, nil
		}
		if o.NativeStore == "containerd" {
			local, localErr := openLocal(ctx, o, info.ID, im.Platform)
			if localErr != nil {
				return false, info.ID, localErr
			}
			defer local.close()
			return local.im.Descriptors[0].Digest == im.Descriptors[0].Digest, info.ID, nil
		}
		return false, info.ID, nil
	}
	if o.SkipPresent {
		// Resolve metadata first, then trust only Docker's immutable image identity
		// and matching platform. No upstream layer acquisition is necessary here.
		same, runtimeID, inspectErr := matches(o.Tag)
		if inspectErr == nil && same {
			result.State, result.ImageID, result.AlreadyPresent = "COMPLETE", target, true
			result.DockerImageID = runtimeID
			result.ImportMethod = "none"
			return result, nil
		}
		if inspectErr != nil && !errdefs.IsNotFound(inspectErr) {
			return result, inspectErr
		}

	}
	checkTag := func() error {
		same, _, inspectErr := matches(o.Tag)
		if inspectErr != nil && !errdefs.IsNotFound(inspectErr) {
			return inspectErr
		}
		if inspectErr == nil && !same && !o.ReplaceTag {
			return errors.New("destination tag conflicts; enable explicit replacement")
		}
		return nil
	}
	if err = checkTag(); err != nil {
		return result, err
	}
	progress.phase("discovering")
	view, src, cleanupLocal, err := receiverView(ctx, e, c, im, o, &result)
	defer func() { progress.phase("cleanup"); cleanupLocal() }()
	if err != nil {
		return result, err
	}
	result.InstalledManifest = string(view.Digest)
	if err = c.NegotiateLayers(ctx, im, view, result.Store, result.ReusedLayers, src.availability); err != nil {
		return result, err
	}
	progress.plan(view.Descriptors[1:], src.local, result.ReusedLayers+result.CachedBlobLayers)
	src.remote = progressSource{source: src.remote, progress: progress}
	cache, err := transfer.New(ctx, src, o.Memory, o.Parallel)
	if err != nil {
		return result, err
	}
	defer cache.Close()
	decoded := &decodedLayers{image: view}
	if o.Decode.Mode != "" && o.Decode.Mode != "none" && o.Import == "pull" {
		if result.Store == "overlay2" {
			progress.phase("decoding")
			decoded, err = prepareDecoded(ctx, view, cache, src.availability, o.Decode)
			if decoded != nil {
				result.Decoder = &decoded.metrics
				defer func() {
					if ce := decoded.close(); ce != nil {
						result.CleanupError = errors.Join(errors.New(result.CleanupError), ce).Error()
					}
				}()
			}
			if err != nil {
				return result, err
			}
			view = decoded.image
			result.InstalledManifest = string(view.Digest)
			if err = c.NegotiateLayers(ctx, im, view, result.Store, result.ReusedLayers, decoded.availability); err != nil {
				return result, err
			}
		} else if o.Decode.Mode == "unpigz" {
			return result, errors.New("unpigz decoder requires overlay2 receiver")
		}
	}
	progress.phase("transferring")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return result, err
	}
	repo := "relay/" + c.Credentials.Transfer
	server := &http.Server{Handler: decoded.registry(cache, repo), ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 32 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	tempTag := listener.Addr().String() + "/" + repo + ":transfer"
	defer func() {
		progress.phase("cleanup")
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cleanupErr := e.RemoveTag(cleanup, tempTag); cleanupErr != nil && !errdefs.IsNotFound(cleanupErr) {
			result.CleanupError = cleanupErr.Error()
		}
		result.Metrics = cache.Metrics()
	}()
	pullStart := time.Now()
	loaded := false
	if o.Import != "pull" {
		progress.phase("importing")
		loaded, err = loadImage(ctx, e, view, cache, tempTag, o, &result)
		result.PullSeconds = time.Since(pullStart).Seconds()
		if err != nil {
			return result, fmt.Errorf("receiver Docker load: %w", err)
		}
	}
	if !loaded {
		result.ImportMethod = "pull"
	}
	for attempt := 0; !loaded && attempt <= o.Retries; attempt++ {
		err = e.Pull(ctx, tempTag, im.Platform, func(p engine.Progress) {
			progress.docker(p)
			switch p.Status {
			case "Download complete":
				result.LastDownloadSeconds = time.Since(pullStart).Seconds()
			case "Pull complete":
				result.LastExtractSeconds = time.Since(pullStart).Seconds()
			}
		})
		result.PullSeconds = time.Since(pullStart).Seconds()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if attempt < o.Retries {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			}
		}
	}
	if err != nil {
		return result, fmt.Errorf("receiver Docker pull: %w", err)
	}
	progress.phase("verifying")
	pulled, err := e.InspectPlatform(ctx, tempTag, view.Platform)
	if err != nil {
		return result, err
	}
	if !engine.MatchesImage(pulled, view) {
		return result, errors.New("pulled image identity/platform mismatch")
	}
	if err = checkTag(); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if err = e.Tag(ctx, tempTag, o.Tag); err != nil {
		return result, err
	}
	final, err := e.InspectPlatform(ctx, o.Tag, view.Platform)
	if err != nil {
		return result, err
	}
	if final.ID != pulled.ID {
		return result, errors.New("destination tag changed during finalization")
	}
	result.ImageID = target
	result.DockerImageID = final.ID
	result.State = "COMPLETE"
	return result, nil
}

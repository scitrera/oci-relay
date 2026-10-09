// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/fileio"
	"github.com/scitrera/oci-relay/internal/image"
)

type DockerOptions struct {
	Reference            string
	Manifest             []byte
	Platform             v1.Platform
	AllowPreparationRead bool
	SpoolDir             string
	MaxSpool             int64
	MaxUpload            int64
	Trace                func(string, string)
}
type Docker struct {
	ctx              context.Context
	cancel           context.CancelFunc
	engine           *engine.Engine
	opts             DockerOptions
	platformPush     bool
	server           *http.Server
	listener         net.Listener
	repo             string
	tag              string
	dir              string
	Image            *image.Image
	PreparationBytes atomic.Int64
	Rounds           atomic.Int64
	PeakSpool        atomic.Int64
	mu               sync.Mutex
	preparing        bool
	raw              []byte
	known            map[digest.Digest]v1.Descriptor
	uploads          map[string]*upload
	demands          map[digest.Digest]*demand
	round            map[digest.Digest]*demand
	wake             chan struct{}
	done             chan struct{}
	uploadSlots      chan struct{}
	spoolSlots       chan struct{}
	reserved         int64
}
type upload struct {
	mu       sync.Mutex
	id       string
	hash     hash.Hash
	size     int64
	f        *os.File
	prepare  bool
	reserved bool
	closed   bool
}
type demand struct {
	ctx      context.Context
	desc     v1.Descriptor
	out      io.Writer
	done     chan error
	finished chan struct{}
	served   bool
	complete bool
	copying  bool
	writeMu  sync.Mutex
}

func NewDocker(ctx context.Context, eng *engine.Engine, o DockerOptions) (s *Docker, err error) {
	if o.MaxSpool <= 0 {
		o.MaxSpool = 8 << 30
	}
	if o.MaxUpload <= 0 {
		o.MaxUpload = o.MaxSpool
	}
	if o.MaxUpload > o.MaxSpool || o.MaxUpload < image.MaxMetadata {
		return nil, errors.New("upload cap must fit spool and permit a config blob")
	}
	inspected, err := eng.Inspect(ctx, o.Reference)
	if err != nil {
		return nil, err
	}
	actual := v1.Platform{OS: inspected.Os, Architecture: inspected.Architecture, Variant: inspected.Variant}
	if !image.Matches(o.Platform, actual) {
		return nil, errors.New("source image platform mismatch")
	}
	o.Platform = actual
	if len(o.Manifest) == 0 && !o.AllowPreparationRead {
		return nil, errors.New("local Docker metadata unavailable: supply --manifest or explicitly allow a preparation read")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(o.SpoolDir, "oci-relay-source-")
	if err != nil {
		listener.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s = &Docker{ctx: ctx, cancel: cancel, engine: eng, opts: o, platformPush: inspected.Descriptor != nil, listener: listener, dir: dir, repo: "relay/" + randomID(), known: map[digest.Digest]v1.Descriptor{}, uploads: map[string]*upload{}, demands: map[digest.Digest]*demand{}, wake: make(chan struct{}, 1), done: make(chan struct{}), uploadSlots: make(chan struct{}, 4), spoolSlots: make(chan struct{}, min(4, int(o.MaxSpool/o.MaxUpload)))}
	s.tag = listener.Addr().String() + "/" + s.repo + ":transfer"
	s.server = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() { _ = s.server.Serve(listener) }()
	started := false
	cleanupSource := s
	defer func() {
		if err != nil {
			if !started {
				close(cleanupSource.done)
			}
			_ = cleanupSource.Close()
		}
	}()
	if err = eng.Tag(ctx, inspected.ID, s.tag); err != nil {
		return nil, err
	}
	if len(o.Manifest) == 0 {
		s.mu.Lock()
		s.preparing = true
		s.mu.Unlock()
		err = s.push(ctx)
		s.mu.Lock()
		s.preparing = false
		s.mu.Unlock()
		s.clearUploads()
		if err != nil {
			return nil, fmt.Errorf("Docker preparation push: %w", err)
		}
	} else {
		s.raw = bytes.Clone(o.Manifest)
	}
	var m v1.Manifest
	if err = json.Unmarshal(s.raw, &m); err != nil {
		return nil, fmt.Errorf("missing or invalid Docker manifest: %w", err)
	}
	if m.SchemaVersion != 2 || (m.MediaType != v1.MediaTypeImageManifest && m.MediaType != image.DockerManifest) || len(m.Layers) >= image.MaxDescriptors {
		return nil, errors.New("Docker source did not produce a supported platform image")
	}
	if inspected.Descriptor == nil && string(m.Config.Digest) != inspected.ID {
		return nil, errors.New("manifest config digest does not match pinned local Docker image")
	}
	s.known = map[digest.Digest]v1.Descriptor{}
	for _, d := range append([]v1.Descriptor{m.Config}, m.Layers...) {
		if err = image.ValidateDescriptor(d); err != nil {
			return nil, err
		}
		s.known[d.Digest] = d
	}
	if m.Config.Size > image.MaxMetadata {
		return nil, errors.New("Docker config exceeds metadata limit")
	}
	go s.schedule()
	started = true
	var cfg bytes.Buffer
	if err = s.Fetch(ctx, m.Config, &cfg); err != nil {
		return nil, fmt.Errorf("acquire exact Docker config: %w", err)
	}
	s.Image, err = image.Parse(s.raw, cfg.Bytes(), "", o.Platform)
	if err == nil && inspected.Descriptor != nil {
		pinned, inspectErr := eng.InspectPlatform(ctx, inspected.ID, o.Platform)
		if inspectErr != nil {
			return nil, inspectErr
		}
		if !engine.MatchesImage(pinned, s.Image) {
			return nil, errors.New("pushed manifest does not match pinned containerd platform image")
		}
	}
	return s, err
}
func (s *Docker) push(ctx context.Context) error {
	if s.platformPush {
		return s.engine.Push(ctx, s.tag, s.opts.Platform)
	}
	return s.engine.Push(ctx, s.tag)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (s *Docker) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	if s.Image != nil && d.Digest == s.Image.Descriptors[0].Digest {
		_, err := w.Write(s.Image.Config)
		return err
	}
	if d.Size > s.opts.MaxUpload {
		return fmt.Errorf("unidentified Docker upload %s needs %d bytes; configured upload cap is %d", d.Digest, d.Size, s.opts.MaxUpload)
	}
	req := &demand{ctx: ctx, desc: d, out: w, done: make(chan error, 1), finished: make(chan struct{})}
	for {
		s.mu.Lock()
		if _, ok := s.known[d.Digest]; !ok {
			s.mu.Unlock()
			return errors.New("unknown Docker source descriptor")
		}
		previous := s.demands[d.Digest]
		if previous == nil {
			s.demands[d.Digest] = req
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		if previous.ctx.Err() == nil {
			return errors.New("duplicate acquisition must be coalesced before Docker source")
		}
		select {
		case <-previous.finished:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	// The source must never write to its caller's writer after Fetch returns.
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		req.writeMu.Lock()
		defer req.writeMu.Unlock()
		return ctx.Err()
	case <-s.ctx.Done():
		req.writeMu.Lock()
		defer req.writeMu.Unlock()
		return s.ctx.Err()
	}
}
func (s *Docker) schedule() {
	defer close(s.done)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-timer.C:
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
		s.mu.Lock()
		round := map[digest.Digest]*demand{}
		for d, req := range s.demands {
			if req.ctx.Err() != nil {
				s.finishDemandLocked(req, req.ctx.Err())
				continue
			}
			round[d] = req
		}
		s.round = round
		s.mu.Unlock()
		if len(round) == 0 {
			continue
		}
		s.Rounds.Add(1)
		roundContext, cancelRound := context.WithCancel(s.ctx)
		var stops []func() bool
		for _, req := range round {
			stops = append(stops, context.AfterFunc(req.ctx, func() {
				for _, remaining := range round {
					if remaining.ctx.Err() == nil {
						return
					}
				}
				cancelRound()
			}))
		}
		err := s.push(roundContext)
		for _, stop := range stops {
			stop()
		}
		cancelRound()
		s.clearUploads()
		s.mu.Lock()
		for _, req := range round {
			result := err
			if result == nil && !req.served {
				result = fmt.Errorf("Docker push completed without requested blob %s; source mode is incompatible", req.desc.Digest)
			}
			s.finishDemandLocked(req, result)
		}
		s.round = nil
		pending := len(s.demands) > 0
		s.mu.Unlock()
		if pending {
			select {
			case s.wake <- struct{}{}:
			default:
			}
		}
	}
}

// A verified blob finishes independently of the other uploads in its push.
// Waiting for the whole push withholds the blob's final bytes downstream and
// can make Docker retry completed layers while another large layer compresses.
// The caller holds s.mu and has stopped writing to req.out.
func (s *Docker) finishDemandLocked(req *demand, err error) {
	if req.complete {
		return
	}
	req.complete = true
	req.served = err == nil
	if s.demands[req.desc.Digest] == req {
		delete(s.demands, req.desc.Digest)
	}
	req.done <- err
	close(req.finished)
}

func (s *Docker) Close() error {
	s.cancel()
	_ = s.server.Close()
	<-s.done
	s.clearUploads()
	cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.engine.RemoveTag(cleanup, s.tag)
	if removeErr := os.RemoveAll(s.dir); err == nil {
		err = removeErr
	}
	return err
}
func (s *Docker) clearUploads() {
	s.mu.Lock()
	uploads := s.uploads
	s.uploads = map[string]*upload{}
	s.mu.Unlock()
	for _, u := range uploads {
		s.release(u)
	}
}
func (s *Docker) release(u *upload) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed {
		return
	}
	u.closed = true
	if u.f != nil {
		_ = u.f.Close()
		_ = os.Remove(u.f.Name())
	}
	if u.reserved {
		<-s.spoolSlots
		s.mu.Lock()
		s.reserved -= s.opts.MaxUpload
		s.mu.Unlock()
	}
	<-s.uploadSlots
}
func registryError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": []map[string]string{{"code": "UNKNOWN", "message": err.Error()}}})
}
func (s *Docker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.opts.Trace != nil {
		s.opts.Trace(r.Method, r.URL.RequestURI())
	}
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	if r.URL.Path == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}
	prefix := "/v2/" + s.repo + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	if strings.HasPrefix(path, "manifests/") {
		if r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		raw, err := image.ReadBounded(r.Body, image.MaxMetadata)
		if err != nil {
			registryError(w, 413, err)
			return
		}
		s.mu.Lock()
		if s.preparing {
			s.raw = raw
		} else if !bytes.Equal(raw, s.raw) {
			s.mu.Unlock()
			registryError(w, 400, errors.New("Docker changed the prepared manifest representation"))
			return
		}
		s.mu.Unlock()
		w.Header().Set("Docker-Content-Digest", string(digest.FromBytes(raw)))
		w.Header().Set("Location", r.URL.Path)
		w.WriteHeader(201)
		return
	}
	if strings.HasPrefix(path, "blobs/") && !strings.HasPrefix(path, "blobs/uploads/") {
		if r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		d := digest.Digest(strings.TrimPrefix(path, "blobs/"))
		s.mu.Lock()
		desc, known := s.known[d]
		needed := s.round[d]
		preparing := s.preparing
		available := known && (preparing || needed == nil || needed.complete || needed.ctx.Err() != nil)
		s.mu.Unlock()
		if !available {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(desc.Size, 10))
		w.Header().Set("Docker-Content-Digest", string(d))
		w.WriteHeader(200)
		return
	}
	if !strings.HasPrefix(path, "blobs/uploads/") {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimPrefix(path, "blobs/uploads/")
	if r.Method == http.MethodPost && id == "" {
		select {
		case s.uploadSlots <- struct{}{}:
		case <-r.Context().Done():
			return
		case <-s.ctx.Done():
			return
		}
		s.mu.Lock()
		prepare := s.preparing
		s.mu.Unlock()
		u := &upload{id: randomID(), hash: sha256.New(), prepare: prepare}
		if !prepare {
			select {
			case s.spoolSlots <- struct{}{}:
				u.reserved = true
			case <-r.Context().Done():
				<-s.uploadSlots
				return
			case <-s.ctx.Done():
				<-s.uploadSlots
				return
			}
			s.mu.Lock()
			s.reserved += s.opts.MaxUpload
			peak := s.reserved
			s.mu.Unlock()
			for old := s.PeakSpool.Load(); peak > old; old = s.PeakSpool.Load() {
				if s.PeakSpool.CompareAndSwap(old, peak) {
					break
				}
			}
			f, err := os.CreateTemp(s.dir, "upload-")
			if err != nil {
				s.release(u)
				registryError(w, 507, err)
				return
			}
			u.f = f
		}
		s.mu.Lock()
		s.uploads[u.id] = u
		s.mu.Unlock()
		s.uploadHeaders(w, u, 0)
		w.WriteHeader(202)
		return
	}
	s.mu.Lock()
	u := s.uploads[id]
	s.mu.Unlock()
	if u == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodDelete {
		s.mu.Lock()
		delete(s.uploads, id)
		s.mu.Unlock()
		s.release(u)
		w.WriteHeader(204)
		return
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodGet {
		s.uploadHeaders(w, u, u.size)
		u.mu.Unlock()
		w.WriteHeader(204)
		return
	}
	if r.Method != http.MethodPatch && r.Method != http.MethodPut {
		u.mu.Unlock()
		w.WriteHeader(405)
		return
	}
	if cr := r.Header.Get("Content-Range"); cr != "" {
		var first, last int64
		if _, err := fmt.Sscanf(cr, "%d-%d", &first, &last); err != nil || first != u.size || last < first {
			u.mu.Unlock()
			registryError(w, 416, errors.New("invalid upload offset"))
			return
		}
	}
	var dst io.Writer = u.hash
	if u.f != nil {
		dst = io.MultiWriter(u.hash, u.f)
	}
	limit := int64(1 << 60)
	if !u.prepare {
		limit = s.opts.MaxUpload - u.size
	}
	n, err := io.CopyBuffer(dst, io.LimitReader(r.Body, limit), make([]byte, 64<<10))
	u.size += n
	if u.prepare {
		s.PreparationBytes.Add(n)
	}
	if err == nil {
		var extra [1]byte
		if count, readErr := r.Body.Read(extra[:]); count > 0 {
			err = errors.New("upload exceeds configured cap")
		} else if readErr != nil && readErr != io.EOF {
			err = readErr
		}
	}
	if err != nil {
		u.mu.Unlock()
		s.mu.Lock()
		delete(s.uploads, id)
		s.mu.Unlock()
		s.release(u)
		registryError(w, 413, err)
		return
	}
	if r.Method == http.MethodPatch {
		s.uploadHeaders(w, u, u.size)
		u.mu.Unlock()
		w.WriteHeader(202)
		return
	}
	dg := digest.Digest(r.URL.Query().Get("digest"))
	if dg.Validate() != nil || dg.Algorithm() != digest.SHA256 || string(dg) != "sha256:"+hex.EncodeToString(u.hash.Sum(nil)) {
		u.mu.Unlock()
		registryError(w, 400, errors.New("upload digest mismatch"))
		return
	}
	s.mu.Lock()
	req := s.round[dg]
	known, exists := s.known[dg]
	copyDemand := req != nil && !req.complete && !req.copying
	if copyDemand {
		req.copying = true
	}
	s.mu.Unlock()
	if !u.prepare && (!exists || known.Size != u.size) {
		u.mu.Unlock()
		registryError(w, 400, errors.New("upload does not match prepared descriptor"))
		return
	}
	if u.prepare {
		s.mu.Lock()
		if len(s.known) >= image.MaxDescriptors {
			s.mu.Unlock()
			u.mu.Unlock()
			registryError(w, 413, errors.New("too many preparation blobs"))
			return
		}
		s.known[dg] = v1.Descriptor{Digest: dg, Size: u.size}
		s.mu.Unlock()
	}
	if copyDemand {
		req.writeMu.Lock()
		if req.ctx.Err() != nil {
			err = req.ctx.Err()
		} else if s.ctx.Err() != nil {
			err = s.ctx.Err()
		} else {
			reader := fileio.NewReader(u.f, 0, u.size+1)
			_, err = io.CopyBuffer(req.out, reader, make([]byte, 64<<10))
			_ = reader.Close()
		}
		req.writeMu.Unlock()
		s.mu.Lock()
		req.copying = false
		s.finishDemandLocked(req, err)
		s.mu.Unlock()
		// The upload itself was verified even if its consumer failed. Release
		// its reservation and let the other demands finish. Retaining it behind
		// a 500 response can deadlock Docker's new upload against a full spool.
	}
	s.mu.Lock()
	delete(s.uploads, id)
	s.mu.Unlock()
	u.mu.Unlock()
	s.release(u)
	w.Header().Set("Location", "/v2/"+s.repo+"/blobs/"+string(dg))
	w.Header().Set("Docker-Content-Digest", string(dg))
	w.WriteHeader(201)
}
func (s *Docker) uploadHeaders(w http.ResponseWriter, u *upload, size int64) {
	w.Header().Set("Location", "/v2/"+s.repo+"/blobs/uploads/"+u.id)
	w.Header().Set("Docker-Upload-UUID", u.id)
	w.Header().Set("Range", fmt.Sprintf("0-%d", max(0, size-1)))
	w.Header().Set("Content-Length", "0")
}

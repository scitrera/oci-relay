// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"context"
	"encoding/json"
	"errors"
	digest "github.com/opencontainers/go-digest"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	Decoder  *DecodeMetrics    `json:"decoder,omitempty"`
	Progress *ReceiverProgress `json:"progress,omitempty"`
	// ImageID is the protocol-1 config-digest identity, not Docker's backend ID.
	ConfigDigest        string           `json:"config_digest,omitempty"`
	DockerImageID       string           `json:"docker_image_id,omitempty"`
	InstalledManifest   string           `json:"installed_manifest_digest,omitempty"`
	Store               string           `json:"store,omitempty"`
	LocalBytes          int64            `json:"local_bytes"`
	AlreadyPresent      bool             `json:"already_present,omitempty"`
	Version             int              `json:"version"`
	Transfer            string           `json:"transfer"`
	Peer                string           `json:"peer"`
	State               string           `json:"state"`
	Manifest            string           `json:"manifest_digest,omitempty"`
	ImageID             string           `json:"image_id,omitempty"`
	Tag                 string           `json:"tag,omitempty"`
	Error               string           `json:"error,omitempty"`
	CleanupError        string           `json:"cleanup_error,omitempty"`
	Metrics             transfer.Metrics `json:"metrics"`
	PullSeconds         float64          `json:"pull_seconds"`
	LastDownloadSeconds float64          `json:"last_download_seconds"`
	LastExtractSeconds  float64          `json:"last_extract_seconds"`
	ImportMethod        string           `json:"import_method"`
	ReusedLayers        int              `json:"reused_layers"`
	CachedBlobLayers    int              `json:"cached_blob_layers"`
	DiscoverySeconds    float64          `json:"discovery_seconds"`
	DiscoveryImages     int              `json:"discovery_images"`
	DiscoveryInspected  int              `json:"discovery_inspected"`
	DiscoveryComplete   bool             `json:"discovery_complete"`
	DiscoveryStopReason string           `json:"discovery_stop_reason,omitempty"`
	DiscoveryStale      int              `json:"discovery_stale"`
	CacheProbeSeconds   float64          `json:"cache_probe_seconds"`
	CacheProbeLimited   bool             `json:"cache_probe_limited"`
	ImportArchiveBytes  int64            `json:"import_archive_bytes"`
	LoadSeconds         float64          `json:"load_seconds"`
	TransferSeconds     float64          `json:"transfer_seconds,omitempty"`
	Paths               []PathMetrics    `json:"paths,omitempty"`
}
type Server struct {
	Session          *Session
	Image            *image.Image
	Cache            *transfer.Cache
	HTTP             *http.Server
	Listener         net.Listener
	Socket           string
	ctx              context.Context
	cancel           context.CancelFunc
	streams          chan struct{}
	mu               sync.Mutex
	results          map[string]Result
	agreements       map[string]agreement
	receiverProgress map[string]ReceiverProgress
	lastLease        atomic.Int64
	lease            time.Duration
	done             chan struct{}
	stripeGroups     map[string]*stripeGroup
	stripeStreams    chan struct{}
}

func NewServer(ctx context.Context, sess *Session, im *image.Image, cache *transfer.Cache, bind, socket string, lease time.Duration) (*Server, error) {
	if err := im.Validate(); err != nil {
		return nil, err
	}
	config, err := sess.Source.TLS(true)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{Session: sess, Image: im, Cache: cache, Listener: listener, Socket: socket, ctx: ctx, cancel: cancel, streams: make(chan struct{}, 32), stripeStreams: make(chan struct{}, 256), results: map[string]Result{}, stripeGroups: map[string]*stripeGroup{}, lease: lease, done: make(chan struct{})}
	s.lastLease.Store(time.Now().UnixNano())
	s.HTTP = &http.Server{Handler: s, TLSConfig: config, ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 32 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = s.HTTP.ServeTLS(listener, "", "") }()
	if socket != "" {
		if err = os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
			s.Close()
			return nil, err
		}
		unix, err := net.Listen("unix", socket)
		if err != nil {
			s.Close()
			return nil, err
		}
		if err = os.Chmod(socket, 0600); err != nil {
			unix.Close()
			s.Close()
			return nil, err
		}
		go func() { _ = s.HTTP.ServeTLS(unix, "", "") }()
	}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if lease > 0 && time.Since(time.Unix(0, s.lastLease.Load())) > lease {
					s.cancel()
					_ = s.HTTP.Close()
					return
				}
			}
		}
	}()
	return s, nil
}
func (s *Server) Close() error {
	// Let the final result acknowledgement finish before closing its connection.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := s.HTTP.Shutdown(ctx)
	s.cancel()
	_ = s.HTTP.Close()
	if s.Socket != "" {
		_ = os.Remove(s.Socket)
	}
	return err
}
func (s *Server) Done() <-chan struct{} { return s.ctx.Done() }
func (s *Server) RenewLease()           { s.lastLease.Store(time.Now().UnixNano()) }
func (s *Server) Results() map[string]Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]Result{}
	for k, v := range s.results {
		out[k] = v
	}
	return out
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) != 1 || r.ProtoMajor != 2 {
		http.Error(w, "authenticated HTTP/2 required", 403)
		return
	}
	id := r.TLS.PeerCertificates[0].Subject.CommonName
	if _, ok := s.Session.Peers[id]; !ok {
		http.Error(w, "unknown peer", 403)
		return
	}
	switch r.URL.Path {
	case "/relay/v1/progress":
		s.progress(w, r, id)
		return
	case "/relay/v1/negotiate":
		s.negotiate(w, r, id)
		return
	case "/relay/v1/heartbeat":
		if id != "manager" || r.Method != http.MethodPost {
			http.Error(w, "manager POST required", 403)
			return
		}
		s.lastLease.Store(time.Now().UnixNano())
		w.WriteHeader(204)
		return
	case "/relay/v1/result":
		if r.Method != http.MethodPost || id == "manager" {
			http.Error(w, "receiver POST required", 403)
			return
		}
		raw, err := image.ReadBounded(r.Body, 64<<10)
		if err != nil {
			http.Error(w, err.Error(), 413)
			return
		}
		var result Result
		if json.Unmarshal(raw, &result) != nil || result.Version != ProtocolVersion || result.Transfer != s.Session.Source.Transfer || result.Peer != id || (result.State != "COMPLETE" && result.State != "VERIFIED" && result.State != "FAILED" && result.State != "CANCELLED") {
			http.Error(w, "invalid result", 400)
			return
		}
		if result.State == "COMPLETE" && (result.Manifest != string(s.Image.Digest) || result.ImageID != string(s.Image.Descriptors[0].Digest) || (result.ConfigDigest != "" && result.ConfigDigest != result.ImageID) || result.Tag == "" || result.Error != "") {
			http.Error(w, "incomplete success result", 400)
			return
		}
		if result.State == "VERIFIED" {
			descriptors := uniqueDescriptors(s.Image)
			var size int64
			for _, d := range descriptors {
				size += d.Size
			}
			if result.Manifest != string(s.Image.Digest) || result.ImageID != "" || result.Tag != "" || result.Error != "" || result.ImportMethod != "none" || result.Metrics.VerifiedBytes != size || result.Metrics.VerifiedBlobs != int64(len(descriptors)) {
				http.Error(w, "incomplete verification result", 400)
				return
			}
		}
		s.mu.Lock()
		if result.State == "COMPLETE" && result.InstalledManifest != "" && string(s.agreements[id].Manifest) != result.InstalledManifest {
			s.mu.Unlock()
			http.Error(w, "installed representation was not negotiated", 400)
			return
		}
		if _, exists := s.results[id]; exists {
			s.mu.Unlock()
			http.Error(w, "receiver already finalized", 409)
			return
		}
		s.results[id] = result
		s.mu.Unlock()
		w.WriteHeader(204)
		return
	case "/relay/v1/image":
		if r.Method != http.MethodGet {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(stripingHeader, "1")
		w.Header().Set(stripePieceHeader, formatSize(maxStripePiece))
		_ = json.NewEncoder(w).Encode(s.Image)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/relay/v1/stripes/") {
		s.serveStripe(w, r, id)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/relay/v1/blobs/") || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	d := digest.Digest(strings.TrimPrefix(r.URL.Path, "/relay/v1/blobs/"))
	desc, ok := s.Image.Descriptor(d)
	if !ok {
		http.NotFound(w, r)
		return
	}
	select {
	case s.streams <- struct{}{}:
		defer func() { <-s.streams }()
	default:
		http.Error(w, "stream limit reached", 429)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	reader, err := s.Cache.Open(ctx, desc)
	if err != nil {
		http.Error(w, err.Error(), 503)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Length", formatSize(desc.Size))
	w.Header().Set("Docker-Content-Digest", string(desc.Digest))
	if err = CopyResponse(ctx, w, reader); err != nil {
		panic(http.ErrAbortHandler)
	}
}
func CopyResponse(ctx context.Context, w http.ResponseWriter, r io.Reader) error {
	b := make([]byte, transfer.FrameSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := r.Read(b)
		if n > 0 {
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
			if _, writeErr := w.Write(b[:n]); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

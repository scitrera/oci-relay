// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0
package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
)

// A real native executable exercises Docker's helper lookup and stdin/stdout
// contract on Windows as well as Unix, without relying on a shell script.
func TestMain(m *testing.M) {
	if os.Getenv("OCI_RELAY_TEST_CREDENTIAL_HELPER") == "1" {
		raw, err := io.ReadAll(io.LimitReader(os.Stdin, 4096))
		if err != nil || len(os.Args) != 2 || os.Args[1] != "get" || strings.TrimSpace(string(raw)) != "registry.example" {
			os.Exit(2)
		}
		fmt.Println(`{"Username":"<token>","Secret":"refresh-secret"}`)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type registryFixture struct {
	manifest, config, blob, index            []byte
	manifestDigest, configDigest, blobDigest digest.Digest
	requests                                 atomic.Int64
	handler                                  func(http.ResponseWriter, *http.Request) bool
}

func newRegistryFixture(t testing.TB) *registryFixture {
	t.Helper()
	return newRegistryFixtureSize(t, 256<<10)
}

func newRegistryFixtureSize(t testing.TB, size int) *registryFixture {
	t.Helper()
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write(raw)
	_ = gz.Close()
	platform := v1.Platform{OS: "linux", Architecture: runtime.GOARCH}
	cfg, _ := json.Marshal(v1.Image{Platform: platform, RootFS: v1.RootFS{Type: "layers", DiffIDs: []digest.Digest{digest.FromBytes(raw)}}})
	f := &registryFixture{config: cfg, blob: compressed.Bytes()}
	f.configDigest = digest.FromBytes(cfg)
	f.blobDigest = digest.FromBytes(f.blob)
	f.manifest, _ = json.Marshal(v1.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageManifest, Config: v1.Descriptor{MediaType: v1.MediaTypeImageConfig, Digest: f.configDigest, Size: int64(len(cfg))}, Layers: []v1.Descriptor{{MediaType: v1.MediaTypeImageLayerGzip, Digest: f.blobDigest, Size: int64(len(f.blob))}}})
	f.manifestDigest = digest.FromBytes(f.manifest)
	f.index, _ = json.Marshal(v1.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: v1.MediaTypeImageIndex, Manifests: []v1.Descriptor{{MediaType: v1.MediaTypeImageManifest, Digest: f.manifestDigest, Size: int64(len(f.manifest)), Platform: &platform}, {MediaType: v1.MediaTypeImageManifest, Digest: digest.FromString("other-platform"), Size: 123, Platform: &v1.Platform{OS: "other", Architecture: "other"}}}})
	return f
}
func (f *registryFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/blobs/"+f.blobDigest.String()) {
		f.requests.Add(1)
	}
	if f.handler != nil && f.handler(w, r) {
		return
	}
	var body []byte
	switch r.URL.Path {
	case "/v2/test/image/manifests/latest":
		body = f.index
	case "/v2/test/image/manifests/" + f.manifestDigest.String():
		body = f.manifest
	case "/v2/test/image/blobs/" + f.configDigest.String():
		body = f.config
	case "/v2/test/image/blobs/" + f.blobDigest.String():
		body = f.blob
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	_, _ = w.Write(body)
}
func registryOptions(server *httptest.Server) RegistryOptions {
	return RegistryOptions{Reference: strings.TrimPrefix(server.URL, "http://") + "/test/image:latest", PlainHTTP: true, Credentials: &RegistryCredentials{}}
}

func TestRegistryUnavailableRetentionContinuesVerifiedStreaming(t *testing.T) {
	f := newRegistryFixture(t)
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.MaxCacheBytes = int64(len(f.blob))
	opts.SpoolDir = t.TempDir()
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Deterministically fail file creation without filling the test filesystem.
	if err = os.Remove(r.dir); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		var got bytes.Buffer
		if err = r.Fetch(context.Background(), r.Image.Descriptors[1], &got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), f.blob) {
			t.Fatal("lost streamed bytes on cache failure")
		}
	}
	m := r.Metrics()
	if m.Downloads != 2 || m.CacheWriteErrors != 1 || m.CacheHits != 0 || m.CacheReservedBytes != 0 {
		t.Fatalf("cache failure accounting: %+v", m)
	}
}

func TestRegistryCacheDirectoryFailureIsOptional(t *testing.T) {
	f := newRegistryFixture(t)
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.MaxCacheBytes = int64(len(f.blob))
	opts.SpoolDir = filepath.Join(t.TempDir(), "absent")
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal(err)
	}
	if r.Metrics().CacheWriteErrors != 1 || r.Metrics().CacheBypasses != 1 || r.dir != "" {
		t.Fatal(r.Metrics())
	}
}

type shortCacheWriter struct{}

func (shortCacheWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

func TestOptionalCacheShortWriteDoesNotInterruptStream(t *testing.T) {
	w := &optionalCacheWriter{file: shortCacheWriter{}}
	if n, err := w.Write([]byte("fixture")); n != 7 || err != nil || w.err != io.ErrShortWrite {
		t.Fatalf("short cache write: %d %v %v", n, err, w.err)
	}
}
func TestRegistryPinnedStreamingAndRetainedCache(t *testing.T) {
	f := newRegistryFixture(t)
	first, finish := make(chan struct{}), make(chan struct{})
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
			return false
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(f.blob)))
		_, _ = w.Write(f.blob[:65536])
		w.(http.Flusher).Flush()
		<-finish
		_, _ = w.Write(f.blob[65536:])
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.MaxCacheBytes = int64(len(f.blob))
	opts.SpoolDir = t.TempDir()
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if f.requests.Load() != 0 || r.RootDigest != digest.FromBytes(f.index) || r.Image.Digest != f.manifestDigest || !bytes.Equal(r.Image.Manifest, f.manifest) {
		t.Fatal("metadata resolution fetched payload or changed identity")
	}
	layer := r.Image.Descriptors[1]
	done := make(chan error, 1)
	var out bytes.Buffer
	go func() {
		done <- r.Fetch(context.Background(), layer, &firstWrite{Writer: &out, once: new(sync.Once), first: first})
	}()
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		close(finish)
		t.Fatal("did not stream before upstream completion")
	}
	close(finish)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), f.blob) {
		t.Fatal("compressed bytes changed")
	}
	out.Reset()
	if err = r.Fetch(context.Background(), layer, &out); err != nil {
		t.Fatal(err)
	}
	if f.requests.Load() != 1 || r.Metrics().CacheHits != 1 || r.Metrics().UpstreamBytes != int64(len(f.blob)) {
		t.Fatalf("unexpected reuse: %+v", r.Metrics())
	}
	dir := r.dir
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("cache directory leaked")
	}
}

type firstWrite struct {
	io.Writer
	once  *sync.Once
	first chan struct{}
}

func (w *firstWrite) Write(p []byte) (int, error) {
	n, e := w.Writer.Write(p)
	w.once.Do(func() { close(w.first) })
	return n, e
}

func TestRegistryNoCacheAndSmallBudgetRefetch(t *testing.T) {
	for _, budget := range []int64{0, 1} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			f := newRegistryFixture(t)
			srv := httptest.NewServer(f)
			defer srv.Close()
			opts := registryOptions(srv)
			opts.MaxCacheBytes = budget
			opts.SpoolDir = t.TempDir()
			r, err := NewRegistry(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			for range 2 {
				if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
					t.Fatal(err)
				}
			}
			if f.requests.Load() != 2 || r.Metrics().CacheReservedBytes != 0 || r.Metrics().CacheBypasses != 2 {
				t.Fatal(r.Metrics())
			}
		})
	}
}
func TestRegistryCorruptionCannotCompleteOrEnterDiskCache(t *testing.T) {
	f := newRegistryFixture(t)
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if !strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
			return false
		}
		bad := bytes.Clone(f.blob)
		bad[len(bad)-1] ^= 1
		_, _ = w.Write(bad)
		return true
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.MaxCacheBytes = int64(len(f.blob))
	opts.SpoolDir = t.TempDir()
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cache, err := transfer.New(context.Background(), r, 8<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	reader, err := cache.Open(context.Background(), r.Image.Descriptors[1])
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	n, err := io.Copy(io.Discard, reader)
	if err == nil || n >= int64(len(f.blob)) || cache.Metrics().VerifiedBlobs != 0 || r.Metrics().CacheReservedBytes != 0 {
		t.Fatalf("corruption accepted: n=%d err=%v", n, err)
	}
}
func TestRegistryRejectsMetadata(t *testing.T) {
	for _, failure := range []string{"oversize", "wrong-child-digest", "wrong-config", "wrong-platform", "ambiguous", "external", "schema1"} {
		t.Run(failure, func(t *testing.T) {
			f := newRegistryFixture(t)
			switch failure {
			case "oversize":
				f.index = bytes.Repeat([]byte("x"), image.MaxMetadata+1)
			case "wrong-child-digest":
				f.manifest = append(f.manifest, ' ')
			case "wrong-config":
				f.config = append(f.config, ' ')
			case "wrong-platform":
				var idx v1.Index
				_ = json.Unmarshal(f.index, &idx)
				idx.Manifests = idx.Manifests[1:]
				f.index, _ = json.Marshal(idx)
			case "ambiguous":
				var idx v1.Index
				_ = json.Unmarshal(f.index, &idx)
				idx.Manifests = append(idx.Manifests, idx.Manifests[0])
				f.index, _ = json.Marshal(idx)
			case "external":
				var idx v1.Index
				_ = json.Unmarshal(f.index, &idx)
				idx.Manifests[0].URLs = []string{"https://example.invalid/foreign"}
				f.index, _ = json.Marshal(idx)
			case "schema1":
				f.index = []byte(`{"schemaVersion":1,"fsLayers":[]}`)
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			r, err := NewRegistry(context.Background(), registryOptions(srv))
			if err == nil {
				r.Close()
				t.Fatal("invalid metadata accepted")
			}
			if f.requests.Load() != 0 {
				t.Fatal("invalid metadata fetched a layer")
			}
		})
	}
}
func TestRegistryBearerRefreshConcurrentAndPullScope(t *testing.T) {
	f := newRegistryFixture(t)
	var tokenCalls atomic.Int64
	var wanted atomic.Int64
	wanted.Store(1)
	var srv *httptest.Server
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/token" {
			if r.URL.Query().Get("scope") != "repository:test/image:pull" {
				t.Error("wrong token scope")
			}
			user, pass, ok := r.BasicAuth()
			if !ok || user != "user" || pass != "secret" {
				t.Error("missing token credentials")
			}
			tokenCalls.Add(1)
			fmt.Fprintf(w, `{"token":"token-%d","expires_in":3600}`, wanted.Load())
			return true
		}
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", wanted.Load()) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="fixture"`)
			w.WriteHeader(401)
			return true
		}
		return false
	}
	srv = httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.Credentials = &RegistryCredentials{Username: "user", Password: "secret"}
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	wanted.Store(2)
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if tokenCalls.Load() != 2 {
		t.Fatalf("token refresh not shared: %d", tokenCalls.Load())
	}
}
func TestRegistryRedirectStripsCredentials(t *testing.T) {
	f := newRegistryFixture(t)
	var seen atomic.Int64
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("credential forwarded to CDN")
		}
		seen.Add(1)
		if r.URL.Path == "/first" {
			http.Redirect(w, r, "/blob", 307)
			return
		}
		_, _ = w.Write(f.blob)
	}))
	defer cdn.Close()
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
			http.Redirect(w, r, cdn.URL+"/first?signature=private", 307)
			return true
		}
		return false
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.Credentials = &RegistryCredentials{RegistryToken: "secret"}
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 2 {
		t.Fatal("redirect not followed")
	}
}
func TestRegistryTLSVerificationAndCancellation(t *testing.T) {
	f := newRegistryFixture(t)
	srv := httptest.NewTLSServer(f)
	defer srv.Close()
	opts := RegistryOptions{Reference: strings.TrimPrefix(srv.URL, "https://") + "/test/image:latest", Credentials: &RegistryCredentials{}}
	if r, err := NewRegistry(context.Background(), opts); err == nil {
		r.Close()
		t.Fatal("untrusted TLS accepted")
	}
	opts.Transport = srv.Client().Transport.(*http.Transport)
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	f2 := newRegistryFixture(t)
	entered := make(chan struct{})
	f2.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, f2.blobDigest.String()) {
			close(entered)
			<-r.Context().Done()
			return true
		}
		return false
	}
	slow := httptest.NewServer(f2)
	defer slow.Close()
	r, err = NewRegistry(context.Background(), registryOptions(slow))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard) }()
	<-entered
	r.Close()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("cancelled transfer succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation blocked")
	}
}
func TestRegistryCredentialsBoundedAndScoped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw, _ := json.Marshal(map[string]any{"auths": map[string]any{"https://index.docker.io/v1/": map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte("user:secret"))}}})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := DockerRegistryCredentials(context.Background(), "registry-1.docker.io", path)
	if err != nil || c.Username != "user" || c.Password != "secret" {
		t.Fatal("credential resolution failed", err)
	}
	c, err = DockerRegistryCredentials(context.Background(), "unrelated.example", path)
	if err != nil || c.Username != "" {
		t.Fatal("credentials escaped registry scope")
	}
	_ = os.WriteFile(path, bytes.Repeat([]byte("x"), (1<<20)+1), 0600)
	if _, err = DockerRegistryCredentials(context.Background(), "registry-1.docker.io", path); err == nil {
		t.Fatal("unbounded credential input")
	}
}

func TestRegistryMalformedBodyNeverExceedsDiskReservation(t *testing.T) {
	for _, mode := range []string{"truncated", "excess", "encoded"} {
		t.Run(mode, func(t *testing.T) {
			f := newRegistryFixture(t)
			f.handler = func(w http.ResponseWriter, r *http.Request) bool {
				if !strings.HasSuffix(r.URL.Path, f.blobDigest.String()) {
					return false
				}
				if mode == "encoded" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				if mode == "truncated" {
					_, _ = w.Write(f.blob[:len(f.blob)-1])
				} else {
					_, _ = w.Write(f.blob)
					if mode == "excess" {
						_, _ = w.Write([]byte("extra"))
					}
				}
				return true
			}
			srv := httptest.NewServer(f)
			defer srv.Close()
			opts := registryOptions(srv)
			opts.MaxCacheBytes = int64(len(f.blob))
			opts.SpoolDir = t.TempDir()
			r, err := NewRegistry(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err == nil {
				t.Fatal("malformed upstream succeeded")
			}
			if r.Metrics().CacheReservedBytes != 0 || r.Metrics().PeakCacheReservedBytes > opts.MaxCacheBytes {
				t.Fatal(r.Metrics())
			}
			entries, err := os.ReadDir(r.dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("failed cache file retained", err)
			}
		})
	}
}
func TestRegistryDiskWriteFailureDoesNotPublishCache(t *testing.T) {
	f := newRegistryFixture(t)
	srv := httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.MaxCacheBytes = int64(len(f.blob))
	opts.SpoolDir = t.TempDir()
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	// Force a real filesystem failure at creation without exhausting a host disk.
	if err = os.Remove(r.dir); err != nil {
		t.Fatal(err)
	}
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal("optional cache failure broke streaming", err)
	}
	if r.Metrics().CacheReservedBytes != 0 || len(r.blobs) != 0 {
		t.Fatal("failed reservation retained")
	}
}
func TestRegistryCredentialHelperAndIdentityToken(t *testing.T) {
	dir := t.TempDir()
	helper := filepath.Join(dir, "docker-credential-fixture")
	if runtime.GOOS == "windows" {
		helper += ".exe"
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(helper, data, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OCI_RELAY_TEST_CREDENTIAL_HELPER", "1")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	config := filepath.Join(dir, "config.json")
	_ = os.WriteFile(config, []byte(`{"credHelpers":{"registry.example":"fixture"}}`), 0600)
	creds, err := DockerRegistryCredentials(context.Background(), "registry.example", config)
	if err != nil || creds.IdentityToken != "refresh-secret" {
		t.Fatal("helper identity token not read", err)
	}
	f := newRegistryFixture(t)
	var srv *httptest.Server
	var tokens atomic.Int64
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/token" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Method != "POST" || r.Form.Get("refresh_token") != "refresh-secret" || r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("scope") != "repository:test/image:pull" {
				t.Error("wrong refresh request")
			}
			tokens.Add(1)
			_, _ = w.Write([]byte(`{"access_token":"access","expires_in":300}`))
			return true
		}
		if r.Header.Get("Authorization") != "Bearer access" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token"`)
			w.WriteHeader(401)
			return true
		}
		return false
	}
	srv = httptest.NewServer(f)
	defer srv.Close()
	opts := registryOptions(srv)
	opts.Credentials = &creds
	r, err := NewRegistry(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Fetch(context.Background(), r.Image.Descriptors[1], io.Discard); err != nil {
		t.Fatal(err)
	}
	if tokens.Load() != 1 {
		t.Fatal("token was not reused")
	}
}
func TestRegistryRejectsUnsafeTokenRealmAndSuppressesSecrets(t *testing.T) {
	f := newRegistryFixture(t)
	f.handler = func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("WWW-Authenticate", `Bearer realm="http://bad.example/token?secret=must-not-log"`)
		w.WriteHeader(401)
		return true
	}
	srv := httptest.NewTLSServer(f)
	defer srv.Close()
	opts := RegistryOptions{Reference: strings.TrimPrefix(srv.URL, "https://") + "/test/image:latest", Credentials: &RegistryCredentials{}, Transport: srv.Client().Transport.(*http.Transport)}
	r, err := NewRegistry(context.Background(), opts)
	if err == nil {
		r.Close()
		t.Fatal("insecure realm accepted")
	}
	if strings.Contains(err.Error(), "must-not-log") {
		t.Fatal("signed URL leaked")
	}
}
func TestRegistryReferenceNormalization(t *testing.T) {
	for _, tc := range []struct{ input, host, repo, ref string }{
		{"alpine", "registry-1.docker.io", "library/alpine", "latest"},
		{"org/image:v1", "registry-1.docker.io", "org/image", "v1"},
		{"example.test:5000/org/image:v1", "example.test:5000", "org/image", "v1"},
		{"example.test/org/image@sha256:" + strings.Repeat("a", 64), "example.test", "org/image", "sha256:" + strings.Repeat("a", 64)},
	} {
		r, err := parseRegistryReference(tc.input, false)
		if err != nil || r.Registry != tc.host || r.Repository != tc.repo || r.Reference != tc.ref || r.Scheme != "https" {
			t.Fatalf("reference %s: %+v %v", tc.input, r, err)
		}
	}
}

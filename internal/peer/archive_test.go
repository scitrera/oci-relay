// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"archive/tar"
	"bytes"
	"context"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/transfer"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPortableArchivePreservesOCIAndBudget(t *testing.T) {
	ctx, s, c := fixtureServer(t)
	cache, err := transfer.New(ctx, c, 8<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	entries, size, err := portableEntries(s.Image, "example.test/fixture:tag", 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err = writeLoadArchive(ctx, &buf, cache, entries); err != nil {
		t.Fatal(err)
	}
	if int64(buf.Len()) != size {
		t.Fatalf("archive size %d != %d", buf.Len(), size)
	}
	files := map[string][]byte{}
	tr := tar.NewReader(&buf)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		files[h.Name], err = io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
	}
	read := func(name string, limit int64) ([]byte, error) { return files[filepath.ToSlash(name)], nil }
	im, err := image.LoadLayoutReader(read, "", s.Image.Platform)
	if err != nil {
		t.Fatal(err)
	}
	if im.Digest != s.Image.Digest || !bytes.Equal(im.Config, s.Image.Config) {
		t.Fatal("archive changed image identity")
	}
	if _, _, err = portableEntries(s.Image, "example.test/fixture:tag", size-1); err == nil {
		t.Fatal("archive exceeded its budget")
	}
	if len(files["manifest.json"]) == 0 {
		t.Fatal("Docker archive compatibility missing")
	}
}

func TestRealDockerPortableArchive(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	ctx, s, c := fixtureServer(t)
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tag := "oci-relay-test:archive-" + s.Session.Source.Transfer
	defer e.RemoveTag(context.Background(), tag)
	options := PullOptions{Tag: tag, Import: "archive", MaxImport: 16 << 20, Memory: 8 << 20, Parallel: 2, SkipPresent: true}
	first, err := Pull(ctx, c, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != "COMPLETE" || first.ImportMethod != "archive" {
		t.Fatalf("%+v", first)
	}
	second, err := Pull(ctx, c, options)
	if err != nil || !second.AlreadyPresent || second.ImportMethod != "none" {
		t.Fatalf("warm: %+v %v", second, err)
	}
}

// Exercise the VM/remote-daemon topology through a TCP API proxy. No daemon
// TCP port or configuration is enabled, and image data uses ImageLoad only.
func TestRealDockerPortableArchiveTCP(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1")
	}
	local, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	host := local.Client.DaemonHost()
	if !strings.HasPrefix(host, "unix://") {
		t.Skip("test proxy requires a local Unix-socket Docker daemon")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", strings.TrimPrefix(host, "unix://"))
	}}
	defer transport.CloseIdleConnections()
	proxy := httptest.NewServer(&httputil.ReverseProxy{Director: func(r *http.Request) { r.URL.Scheme = "http"; r.URL.Host = "docker" }, Transport: transport})
	defer proxy.Close()
	ctx, s, c := fixtureServer(t)
	tag := "oci-relay-test:tcp-" + s.Session.Source.Transfer
	defer local.RemoveTag(context.Background(), tag)
	config := engine.Config{DockerHost: "tcp://" + proxy.Listener.Addr().String()}
	result, err := Pull(ctx, c, PullOptions{Config: config, Tag: tag, Import: "archive", MaxImport: 16 << 20, Memory: 8 << 20, Parallel: 2})
	if err != nil || result.State != "COMPLETE" {
		t.Fatalf("TCP import: %+v %v", result, err)
	}
	api, err := engine.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	saveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	saved, err := api.Save(saveCtx, tag, s.Image.Platform)
	if err != nil {
		t.Fatal(err)
	}
	defer saved.Close()
	if _, err = io.Copy(io.Discard, saved); err != nil {
		t.Fatal(err)
	}
}

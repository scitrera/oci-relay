// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func cleanDockerEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY"} {
		t.Setenv(name, "")
	}
	t.Setenv("DOCKER_CONFIG", t.TempDir())
}

func TestEndpointValidationAndDefaults(t *testing.T) {
	cleanDockerEnvironment(t)
	c, err := (Config{}).resolve()
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" && c.DockerHost != "npipe:////./pipe/docker_engine" {
		t.Fatalf("Windows default: %+v", c)
	}
	for _, c := range []Config{
		{DockerHost: "tcp://example.test:2375"}, {DockerHost: "tcp://localhost"},
		{DockerHost: "tcp://user:secret@localhost:2375"}, {DockerHost: "tcp://localhost:2375/path"},
		{DockerHost: "ssh://remote"}, {DockerHost: "tcp://localhost:2375", DockerCert: "unpaired.pem"},
		{DockerHost: "tcp://localhost:2375", DockerContext: "other"},
	} {
		if _, err := c.resolve(); err == nil {
			t.Fatalf("accepted unsafe/malformed connection: %+v", c)
		}
	}
	for _, c := range []Config{{DockerHost: "tcp://127.0.0.1:2375"}, {DockerHost: "tcp://[::1]:2375"}, {DockerHost: "tcp://remote.test:2376", DockerTLS: true}, {DockerHost: "tcp://remote.test:2375", DockerAllowPlainHTTP: true}} {
		if _, err := c.resolve(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
	}
}

func TestDockerContextPrecedenceAndTLS(t *testing.T) {
	cleanDockerEnvironment(t)
	root := os.Getenv("DOCKER_CONFIG")
	name := "desktop-linux"
	sum := sha256.Sum256([]byte(name))
	dir := filepath.Join(root, "contexts", "meta", hex.EncodeToString(sum[:]))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"Name":"desktop-linux","Endpoints":{"docker":{"Host":"tcp://localhost:1234"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "config.json"), []byte(`{"currentContext":"desktop-linux"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := (Config{}).resolve()
	if err != nil || c.DockerHost != "tcp://localhost:1234" {
		t.Fatalf("current context: %+v %v", c, err)
	}
	t.Setenv("DOCKER_HOST", "tcp://localhost:5678")
	c, err = (Config{}).resolve()
	if err != nil || c.DockerHost != "tcp://localhost:5678" {
		t.Fatalf("environment: %+v %v", c, err)
	}
	t.Setenv("DOCKER_CONTEXT", name)
	c, err = (Config{}).resolve()
	if err != nil || c.DockerHost != "tcp://localhost:1234" {
		t.Fatalf("context environment: %+v %v", c, err)
	}
	c, err = (Config{DockerHost: "tcp://localhost:9012"}).resolve()
	if err != nil || c.DockerHost != "tcp://localhost:9012" {
		t.Fatalf("explicit host: %+v %v", c, err)
	}
}

func TestDockerTCPAndVerifiedTLS(t *testing.T) {
	cleanDockerEnvironment(t)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_ping" {
			w.Header().Set("API-Version", "1.52")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/images/fixture/json") {
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": "sha256:" + strings.Repeat("a", 64)})
			return
		}
		http.NotFound(w, r)
	})
	for _, tls := range []bool{false, true} {
		t.Run(map[bool]string{false: "TCP", true: "TLS"}[tls], func(t *testing.T) {
			var server *httptest.Server
			if tls {
				server = httptest.NewTLSServer(handler)
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			c := Config{DockerHost: "tcp://" + server.Listener.Addr().String(), DockerTLS: tls}
			if tls {
				c.DockerCA = filepath.Join(t.TempDir(), "ca.pem")
				if err := os.WriteFile(c.DockerCA, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
					t.Fatal(err)
				}
			}
			e, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			got, err := e.Inspect(context.Background(), "fixture")
			if err != nil || got.ID == "" {
				t.Fatalf("inspect: %+v %v", got, err)
			}
			if e.NativeStoreLocal() {
				t.Fatal("TCP daemon treated as local storage")
			}
			if tls {
				c.DockerCA = ""
				untrusted, err := Open(c)
				if err != nil {
					t.Fatal(err)
				}
				defer untrusted.Close()
				if _, err = untrusted.Inspect(context.Background(), "fixture"); err == nil {
					t.Fatal("untrusted TLS accepted")
				}
			}
		})
	}
}

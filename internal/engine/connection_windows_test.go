// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Microsoft/go-winio"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDockerNamedPipe(t *testing.T) {
	cleanDockerEnvironment(t)
	name := fmt.Sprintf("oci-relay-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	listener, err := winio.ListenPipe(`\\.\pipe\`+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_ping" {
			w.Header().Set("API-Version", "1.52")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/images/fixture/json") {
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": "sha256:" + strings.Repeat("a", 64)})
			return
		}
		http.NotFound(w, r)
	})}
	defer server.Close()
	go server.Serve(listener)
	e, err := Open(Config{DockerHost: "npipe:////./pipe/" + name})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := e.Inspect(ctx, "fixture")
	if err != nil || info.ID == "" {
		t.Fatalf("named pipe: %+v %v", info, err)
	}
	if e.NativeStoreLocal() {
		t.Fatal("Windows named pipe qualified as Linux local storage")
	}
}

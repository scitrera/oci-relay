// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func inventoryEngine(t *testing.T, count int, inspect func(int, http.ResponseWriter, *http.Request)) (*Engine, []string) {
	t.Helper()
	ids := make([]string, count)
	for i := range ids {
		ids[i] = string(digest.FromString(fmt.Sprint(i)))
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/images/json") {
			if r.URL.Query().Get("all") != "1" {
				t.Error("omitted dangling/intermediate images")
			}
			items := []map[string]string{}
			for _, id := range ids {
				items = append(items, map[string]string{"Id": id})
			}
			// Model repeated tags/records before deduplication, exceeding the old cap.
			for range 150 {
				items = append([]map[string]string{{"Id": ids[0]}}, items...)
			}
			_ = json.NewEncoder(w).Encode(items)
			return
		}
		for i, id := range ids {
			if strings.HasSuffix(r.URL.Path, "/images/"+id+"/json") {
				inspect(i, w, r)
				return
			}
		}
		t.Errorf("unexpected request %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	t.Cleanup(s.Close)
	c, err := client.New(client.WithHost(s.URL), client.WithAPIVersion("1.52"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &Engine{Client: c}, ids
}

func writeInventoryImage(w http.ResponseWriter, layers ...digest.Digest) {
	_ = json.NewEncoder(w).Encode(map[string]any{"Os": "linux", "Architecture": "arm64", "RootFS": map[string]any{"Type": "layers", "Layers": layers}})
}

func TestInventoryFindsOldImageDeduplicatesAndBoundsConcurrency(t *testing.T) {
	want := digest.FromString("wanted")
	var active, peak, calls atomic.Int64
	e, ids := inventoryEngine(t, 180, func(i int, w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		calls.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		if i == 179 {
			writeInventoryImage(w, want)
		} else {
			writeInventoryImage(w, digest.FromString("unrelated"))
		}
	})
	inv, err := e.Discover(context.Background(), InventoryRequest{[]digest.Digest{want}, v1.Platform{OS: "linux", Architecture: "arm64"}}, time.Second*5, false)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Listed != 180 || inv.Inspected != 180 || calls.Load() != 180 || peak.Load() > 8 || inv.Base != ids[179] || inv.Prefix != 1 || !inv.Complete || !slices.Equal(inv.Candidates, []string{ids[179]}) {
		t.Fatalf("inventory %+v calls %d peak %d", inv, calls.Load(), peak.Load())
	}
}

func TestInventoryCombinesBlobMatchesWithoutInventingParentChains(t *testing.T) {
	a, b := digest.FromString("a"), digest.FromString("b")
	e, ids := inventoryEngine(t, 100, func(i int, w http.ResponseWriter, r *http.Request) {
		switch i {
		case 0:
			writeInventoryImage(w, digest.FromString("parent-x"), a)
		case 1:
			writeInventoryImage(w, digest.FromString("parent-y"), b)
		default:
			writeInventoryImage(w, digest.FromString("other"))
		}
	})
	inv, err := e.Discover(context.Background(), InventoryRequest{[]digest.Digest{a, b}, v1.Platform{OS: "linux", Architecture: "arm64"}}, time.Second, false)
	if err != nil || inv.Prefix != 0 || inv.Inspected != 32 || !slices.Equal(inv.Candidates, ids[:2]) || !inv.Complete {
		t.Fatalf("%+v %v", inv, err)
	}
}

func TestInventoryDeadlineAndErrorsAreDistinct(t *testing.T) {
	request := InventoryRequest{[]digest.Digest{digest.FromString("missing")}, v1.Platform{OS: "linux", Architecture: "arm64"}}
	for _, mode := range []string{"deadline", "cancel", "daemon", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			e, _ := inventoryEngine(t, 8, func(i int, w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "deadline", "cancel":
					<-r.Context().Done()
				case "daemon":
					http.Error(w, `{"message":"failure"}`, 500)
				case "deleted":
					http.NotFound(w, r)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				cancel()
			}
			budget := 5 * time.Second
			if mode == "deadline" {
				budget = 20 * time.Millisecond
			}
			inv, err := e.Discover(ctx, request, budget, false)
			switch mode {
			case "deadline":
				if err != nil || inv.Complete || inv.StopReason != "time_budget" {
					t.Fatalf("%+v %v", inv, err)
				}
			case "cancel", "daemon":
				if err == nil {
					t.Fatal("hidden cancellation/daemon error")
				}
			case "deleted":
				if err != nil || !inv.Complete || len(inv.Candidates) != 0 {
					t.Fatalf("%+v %v", inv, err)
				}
			}
		})
	}
}

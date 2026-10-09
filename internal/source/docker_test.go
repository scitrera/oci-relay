// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestUploadCompletesDemandBeforeOtherRoundBlobs(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "consumer-failure"
		}
		t.Run(name, func(t *testing.T) {
			data := []byte("verified layer contents")
			d := v1.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
			var got bytes.Buffer
			var out io.Writer = &got
			if failed {
				out = failedWriter{}
			}
			req := &demand{ctx: context.Background(), desc: d, out: out, done: make(chan error, 1), finished: make(chan struct{})}
			other := &demand{ctx: context.Background(), desc: v1.Descriptor{Digest: digest.FromString("another large blob")},
				done: make(chan error, 1), finished: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &Docker{ctx: ctx, dir: t.TempDir(), repo: "relay/test", opts: DockerOptions{MaxUpload: 4 << 20},
				known: map[digest.Digest]v1.Descriptor{d.Digest: d}, uploads: map[string]*upload{},
				demands:     map[digest.Digest]*demand{d.Digest: req, other.desc.Digest: other},
				round:       map[digest.Digest]*demand{d.Digest: req, other.desc.Digest: other},
				uploadSlots: make(chan struct{}, 1), spoolSlots: make(chan struct{}, 1)}
			defer s.clearUploads()
			post := httptest.NewRecorder()
			s.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/v2/relay/test/blobs/uploads/", nil))
			if post.Code != http.StatusAccepted {
				t.Fatal(post.Code, post.Body.String())
			}
			commit := httptest.NewRecorder()
			s.ServeHTTP(commit, httptest.NewRequest(http.MethodPut,
				post.Header().Get("Location")+"?digest="+string(d.Digest), bytes.NewReader(data)))
			if commit.Code != http.StatusCreated {
				t.Fatal(commit.Code, commit.Body.String())
			}
			select {
			case err := <-req.done:
				if failed && !errors.Is(err, io.ErrClosedPipe) || !failed && err != nil {
					t.Fatal("unexpected completion", err)
				}
			default:
				t.Fatal("verified blob is waiting for an unrelated upload in the same round")
			}
			if !failed && !bytes.Equal(got.Bytes(), data) {
				t.Fatal("consumer did not receive the exact blob")
			}
			if other.complete || s.demands[other.desc.Digest] != other || s.demands[d.Digest] != nil {
				t.Fatal("completion corrupted another pending acquisition")
			}
			// Finishing the enclosing push later must neither overwrite this
			// result nor close the completion channel twice.
			s.mu.Lock()
			s.finishDemandLocked(req, errors.New("later unrelated push failure"))
			s.mu.Unlock()
			select {
			case <-req.done:
				t.Fatal("demand completed twice")
			default:
			}
			if s.reserved != 0 || len(s.uploads) != 0 {
				t.Fatal("completed upload retained its staging reservation")
			}
			// A fresh Docker retry must be admitted with a one-upload budget.
			admitted := make(chan int, 1)
			go func() {
				retry := httptest.NewRecorder()
				s.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/v2/relay/test/blobs/uploads/", nil))
				admitted <- retry.Code
			}()
			select {
			case status := <-admitted:
				if status != http.StatusAccepted {
					t.Fatal(status)
				}
			case <-time.After(time.Second):
				cancel()
				<-admitted
				t.Fatal("new upload blocked behind a leaked reservation")
			}
		})
	}
}

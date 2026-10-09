// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"github.com/spark-arena/oci-relay/internal/engine"
	"github.com/spark-arena/oci-relay/internal/image"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDockerSource(t *testing.T) {
	if os.Getenv("OCI_RELAY_DOCKER_TESTS") != "1" {
		t.Skip("set OCI_RELAY_DOCKER_TESTS=1 for an isolated real-daemon fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	e, err := engine.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	dir := t.TempDir()
	data := make([]byte, 2<<20)
	if _, err = rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "payload"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\nCOPY payload /payload\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tag := "oci-relay-test:" + randomID()
	output, err := exec.CommandContext(ctx, "docker", "build", "-q", "-t", tag, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("build fixture: %v: %s", err, output)
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if err := e.RemoveTag(cleanup, tag); err != nil {
			t.Log(err)
		}
	}()
	s, err := NewDocker(ctx, e, DockerOptions{Reference: tag, AllowPreparationRead: true, MaxSpool: 8 << 20, MaxUpload: 4 << 20, SpoolDir: t.TempDir(), Trace: func(method, path string) { t.Log(method, path) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = s.Image.Validate(); err != nil {
		t.Fatal(err)
	}
	if s.PreparationBytes.Load() == 0 {
		t.Fatal("preparation byte count missing")
	}
	for _, d := range s.Image.Descriptors {
		var got bytes.Buffer
		if err = s.Fetch(ctx, d, &got); err != nil {
			t.Fatal(err)
		}
		if err = image.Verify(got.Bytes(), d); err != nil {
			t.Fatal(err)
		}
	}
	if s.PeakSpool.Load() > 8<<20 {
		t.Fatal("spool reservation exceeded limit")
	}
	t.Logf("prepared=%d rounds=%d reserved_peak=%d manifest=%s", s.PreparationBytes.Load(), s.Rounds.Load(), s.PeakSpool.Load(), s.Image.Digest)
	// A second independent push must still reproduce the prepared layer exactly.
	var again bytes.Buffer
	d := s.Image.Descriptors[len(s.Image.Descriptors)-1]
	if err = s.Fetch(ctx, d, &again); err != nil {
		t.Fatal(err)
	}
	if err = image.Verify(again.Bytes(), d); err != nil {
		t.Fatal(err)
	}
	cancelled, stop := context.WithCancel(ctx)
	err = s.Fetch(cancelled, d, cancelWriter{stop})
	stop()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected demand cancellation, got %v", err)
	}
	retry, finish := context.WithTimeout(ctx, 5*time.Second)
	defer finish()
	again.Reset()
	if err = s.Fetch(retry, d, &again); err != nil {
		t.Fatalf("retry behind cancelled round: %v", err)
	}
	if err = image.Verify(again.Bytes(), d); err != nil {
		t.Fatal(err)
	}
	// CLI-supplied metadata bypasses the full preparation read and still binds
	// to the exact source image/config and subsequent compressed layer bytes.
	prepared, err := NewDocker(ctx, e, DockerOptions{Reference: tag, Manifest: s.Image.Manifest, MaxSpool: 8 << 20, MaxUpload: 4 << 20, SpoolDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if prepared.PreparationBytes.Load() != 0 || prepared.Image.Digest != s.Image.Digest {
		t.Fatal("supplied manifest triggered preparation or changed identity")
	}
	again.Reset()
	if err = prepared.Fetch(ctx, d, &again); err != nil {
		t.Fatal(err)
	}
	if err = image.Verify(again.Bytes(), d); err != nil {
		t.Fatal(err)
	}
	archive, err := NewArchive(ctx, e, ArchiveOptions{Reference: tag, MaxSpool: 8 << 20, SpoolDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	if archive.Image.Descriptors[0].Digest != s.Image.Descriptors[0].Digest {
		t.Fatal("export changed config identity")
	}
	for _, desc := range archive.Image.Descriptors {
		var got bytes.Buffer
		if err = archive.Fetch(ctx, desc, &got); err != nil {
			t.Fatal(err)
		}
		if err = image.Verify(got.Bytes(), desc); err != nil {
			t.Fatal(err)
		}
	}
}

type cancelWriter struct{ cancel context.CancelFunc }

func (w cancelWriter) Write([]byte) (int, error) { w.cancel(); return 0, context.Canceled }

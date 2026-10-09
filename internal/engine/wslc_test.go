// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/containerd/errdefs"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A native helper subprocess exercises binary pipes and exit codes on Windows
// as well as Unix. It models the public CLI contract, not a running WSLC VM.
func TestMain(m *testing.M) {
	if os.Getenv("OCI_RELAY_TEST_WSLC_HELPER") == "1" {
		os.Exit(wslcTestCLI())
	}
	os.Exit(m.Run())
}
func wslcTestCLI() int {
	args := os.Args[1:]
	if len(args) < 4 || args[0] != "--session" || args[1] != "test session" {
		return 9
	}
	args = args[2:]
	mode := os.Getenv("OCI_RELAY_TEST_WSLC_MODE")
	switch strings.Join(args[:2], " ") {
	case "image inspect":
		if mode == "missing" {
			fmt.Fprintln(os.Stderr, "localized diagnostic")
			fmt.Println("[]")
			return 1
		}
		if mode == "offline" {
			fmt.Fprintln(os.Stderr, "service unavailable")
			return 1
		}
		_ = json.NewEncoder(os.Stdout).Encode([]map[string]any{{"Id": "sha256:" + strings.Repeat("a", 64), "Os": "linux", "Architecture": "arm64", "RootFS": map[string]any{"Type": "layers", "Layers": []string{}}}})
	case "image save":
		_, _ = os.Stdout.Write([]byte{0, 1, 255, 13, 10, 0, 128})
		if mode == "failed-save" {
			fmt.Fprint(os.Stderr, "export failed")
			return 2
		}
	case "image load":
		if len(args) != 4 || args[2] != "--input" {
			return 8
		}
		data, err := os.ReadFile(args[3])
		if err != nil || len(data) != 2048 {
			return 7
		}
		if err = os.WriteFile(os.Getenv("OCI_RELAY_TEST_WSLC_MARKER"), data, 0600); err != nil {
			return 6
		}
	case "image list":
		fmt.Println("sha256:" + strings.Repeat("a", 64))
	default:
		return 5
	}
	return 0
}
func testWSLC(t *testing.T) *Engine {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OCI_RELAY_TEST_WSLC_HELPER", "1")
	e, err := Open(Config{Runtime: "wslc", WSLCExecutable: exe, WSLCSession: "test session"})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestWSLCInspectionAndExportFailures(t *testing.T) {
	e := testWSLC(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, mode := range []string{"missing", "offline"} {
		t.Setenv("OCI_RELAY_TEST_WSLC_MODE", mode)
		_, err := e.Inspect(ctx, "example.test/image:tag")
		if err == nil || errdefs.IsNotFound(err) != (mode == "missing") {
			t.Fatalf("%s: %v", mode, err)
		}
	}
	for _, mode := range []string{"", "failed-save"} {
		t.Setenv("OCI_RELAY_TEST_WSLC_MODE", mode)
		r, err := e.Save(ctx, "example.test/image:tag", v1.Platform{OS: "linux", Architecture: "arm64"})
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if !bytes.Equal(data, []byte{0, 1, 255, 13, 10, 0, 128}) {
			t.Fatalf("binary pipe changed: %v", data)
		}
		if (errors.Join(readErr, closeErr) != nil) != (mode != "") {
			t.Fatalf("export failure lost: %v %v", readErr, closeErr)
		}
	}
}
func TestWSLCStagingIsBoundedAndRemoved(t *testing.T) {
	e := testWSLC(t)
	root := t.TempDir()
	marker := filepath.Join(t.TempDir(), "loaded")
	t.Setenv("OCI_RELAY_TEST_WSLC_MARKER", marker)
	for _, size := range []int{2047, 2049, 2048} {
		err := e.LoadArchive(context.Background(), bytes.NewReader(make([]byte, size)), 2048, root)
		if (err == nil) != (size == 2048) {
			t.Fatalf("size=%d err=%v", size, err)
		}
		files, _ := os.ReadDir(root)
		if len(files) != 0 {
			t.Fatal("staging leaked")
		}
		if size != 2048 {
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("invalid archive reached WSLC")
			}
		}
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("valid archive never loaded")
	}
}

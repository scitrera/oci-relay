// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/moby/moby/client"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/privatefs"
)

// WSLC's public CLI preserves Docker inspect JSON and streams image save to
// stdout. Image load currently requires a seekable file. Payload never passes
// through a shell or the Python controller.
type wslcRuntime struct{ executable, session string }

func newWSLC(c Config) (*Engine, error) {
	if c.DockerHost != "" || c.DockerContext != "" || c.DockerTLS || c.DockerCA != "" || c.DockerCert != "" || c.DockerKey != "" || c.DockerAllowPlainHTTP {
		return nil, errors.New("WSLC cannot be combined with Docker connection options")
	}
	name := c.WSLCExecutable
	if name == "" {
		name = "wslc.exe"
	}
	path, err := exec.LookPath(name)
	if err != nil {
		return nil, fmt.Errorf("WSLC executable: %w", err)
	}
	return &Engine{wslc: &wslcRuntime{path, c.WSLCSession}}, nil
}

type limitedOutput struct {
	bytes.Buffer
	limit int
}

func (b *limitedOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("runtime output exceeds limit")
	}
	return b.Buffer.Write(p)
}

func (w *wslcRuntime) command(ctx context.Context, args ...string) *exec.Cmd {
	if w.session != "" {
		args = append([]string{"--session", w.session}, args...)
	}
	cmd := exec.CommandContext(ctx, w.executable, args...)
	cmd.WaitDelay = 2 * time.Second
	return cmd
}

func (w *wslcRuntime) output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := w.command(ctx, args...)
	stdout, stderr := &limitedOutput{limit: image.MaxMetadata}, &limitedOutput{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return bytes.TrimPrefix(stdout.Bytes(), []byte{0xef, 0xbb, 0xbf}), fmt.Errorf("WSLC %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return bytes.TrimPrefix(stdout.Bytes(), []byte{0xef, 0xbb, 0xbf}), nil
}
func (w *wslcRuntime) simple(ctx context.Context, args ...string) error {
	_, err := w.output(ctx, args...)
	return err
}

func (w *wslcRuntime) inspect(ctx context.Context, ref string) (client.ImageInspectResult, error) {
	if _, err := reference.ParseAnyReference(ref); err != nil {
		return client.ImageInspectResult{}, err
	}
	raw, err := w.output(ctx, "image", "inspect", ref)
	if err != nil {
		// The CLI emits [] and exits 1 for a valid missing image. Match this
		// structured contract, never localized stderr prose.
		var missing []json.RawMessage
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 && json.Unmarshal(raw, &missing) == nil && string(bytes.TrimSpace(raw)) == "[]" {
			return client.ImageInspectResult{}, fmt.Errorf("%w: %s", errdefs.ErrNotFound, ref)
		}
		return client.ImageInspectResult{}, err
	}
	var images []client.ImageInspectResult
	if err := json.Unmarshal(raw, &images); err != nil || len(images) != 1 {
		return client.ImageInspectResult{}, errors.New("WSLC returned invalid image inspection JSON")
	}
	info := images[0]
	if info.ID == "" || info.Os != "linux" || info.Architecture == "" || info.RootFS.Type != "layers" {
		return info, errors.New("WSLC inspection lacks Linux image identity and layer metadata")
	}
	return info, nil
}

func (w *wslcRuntime) remove(ctx context.Context, ref string) error {
	if _, err := w.inspect(ctx, ref); err != nil {
		return err
	}
	return w.simple(ctx, "image", "rm", ref)
}

func (e *Engine) ListImages(ctx context.Context) ([]string, error) {
	if e.wslc != nil {
		raw, err := e.wslc.output(ctx, "image", "list", "--all", "--quiet", "--no-trunc")
		if err != nil {
			return nil, err
		}
		return strings.Fields(string(raw)), nil
	}
	r, err := e.Client.ImageList(ctx, client.ImageListOptions{All: true})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(r.Items))
	for _, item := range r.Items {
		ids = append(ids, item.ID)
	}
	return ids, nil
}

type commandStream struct {
	io.ReadCloser
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stderr *limitedOutput
	once   sync.Once
	err    error
}

func (s *commandStream) finish() error {
	s.once.Do(func() {
		s.err = s.cmd.Wait()
		if s.err != nil {
			s.err = fmt.Errorf("WSLC image save: %w: %s", s.err, s.stderr.String())
		}
	})
	return s.err
}
func (s *commandStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if err == io.EOF {
		if finish := s.finish(); finish != nil {
			return n, finish
		}
	}
	return n, err
}
func (s *commandStream) Close() error { s.cancel(); _ = s.ReadCloser.Close(); return s.finish() }

func (e *Engine) Save(ctx context.Context, id string, p v1.Platform) (io.ReadCloser, error) {
	if e.wslc == nil {
		var options []client.ImageSaveOption
		if p.OS != "" {
			options = append(options, client.ImageSaveWithPlatforms(p))
		}
		return e.Client.ImageSave(ctx, []string{id}, options...)
	}
	info, err := e.Inspect(ctx, id)
	if err != nil {
		return nil, err
	}
	if !image.Matches(p, v1.Platform{OS: info.Os, Architecture: info.Architecture, Variant: info.Variant}) {
		return nil, errors.New("WSLC source platform mismatch")
	}
	ctx, cancel := context.WithCancel(ctx)
	cmd := e.wslc.command(ctx, "image", "save", id)
	stderr := &limitedOutput{limit: 64 << 10}
	cmd.Stderr = stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		cancel()
		_ = out.Close()
		return nil, err
	}
	return &commandStream{ReadCloser: out, cmd: cmd, cancel: cancel, stderr: stderr}, nil
}

// LoadArchive uses streaming Docker API import, or one bounded, owned WSLC
// staging file. Size is the validated tar size, never an untrusted allocation.
func (e *Engine) LoadArchive(ctx context.Context, input io.Reader, size int64, directory string) (returnErr error) {
	if e.wslc == nil {
		return e.Load(ctx, input)
	}
	if size < 1024 || size > 1<<50 {
		return errors.New("invalid WSLC archive size")
	}
	dir, err := os.MkdirTemp(directory, "oci-relay-wslc-")
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, os.RemoveAll(dir)) }()
	if err = privatefs.Ensure(dir); err != nil {
		return err
	}
	available, err := privatefs.Available(dir)
	if err != nil {
		return err
	}
	if size > available-(1<<30) {
		return errors.New("WSLC archive staging requires its full size plus 1 GiB free")
	}
	f, err := os.OpenFile(filepath.Join(dir, "image.tar"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyN(f, input, size)
	closeErr := f.Close()
	if err = errors.Join(copyErr, closeErr); err != nil {
		return err
	}
	var extra [1]byte
	if n, err := input.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("WSLC archive size mismatch")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return e.wslc.simple(ctx, "image", "load", "--input", f.Name())
}

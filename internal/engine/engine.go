// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/distribution/reference"
	"github.com/moby/moby/client"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Engine struct{ Client *client.Client }

func New(host string) (*Engine, error) {
	if host == "" {
		host = "unix:///var/run/docker.sock"
	}
	if !strings.HasPrefix(host, "unix://") {
		return nil, errors.New("only explicit local unix Docker sockets are supported")
	}
	cli, err := client.New(client.WithHost(host))
	if err != nil {
		return nil, err
	}
	return &Engine{cli}, nil
}
func (e *Engine) Close() error { return e.Client.Close() }
func (e *Engine) Inspect(ctx context.Context, ref string) (client.ImageInspectResult, error) {
	return e.Client.ImageInspect(ctx, ref)
}
func (e *Engine) Tag(ctx context.Context, src, dst string) error {
	_, err := e.Client.ImageTag(ctx, client.ImageTagOptions{Source: src, Target: dst})
	return err
}
func (e *Engine) RemoveTag(ctx context.Context, tag string) error {
	_, err := e.Client.ImageRemove(ctx, tag, client.ImageRemoveOptions{})
	return err
}
func (e *Engine) Push(ctx context.Context, ref string) error {
	r, err := e.Client.ImagePush(ctx, ref, client.ImagePushOptions{RegistryAuth: "e30="})
	if err != nil {
		return err
	}
	return progress(ctx, r)
}
func (e *Engine) Pull(ctx context.Context, ref string, p v1.Platform, observe ...func(Progress)) error {
	r, err := e.Client.ImagePull(ctx, ref, client.ImagePullOptions{Platforms: []v1.Platform{p}})
	if err != nil {
		return err
	}
	return progress(ctx, r, observe...)
}
func (e *Engine) Load(ctx context.Context, input io.Reader) error {
	r, err := e.Client.ImageLoad(ctx, input, client.ImageLoadWithQuiet(true))
	if err != nil {
		return err
	}
	return progress(ctx, r)
}

type Progress struct {
	ID          string
	Status      string
	Error       string
	ErrorDetail struct{ Message string }
}

func progress(ctx context.Context, r io.ReadCloser, observe ...func(Progress)) error {
	defer r.Close()
	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	defer stop()
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	for scan.Scan() {
		var message Progress
		if err := json.Unmarshal(scan.Bytes(), &message); err != nil {
			return fmt.Errorf("invalid Docker progress: %w", err)
		}
		if message.ErrorDetail.Message != "" {
			return errors.New(message.ErrorDetail.Message)
		}
		if message.Error != "" {
			return errors.New(message.Error)
		}
		for _, callback := range observe {
			callback(message)
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return scan.Err()
}
func ValidateTag(tag string) error {
	ref, err := reference.ParseNormalizedNamed(tag)
	if err != nil {
		return err
	}
	if _, ok := ref.(reference.Digested); ok {
		return errors.New("destination must be a tag, not a digest")
	}
	if reference.IsNameOnly(ref) {
		return fmt.Errorf("destination %q requires an explicit tag", tag)
	}
	return nil
}

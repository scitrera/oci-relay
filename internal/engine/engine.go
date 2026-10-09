// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/scitrera/oci-relay/internal/image"
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

// InspectPlatform resolves a containerd index to the selected manifest. Classic
// image IDs remain config digests; containerd IDs must not be treated as such.
func (e *Engine) InspectPlatform(ctx context.Context, ref string, p v1.Platform) (client.ImageInspectResult, error) {
	info, err := e.Inspect(ctx, ref)
	if err != nil || info.Descriptor == nil {
		return info, err
	}
	return e.Client.ImageInspect(ctx, ref, client.ImageInspectWithPlatform(&p))
}

func MatchesImage(info client.ImageInspectResult, im *image.Image) bool {
	if !image.Matches(im.Platform, v1.Platform{OS: info.Os, Architecture: info.Architecture, Variant: info.Variant}) {
		return false
	}
	var cfg v1.Image
	if json.Unmarshal(im.Config, &cfg) != nil || len(info.RootFS.Layers) != len(cfg.RootFS.DiffIDs) {
		return false
	}
	for i, d := range cfg.RootFS.DiffIDs {
		if info.RootFS.Layers[i] != string(d) {
			return false
		}
	}
	if info.Descriptor != nil {
		return info.Descriptor.Digest == im.Digest
	}
	return info.ID == string(im.Descriptors[0].Digest)
}
func (e *Engine) Tag(ctx context.Context, src, dst string) error {
	_, err := e.Client.ImageTag(ctx, client.ImageTagOptions{Source: src, Target: dst})
	return err
}
func (e *Engine) RemoveTag(ctx context.Context, tag string) error {
	_, err := e.Client.ImageRemove(ctx, tag, client.ImageRemoveOptions{})
	return err
}
func (e *Engine) Push(ctx context.Context, ref string, platform ...v1.Platform) error {
	options := client.ImagePushOptions{RegistryAuth: "e30="}
	if len(platform) > 0 {
		options.Platform = &platform[0]
	}
	r, err := e.Client.ImagePush(ctx, ref, options)
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
	ID             string
	Status         string
	Error          string
	ErrorDetail    struct{ Message string }
	ProgressDetail struct {
		Current int64
		Total   int64
	}
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

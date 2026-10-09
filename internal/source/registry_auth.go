// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spark-arena/oci-relay/internal/image"
)

// RegistryCredentials are read on the fetcher, never sent to relay receivers.
type RegistryCredentials struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	IdentityToken string `json:"identitytoken"`
	RegistryToken string `json:"registrytoken"`
	Auth          string `json:"auth"`
}

// DockerRegistryCredentials honors Docker's per-registry helper, default store,
// and inline auth precedence. Errors deliberately omit credential/helper output.
func DockerRegistryCredentials(ctx context.Context, host, configPath string) (RegistryCredentials, error) {
	var empty RegistryCredentials
	explicit := configPath != ""
	if !explicit {
		dir := os.Getenv("DOCKER_CONFIG")
		if dir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return empty, err
			}
			dir = filepath.Join(home, ".docker")
		}
		configPath = filepath.Join(dir, "config.json")
	}
	raw, err := image.ReadFile(configPath, 1<<20)
	if errors.Is(err, os.ErrNotExist) && !explicit {
		return empty, nil
	}
	if err != nil {
		return empty, errors.New("cannot read Docker registry credential configuration")
	}
	var cfg struct {
		Auths   map[string]RegistryCredentials `json:"auths"`
		Helpers map[string]string              `json:"credHelpers"`
		Store   string                         `json:"credsStore"`
	}
	if json.Unmarshal(raw, &cfg) != nil {
		return empty, errors.New("invalid Docker registry credential configuration")
	}
	key := host
	aliases := []string{host, "https://" + host, "https://" + host + "/v1/"}
	if host == "registry-1.docker.io" {
		key = "https://index.docker.io/v1/"
		aliases = []string{key, "docker.io", "index.docker.io", host}
	}
	helper := cfg.Helpers[host]
	if host == "registry-1.docker.io" && cfg.Helpers["docker.io"] != "" {
		helper = cfg.Helpers["docker.io"]
	}
	if helper == "" {
		helper = cfg.Store
	}
	if helper != "" {
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`).MatchString(helper) {
			return empty, errors.New("invalid Docker credential helper name")
		}
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker-credential-"+helper, "get")
		cmd.Stdin = strings.NewReader(key + "\n")
		output := &boundedCredentialOutput{}
		cmd.Stdout, cmd.Stderr = output, io.Discard
		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return empty, ctx.Err()
			}
			// Docker's standard helper contract distinguishes missing entries from
			// helper/OS failures. Never silently ignore a broken configured helper.
			if strings.TrimSpace(output.String()) == "credentials not found in native keychain" {
				return empty, nil
			}
			return empty, errors.New("Docker credential helper failed")
		}
		var entry struct{ Username, Secret string }
		if json.Unmarshal(output.Bytes(), &entry) != nil {
			return empty, errors.New("invalid Docker credential helper response")
		}
		if entry.Username == "<token>" {
			return RegistryCredentials{IdentityToken: entry.Secret}, nil
		}
		return RegistryCredentials{Username: entry.Username, Password: entry.Secret}, nil
	}
	for _, alias := range aliases {
		if c, ok := cfg.Auths[alias]; ok {
			if c.Auth != "" {
				decoded, err := base64.StdEncoding.DecodeString(c.Auth)
				if err != nil {
					return empty, errors.New("invalid Docker registry auth encoding")
				}
				user, password, ok := strings.Cut(string(decoded), ":")
				if !ok {
					return empty, errors.New("invalid Docker registry auth entry")
				}
				c.Username, c.Password, c.Auth = user, strings.TrimRight(password, "\x00"), ""
			}
			return c, nil
		}
	}
	return empty, nil
}

type boundedCredentialOutput struct{ bytes.Buffer }

func (b *boundedCredentialOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64<<10 {
		return 0, errors.New("credential helper output exceeds limit")
	}
	return b.Buffer.Write(p)
}

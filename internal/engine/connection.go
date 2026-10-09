// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/moby/moby/client"
	"github.com/scitrera/oci-relay/internal/image"
)

// Config describes the runtime connection, independently of the relay's OS or
// the image platform. Docker contexts are resolved once, before an operation.
type Config struct {
	Runtime              string `json:"runtime,omitempty"`
	DockerHost           string `json:"docker_host,omitempty"`
	DockerContext        string `json:"docker_context,omitempty"`
	DockerTLS            bool   `json:"docker_tls,omitempty"`
	DockerCA             string `json:"docker_ca,omitempty"`
	DockerCert           string `json:"docker_cert,omitempty"`
	DockerKey            string `json:"docker_key,omitempty"`
	DockerAllowPlainHTTP bool   `json:"docker_allow_plain_http,omitempty"`
	WSLCExecutable       string `json:"wslc_executable,omitempty"`
	WSLCSession          string `json:"wslc_session,omitempty"`
}

func (c Config) resolve() (Config, error) {
	if c.WSLCExecutable != "" || c.WSLCSession != "" {
		return c, errors.New("WSLC options require runtime wslc")
	}
	if c.Runtime != "" && c.Runtime != "docker" {
		return c, errors.New("Docker connection requires runtime docker")
	}
	if c.DockerHost != "" && c.DockerContext != "" {
		return c, errors.New("choose docker-host or docker-context, not both")
	}
	root := os.Getenv("DOCKER_CONFIG")
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return c, err
		}
		root = filepath.Join(home, ".docker")
	}
	name := c.DockerContext
	if c.DockerHost == "" && name == "" {
		name = os.Getenv("DOCKER_CONTEXT")
		if name == "" {
			c.DockerHost = os.Getenv("DOCKER_HOST")
		}
		if name == "" && c.DockerHost == "" {
			var settings struct {
				CurrentContext string `json:"currentContext"`
			}
			if err := readJSON(filepath.Join(root, "config.json"), &settings); err != nil && !os.IsNotExist(err) {
				return c, err
			}
			name = settings.CurrentContext
		}
	}
	if name != "" && name != "default" {
		hash := sha256.Sum256([]byte(name))
		id := hex.EncodeToString(hash[:])
		var metadata struct {
			Name      string
			Endpoints map[string]struct {
				Host          string
				SkipTLSVerify bool
			}
		}
		if err := readJSON(filepath.Join(root, "contexts", "meta", id, "meta.json"), &metadata); err != nil {
			return c, fmt.Errorf("Docker context %q: %w", name, err)
		}
		endpoint, ok := metadata.Endpoints["docker"]
		if !ok || metadata.Name != name || endpoint.Host == "" {
			return c, errors.New("invalid Docker context endpoint")
		}
		if endpoint.SkipTLSVerify {
			return c, errors.New("Docker contexts that disable TLS verification are not supported")
		}
		c.DockerHost = endpoint.Host
		dir := filepath.Join(root, "contexts", "tls", id, "docker")
		for _, entry := range []struct {
			name  string
			value *string
		}{{"ca.pem", &c.DockerCA}, {"cert.pem", &c.DockerCert}, {"key.pem", &c.DockerKey}} {
			if *entry.value == "" {
				path := filepath.Join(dir, entry.name)
				if _, err := os.Stat(path); err == nil {
					*entry.value = path
				} else if !os.IsNotExist(err) {
					return c, err
				}
			}
		}
	} else {
		// Explicit hosts and the default context use Docker's environment TLS
		// material. Never borrow certificates from an unrelated named context.
		if dir := os.Getenv("DOCKER_CERT_PATH"); dir != "" && c.DockerCA == "" && c.DockerCert == "" && c.DockerKey == "" {
			c.DockerCA, c.DockerCert, c.DockerKey = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
		}
		c.DockerTLS = c.DockerTLS || os.Getenv("DOCKER_TLS_VERIFY") != ""
	}
	if c.DockerHost == "" {
		c.DockerHost = client.DefaultDockerHost
	}
	c.DockerTLS = c.DockerTLS || c.DockerCA != "" || c.DockerCert != "" || c.DockerKey != ""
	if (c.DockerCert == "") != (c.DockerKey == "") {
		return c, errors.New("Docker TLS requires both client certificate and key")
	}
	u, err := url.Parse(c.DockerHost)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return c, errors.New("invalid Docker endpoint")
	}
	switch u.Scheme {
	case "unix":
		if u.Host != "" || !filepath.IsAbs(u.Path) || c.DockerTLS {
			return c, errors.New("invalid local Docker Unix socket")
		}
	case "npipe":
		if runtime.GOOS != "windows" {
			return c, errors.New("Docker named pipes require Windows")
		}
		if !strings.HasPrefix(c.DockerHost, "npipe:////./pipe/") || c.DockerTLS {
			return c, errors.New("Docker named pipe must be local")
		}
	case "tcp":
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil || host == "" || port == "" || u.Path != "" {
			return c, errors.New("Docker TCP endpoint requires host:port")
		}
		ip := net.ParseIP(host)
		loopback := host == "localhost" || (ip != nil && ip.IsLoopback())
		if !c.DockerTLS && !loopback && !c.DockerAllowPlainHTTP {
			return c, errors.New("non-loopback Docker TCP requires TLS or explicit docker-allow-plain-http")
		}
	default:
		return c, errors.New("Docker endpoint must use unix://, npipe:// or tcp://")
	}
	return c, nil
}

func readJSON(path string, out any) error {
	raw, err := image.ReadFile(path, image.MaxMetadata)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func Open(config Config) (*Engine, error) {
	if config.Runtime == "wslc" {
		return newWSLC(config)
	}
	resolved, err := config.resolve()
	if err != nil {
		return nil, err
	}
	opts := []client.Opt{client.WithHost(resolved.DockerHost)}
	if resolved.DockerTLS {
		opts = append(opts, client.WithTLSClientConfig(resolved.DockerCA, resolved.DockerCert, resolved.DockerKey))
	}
	cli, err := client.New(opts...)
	if err != nil {
		return nil, err
	}
	return &Engine{Client: cli}, nil
}

// NativeStoreLocal never infers VM storage accessibility from daemon OSType.
func (e *Engine) NativeStoreLocal() bool {
	if e.Client == nil || runtime.GOOS != "linux" {
		return false
	}
	switch e.Client.DaemonHost() {
	case "unix:///var/run/docker.sock", "unix:///run/docker.sock":
		return true
	default:
		return false
	}
}

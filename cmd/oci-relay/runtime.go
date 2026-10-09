// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"github.com/scitrera/oci-relay/internal/engine"
)

func runtimeFlags(fs *flag.FlagSet, c *engine.Config) {
	fs.StringVar(&c.Runtime, "runtime", "docker", "container runtime: docker or experimental wslc")
	fs.StringVar(&c.DockerHost, "docker-host", "", "Docker endpoint: unix://, local npipe://, or tcp://; defaults to Docker context/environment")
	fs.StringVar(&c.DockerContext, "docker-context", "", "Docker context name")
	fs.BoolVar(&c.DockerTLS, "docker-tls", false, "require verified TLS for Docker TCP")
	fs.StringVar(&c.DockerCA, "docker-ca", "", "Docker TLS CA certificate file (default system roots)")
	fs.StringVar(&c.DockerCert, "docker-cert", "", "Docker TLS client certificate file")
	fs.StringVar(&c.DockerKey, "docker-key", "", "Docker TLS client private key file")
	fs.BoolVar(&c.DockerAllowPlainHTTP, "docker-allow-plain-http", false, "explicitly allow unencrypted non-loopback Docker TCP")
	fs.StringVar(&c.WSLCExecutable, "wslc-executable", "", "WSLC executable path (default wslc.exe)")
	fs.StringVar(&c.WSLCSession, "wslc-session", "", "WSLC session name")
}

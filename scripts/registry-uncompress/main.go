// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

// registry-uncompress is an explicit experiment, not an automatic source mode.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spark-arena/oci-relay/internal/image"
	"github.com/spark-arena/oci-relay/internal/source"
)

func main() {
	ref := flag.String("image", "", "registry image (prefer digest pinned)")
	platform := flag.String("platform", "linux/arm64", "selected platform")
	spool := flag.String("spool-dir", "", "existing directory with checked disk headroom")
	budget := flag.Int64("max-bytes", 0, "required disk cap, including metadata")
	workers := flag.Int("workers", 3, "concurrent layer decoders (1..32)")
	plain := flag.Bool("plain-http", false, "explicit trusted HTTP registry")
	config := flag.String("registry-config", "", "Docker registry credentials file")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := func() error {
		if *ref == "" || *spool == "" || *budget <= 0 {
			return fmt.Errorf("--image, --spool-dir and --max-bytes required")
		}
		p, err := image.Platform(*platform)
		if err != nil {
			return err
		}
		start := time.Now()
		r, err := source.NewRegistry(ctx, source.RegistryOptions{Reference: *ref, Platform: p, PlainHTTP: *plain, ConfigPath: *config})
		if err != nil {
			return err
		}
		defer r.Close()
		root, err := r.ExportUncompressed(ctx, *spool, *budget, *workers)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"layout": root, "preparation_seconds": time.Since(start).Seconds(),
			"registry_root_digest": r.RootDigest, "config_digest": r.Image.Descriptors[0].Digest, "registry_metrics": r.Metrics()})
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

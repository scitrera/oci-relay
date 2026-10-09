// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/buildinfo"
	"github.com/scitrera/oci-relay/internal/engine"
	"github.com/scitrera/oci-relay/internal/image"
	"github.com/scitrera/oci-relay/internal/peer"
	"github.com/scitrera/oci-relay/internal/source"
	"github.com/scitrera/oci-relay/internal/transfer"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "oci-relay:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: oci-relay {version|session|serve|run|peer|attach|prepare|inventory} [options]")
	}
	switch args[0] {
	case "version", "--version":
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"version": buildinfo.Version, "commit": buildinfo.Commit, "protocol": peer.ProtocolVersion, "capabilities": []string{"receiver-unpigz-v1", "registry-range-v1"}})
	case "session":
		fs := flags("session")
		dir := fs.String("out", "", "private output directory")
		ids := fs.String("peers", "", "comma-separated receiver identifiers")
		duration := fs.Duration("duration", 2*time.Hour, "credential lifetime")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *ids == "" {
			return errors.New("--out and --peers are required")
		}
		s, err := peer.NewSession(strings.Split(*ids, ","), *duration)
		if err != nil {
			return err
		}
		return s.Save(*dir)
	case "serve", "run", "prepare":
		return serve(ctx, args[0], args[1:])
	case "peer":
		return receive(ctx, args[1:])
	case "inventory":
		fs := flags("inventory")
		host := fs.String("docker-host", "", "local Docker Unix socket")
		timeout := fs.Int("timeout-seconds", 10, "metadata discovery budget (1 to 300 seconds)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *timeout < 1 || *timeout > 300 {
			return errors.New("invalid inventory timeout")
		}
		raw, err := image.ReadBounded(os.Stdin, image.MaxMetadata)
		if err != nil {
			return err
		}
		var request engine.InventoryRequest
		if err = json.Unmarshal(raw, &request); err != nil {
			return err
		}
		e, err := engine.New(*host)
		if err != nil {
			return err
		}
		defer e.Close()
		inventory, err := e.Discover(ctx, request, time.Duration(*timeout)*time.Second, false)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(inventory)
	case "attach":
		fs := flags("attach")
		socket := fs.String("socket", "", "source Unix socket")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *socket == "" {
			return errors.New("--socket is required")
		}
		return peer.Attach(ctx, *socket, os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

type plan struct {
	RegistryConfig              string `json:"registry_config"`
	RegistryPlainHTTP           bool   `json:"registry_plain_http"`
	RegistryCacheBytes          int64  `json:"registry_cache_bytes"`
	RegistryRangeConcurrency    int    `json:"registry_range_concurrency"`
	RegistryRangeChunkBytes     int64  `json:"registry_range_chunk_bytes"`
	RegistryRangeThresholdBytes int64  `json:"registry_range_threshold_bytes"`
	RegistryRangeBufferBytes    int64  `json:"registry_range_buffer_bytes"`
	Version                     int    `json:"version"`
	Source                      string `json:"source"`
	Image                       string `json:"image"`
	Layout                      string `json:"layout"`
	Platform                    string `json:"platform"`
	Manifest                    string `json:"manifest"`
	DockerHost                  string `json:"docker_host"`
	DockerRoot                  string `json:"docker_root"`
	EngineVersion               string `json:"engine_version"`
	SocketUID                   *int   `json:"socket_uid,omitempty"`
	SocketGID                   *int   `json:"socket_gid,omitempty"`
	SessionDir                  string `json:"session_dir"`
	Listen                      string `json:"listen"`
	Advertise                   string `json:"advertise"`
	Socket                      string `json:"socket"`
	AllowPreparationRead        bool   `json:"allow_preparation_read"`
	MaxBuffer                   int64  `json:"max_buffer_bytes"`
	MaxSpool                    int64  `json:"max_spool_bytes"`
	MaxUpload                   int64  `json:"max_upload_bytes"`
	SpoolDir                    string `json:"spool_dir"`
	SourceStreams               int    `json:"source_streams"`
	SourceJoinMilliseconds      int    `json:"source_join_milliseconds"`
	TimeoutSeconds              int    `json:"timeout_seconds"`
	LeaseSeconds                int    `json:"lease_seconds"`
	ManagedStdin                bool   `json:"managed_stdin"`
}

func serve(parent context.Context, command string, args []string) (returnErr error) {
	p := plan{Version: peer.ProtocolVersion, Source: "docker", Listen: "127.0.0.1:0", MaxBuffer: 128 << 20, MaxSpool: 8 << 30, SourceStreams: 4, TimeoutSeconds: 3600}
	fs := flags(command)
	planPath := fs.String("plan", "", "versioned JSON source plan")
	fs.StringVar(&p.Source, "source", p.Source, "docker, docker-save (full staging), docker-classic/docker-containerd (read-only store), oci-layout, or registry")
	fs.StringVar(&p.RegistryConfig, "registry-config", "", "Docker config.json on the fetcher (default Docker config location)")
	fs.BoolVar(&p.RegistryPlainHTTP, "registry-plain-http", false, "explicitly use plain HTTP for the source registry")
	fs.Int64Var(&p.RegistryCacheBytes, "registry-cache-bytes", 0, "optional retained compressed-blob disk budget (0 streams without disk cache)")
	fs.IntVar(&p.RegistryRangeConcurrency, "registry-range-concurrency", 0, "parallel upstream ranges per blob (0 defaults to 4; 1 disables)")
	fs.Int64Var(&p.RegistryRangeChunkBytes, "registry-range-chunk-bytes", 0, "upstream range size (0 defaults to 16 MiB)")
	fs.Int64Var(&p.RegistryRangeThresholdBytes, "registry-range-threshold-bytes", 0, "minimum blob size for parallel ranges (0 defaults to 256 MiB)")
	fs.Int64Var(&p.RegistryRangeBufferBytes, "registry-range-buffer-bytes", 0, "shared range buffer reserved from source memory (0 uses one quarter, capped at 128 MiB)")
	fs.StringVar(&p.DockerRoot, "docker-root", "", "read-only qualified Docker store root (or containerd content root)")
	fs.StringVar(&p.EngineVersion, "engine-version", "", "qualified source Docker engine version")
	fs.StringVar(&p.Image, "image", "", "source image or layout reference")
	fs.StringVar(&p.Layout, "layout", "", "existing OCI-layout directory")
	fs.StringVar(&p.Platform, "platform", "", "os/architecture[/variant]")
	fs.StringVar(&p.Manifest, "manifest", "", "raw platform manifest input")
	fs.StringVar(&p.DockerHost, "docker-host", "", "local Docker Unix socket")
	fs.StringVar(&p.SessionDir, "session-dir", "", "private credential directory")
	fs.StringVar(&p.Listen, "listen", p.Listen, "peer listener address")
	fs.StringVar(&p.Advertise, "advertise", "", "advertised source host/IP")
	fs.StringVar(&p.Socket, "socket", "", "private stdio attachment socket")
	fs.BoolVar(&p.AllowPreparationRead, "allow-preparation-read", false, "permit a full metadata preparation pass")
	fs.Int64Var(&p.MaxBuffer, "max-buffer-bytes", p.MaxBuffer, "source cache byte budget")
	fs.Int64Var(&p.MaxSpool, "max-spool-bytes", p.MaxSpool, "source upload spool budget")
	fs.Int64Var(&p.MaxUpload, "max-upload-bytes", 0, "per unidentified upload cap (default spool budget)")
	fs.StringVar(&p.SpoolDir, "spool-dir", "", "parent directory for owned source staging (default system temp)")
	fs.IntVar(&p.SourceStreams, "max-source-streams", p.SourceStreams, "concurrent source acquisitions")
	fs.IntVar(&p.SourceJoinMilliseconds, "source-join-milliseconds", 0, "bounded initial prefix retention for joining receivers (0 disables)")
	fs.IntVar(&p.TimeoutSeconds, "timeout-seconds", p.TimeoutSeconds, "operation deadline in seconds")
	fs.IntVar(&p.LeaseSeconds, "lease-seconds", 0, "manager lease timeout; 0 disables")
	fs.BoolVar(&p.ManagedStdin, "managed-stdin", false, "read manager heartbeat/cancel JSON lines")
	output := fs.String("output", "", "prepare output file; default stdout")
	events := fs.String("events", "jsonl", "machine event format")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *events != "jsonl" {
		return errors.New("only jsonl events are supported")
	}
	if *planPath != "" {
		raw, err := image.ReadFile(*planPath, 1<<20)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err = dec.Decode(&p); err != nil {
			return err
		}
		if err = dec.Decode(new(any)); err != io.EOF {
			return errors.New("plan must contain exactly one JSON object")
		}
	}
	if p.Version != peer.ProtocolVersion {
		return errors.New("unsupported plan version")
	}
	if p.TimeoutSeconds < 1 || p.TimeoutSeconds > 86400 || p.LeaseSeconds < 0 || p.MaxBuffer < 8<<20 || p.MaxBuffer > 64<<30 || p.SourceStreams < 1 || p.SourceStreams > 32 {
		return errors.New("invalid operation limits")
	}
	if p.SourceJoinMilliseconds < 0 || p.SourceJoinMilliseconds > 2000 {
		return errors.New("source join window must be between 0 and 2000 milliseconds")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(p.TimeoutSeconds)*time.Second)
	defer cancel()
	var running atomic.Pointer[peer.Server]
	if p.ManagedStdin {
		go func() {
			scan := bufio.NewScanner(os.Stdin)
			scan.Buffer(make([]byte, 4096), 64<<10)
			for scan.Scan() {
				var msg struct {
					Type string `json:"type"`
				}
				if json.Unmarshal(scan.Bytes(), &msg) != nil {
					cancel()
					return
				}
				switch msg.Type {
				case "heartbeat":
					if s := running.Load(); s != nil {
						s.RenewLease()
					}
				case "cancel":
					cancel()
					return
				default:
					cancel()
					return
				}
			}
			cancel()
		}()
	}
	platform, err := image.Platform(p.Platform)
	if err != nil {
		return err
	}
	var sess *peer.Session
	if command != "prepare" {
		if p.SessionDir == "" {
			return errors.New("--session-dir is required")
		}
		sess, err = peer.LoadSession(filepath.Join(p.SessionDir, "session.json"))
		if err != nil {
			return err
		}
	}
	var im *image.Image
	var src transfer.Source
	var dockerSource *source.Docker
	var archiveSource *source.Archive
	var nativeSource *source.Native
	var containerdSource *source.Containerd
	var registrySource *source.Registry
	sourceCacheBudget := p.MaxBuffer
	switch p.Source {
	case "registry":
		if p.Image == "" || p.Manifest != "" {
			return errors.New("registry requires --image and resolves its own pinned manifest")
		}
		rangeBudget := p.RegistryRangeBufferBytes
		if rangeBudget == 0 {
			rangeBudget = min(p.MaxBuffer/4, 128<<20)
		}
		registrySource, err = source.NewRegistry(ctx, source.RegistryOptions{
			Reference: p.Image, Platform: platform, ConfigPath: p.RegistryConfig, PlainHTTP: p.RegistryPlainHTTP,
			MaxCacheBytes: p.RegistryCacheBytes, SpoolDir: p.SpoolDir,
			RangeConcurrency: p.RegistryRangeConcurrency, RangeChunkBytes: p.RegistryRangeChunkBytes,
			RangeThresholdBytes: p.RegistryRangeThresholdBytes, RangeBufferBytes: rangeBudget,
		})
		if err != nil {
			return err
		}
		defer func() { returnErr = errors.Join(returnErr, registrySource.Close()) }()
		if registrySource.RangeBufferBudget() > p.MaxBuffer-(8<<20) {
			return errors.New("registry range buffer must leave at least 8 MiB for the source cache")
		}
		im, src = registrySource.Image, registrySource
		sourceCacheBudget -= registrySource.RangeBufferBudget()

	case "docker-containerd":
		if p.DockerRoot == "" || p.Manifest != "" {
			return errors.New("docker-containerd requires --docker-root and supplies its own manifest")
		}
		containerdSource, err = source.NewContainerd(ctx, p.DockerRoot, p.Image, p.EngineVersion, platform)
		if err != nil {
			return err
		}
		defer func() { returnErr = errors.Join(returnErr, containerdSource.Close()) }()
		im, src = containerdSource.Image, containerdSource
	case "docker-classic":
		if p.DockerRoot == "" || p.Manifest != "" {
			return errors.New("docker-classic requires --docker-root and supplies its own manifest")
		}
		nativeSource, err = source.NewNative(ctx, p.DockerRoot, p.Image, p.EngineVersion, platform)
		if err != nil {
			return err
		}
		defer func() { returnErr = errors.Join(returnErr, nativeSource.Close()) }()
		im, src = nativeSource.Image, nativeSource
	case "docker-save":
		if !p.AllowPreparationRead {
			return errors.New("docker-save stages a full uncompressed archive; --allow-preparation-read is required")
		}
		if p.Image == "" || p.Manifest != "" {
			return errors.New("docker-save requires --image and supplies its own OCI manifest; do not pass --manifest")
		}
		eng, err := engine.New(p.DockerHost)
		if err != nil {
			return err
		}
		defer eng.Close()
		archiveSource, err = source.NewArchive(ctx, eng, source.ArchiveOptions{Reference: p.Image, Platform: platform, MaxSpool: p.MaxSpool, SpoolDir: p.SpoolDir})
		if err != nil {
			return err
		}
		defer func() {
			returnErr = errors.Join(returnErr, archiveSource.Close())
		}()
		im, src = archiveSource.Image, archiveSource
	case "oci-layout":
		if p.Layout == "" {
			return errors.New("--layout is required")
		}
		if p.Manifest != "" {
			return errors.New("layout already supplies a manifest; select its reference")
		}
		im, err = image.LoadLayout(p.Layout, p.Image, platform)
		if err != nil {
			return err
		}
		src = &source.Layout{Root: p.Layout}
	case "docker":
		if p.Image == "" {
			return errors.New("--image is required")
		}
		var raw []byte
		if p.Manifest != "" {
			raw, err = image.ReadFile(p.Manifest, image.MaxMetadata)
			if err != nil {
				return err
			}
		}
		eng, err := engine.New(p.DockerHost)
		if err != nil {
			return err
		}
		defer eng.Close()
		dockerSource, err = source.NewDocker(ctx, eng, source.DockerOptions{Reference: p.Image, Manifest: raw, Platform: platform, AllowPreparationRead: p.AllowPreparationRead, MaxSpool: p.MaxSpool, MaxUpload: p.MaxUpload, SpoolDir: p.SpoolDir})
		if err != nil {
			return err
		}
		defer func() {
			if cleanupErr := dockerSource.Close(); cleanupErr != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("source cleanup: %w", cleanupErr))
			}
		}()
		im = dockerSource.Image
		src = dockerSource
	default:
		return errors.New("unsupported source; use docker, docker-save, docker-classic, docker-containerd, oci-layout, or registry")
	}
	if command == "prepare" {
		if *output == "" {
			_, err = os.Stdout.Write(im.Manifest)
			return err
		}
		return os.WriteFile(*output, im.Manifest, 0600)
	}
	cache, err := transfer.NewSource(ctx, src, sourceCacheBudget, p.SourceStreams)
	if err != nil {
		return err
	}
	defer cache.Close()
	cache.ConcurrentReplay = p.Source == "docker-save" || p.Source == "oci-layout" || p.Source == "docker-classic" || p.Source == "docker-containerd"
	cache.JoinReaders = len(sess.Peers) - 1 // Manager never consumes blobs.
	cache.JoinWindow = time.Duration(p.SourceJoinMilliseconds) * time.Millisecond
	if p.Socket == "" {
		p.Socket = filepath.Join(p.SessionDir, "source.sock")
	}
	s, err := peer.NewServer(ctx, sess, im, cache, p.Listen, p.Socket, time.Duration(p.LeaseSeconds)*time.Second)
	if err != nil {
		return err
	}
	defer s.Close()
	// A confined root helper may expose its private attachment socket to the
	// coordinating host user. The surrounding operation directory remains 0700.
	if p.SocketUID != nil || p.SocketGID != nil {
		if (p.Source != "docker-classic" && p.Source != "docker-containerd") || p.SocketUID == nil || p.SocketGID == nil || *p.SocketUID < 0 || *p.SocketGID < 0 {
			return errors.New("socket ownership requires a native source and nonnegative uid/gid")
		}
		if err = os.Chown(p.Socket, *p.SocketUID, *p.SocketGID); err != nil {
			return err
		}
	}
	running.Store(s)
	host, port, err := net.SplitHostPort(s.Listener.Addr().String())
	if err != nil {
		return err
	}
	if p.Advertise != "" {
		host = p.Advertise
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return errors.New("wildcard listeners require --advertise")
	}
	endpoint := "https://" + net.JoinHostPort(host, port)
	encode := json.NewEncoder(os.Stdout)
	var config v1.Image
	if err = json.Unmarshal(im.Config, &config); err != nil {
		return err
	}
	if err = encode.Encode(map[string]any{"diff_ids": config.RootFS.DiffIDs, "platform": im.Platform, "type": "ready", "version": peer.ProtocolVersion, "transfer": sess.Source.Transfer, "endpoint": endpoint, "socket": p.Socket, "manifest_digest": im.Digest, "config_digest": im.Descriptors[0].Digest}); err != nil {
		return err
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	progress := time.NewTicker(time.Second)
	defer progress.Stop()
	for {
		results := s.Results()
		if len(results) == len(sess.Peers)-1 {
			complete := true
			verified := true
			for _, r := range results {
				if r.State != "VERIFIED" {
					verified = false
				}
				if r.State != "COMPLETE" {
					complete = false
				}
			}
			state := "COMPLETE"
			if !complete {
				state = "FAILED"
			}
			if verified {
				state = "VERIFIED"
				complete = true
			}
			event := map[string]any{"type": "result", "version": peer.ProtocolVersion, "transfer": sess.Source.Transfer, "state": state, "receivers": results, "metrics": cache.Metrics()}
			if dockerSource != nil {
				event["preparation_bytes"] = dockerSource.PreparationBytes.Load()
				event["push_rounds"] = dockerSource.Rounds.Load()
				event["peak_spool_reserved_bytes"] = dockerSource.PeakSpool.Load()
			}
			if archiveSource != nil {
				event["preparation_bytes"] = archiveSource.Bytes
				event["preparation_seconds"] = archiveSource.PreparationSeconds
				event["export_first_byte_seconds"] = archiveSource.FirstByteSeconds
				event["peak_spool_reserved_bytes"] = archiveSource.Bytes
			}
			if registrySource != nil {
				event["registry_metrics"] = registrySource.Metrics()
				event["registry_root_digest"] = registrySource.RootDigest
				event["preparation_seconds"] = registrySource.PreparationSeconds
			}
			if nativeSource != nil {
				event["preparation_bytes"] = nativeSource.MetadataBytes
				event["preparation_seconds"] = nativeSource.PreparationSeconds
				event["peak_spool_reserved_bytes"] = 0
			}
			if containerdSource != nil {
				event["preparation_bytes"] = containerdSource.MetadataBytes
				event["preparation_seconds"] = containerdSource.PreparationSeconds
				event["peak_spool_reserved_bytes"] = 0
			}
			if err = encode.Encode(event); err != nil {
				return err
			}
			if !complete {
				return errors.New("one or more receivers failed")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			_ = encode.Encode(map[string]any{"type": "result", "version": peer.ProtocolVersion, "transfer": sess.Source.Transfer, "state": "CANCELLED", "error": ctx.Err().Error(), "receivers": results, "metrics": cache.Metrics()})
			return ctx.Err()
		case <-s.Done():
			_ = encode.Encode(map[string]any{"type": "result", "version": peer.ProtocolVersion, "transfer": sess.Source.Transfer, "state": "FAILED", "error": "manager lease expired", "receivers": results, "metrics": cache.Metrics()})
			return errors.New("manager lease expired")
		case <-ticker.C:
		case <-progress.C:
			event := map[string]any{"type": "progress", "version": peer.ProtocolVersion, "transfer": sess.Source.Transfer, "completed_receivers": len(results), "metrics": cache.Metrics(), "receiver_progress": s.Progress(), "receivers": results}
			if registrySource != nil {
				event["registry_metrics"] = registrySource.Metrics()
			}
			if err = encode.Encode(event); err != nil {
				return err
			}
		}
	}
}
func receive(parent context.Context, args []string) error {
	fs := flags("peer")
	endpoint := fs.String("endpoint", "", "HTTPS source endpoint")
	session := fs.String("session", "", "receiver credential file")
	stdio := fs.Bool("stdio", false, "carry TLS/HTTP2 over stdin/stdout")
	check := fs.Bool("check", false, "verify source metadata and transport without starting Docker")
	verify := fs.Bool("transfer-only", false, "download and verify every unique blob without Docker import (reports VERIFIED)")
	connections := fs.Int("connections-per-path", 1, "independent HTTP/2 connections per explicit data path (1 to 4)")
	stripeThreshold := fs.Int64("stripe-threshold-bytes", 256<<20, "stripe blobs at or above this size over available connections; 0 disables (otherwise 8 MiB to 1 PiB)")
	stripeStreams := fs.Int("stripe-streams", 4, "maximum independent connections per striped blob (2 to 8)")
	stripePiece := fs.Int64("stripe-piece-bytes", 1<<20, "stripe piece size (power of two, 1 to 64 MiB)")
	http2Window := fs.Int64("http2-stream-window-bytes", 0, "experimental receive window per HTTP/2 stream (0: Go default; powers of two, 1 to 64 MiB); additional transport memory")
	var paths []peer.Path
	fs.Func("path", "HTTPS_ENDPOINT,LOCAL_IP data route; repeat to distribute layers and large-layer stripes across links", func(value string) error {
		endpoint, local, ok := strings.Cut(value, ",")
		if !ok {
			return errors.New("path requires HTTPS_ENDPOINT,LOCAL_IP")
		}
		paths = append(paths, peer.Path{Endpoint: endpoint, LocalAddress: local})
		return nil
	})
	timeout := fs.Int("timeout-seconds", 3600, "transfer deadline")
	options := peer.PullOptions{Retries: 2}
	fs.StringVar(&options.Decode.Mode, "decoder", "none", "receiver decoder: none, auto or unpigz (overlay2 pull)")
	fs.StringVar(&options.Decode.Helper, "unpigz", "", "absolute verified decoder executable")
	fs.StringVar(&options.Decode.Directory, "decode-spool-dir", "", "private receiver scratch parent directory")
	fs.IntVar(&options.Decode.Workers, "decode-workers", 4, "parallel missing-layer decoders (1..16)")
	fs.Int64Var(&options.Decode.MaxBytes, "max-decode-bytes", 0, "explicit raw layer scratch budget")
	fs.Int64Var(&options.Decode.ReserveBytes, "decode-reserve-bytes", 16<<30, "disk reserve beyond scratch and Docker import allowance")
	inventoryPath := fs.String("cache-inventory", "", "private JSON cache discovery hints from the inventory command")
	fs.StringVar(&options.Tag, "tag", "", "destination image tag")
	fs.StringVar(&options.DockerHost, "docker-host", "", "local Docker Unix socket")
	fs.StringVar(&options.NativeStore, "native-store", "", "qualified receiver native store: overlay2 or containerd")
	fs.StringVar(&options.NativeBase, "native-base", "", "optional pinned local base image ID from coordinator inventory")
	fs.StringVar(&options.NativeRoot, "native-root", "", "read-only receiver store/content root")
	fs.StringVar(&options.EngineVersion, "engine-version", "", "qualified receiver Docker version")
	fs.BoolVar(&options.SkipPresent, "skip-present", false, "skip import when the destination tag already has the exact config ID/platform")
	fs.BoolVar(&options.ReplaceTag, "replace-tag", false, "replace a conflicting destination tag")
	fs.Int64Var(&options.Memory, "max-buffer-bytes", 128<<20, "receiver cache byte budget")
	fs.IntVar(&options.Parallel, "max-source-streams", 4, "receiver source streams")
	fs.IntVar(&options.Retries, "max-retries", 2, "receiver pull retries")
	fs.StringVar(&options.Import, "import", "pull", "receiver import: pull, load-cached, or experimental load")
	fs.Int64Var(&options.MaxImport, "max-import-bytes", 0, "explicit Docker load scratch archive byte budget; required for load modes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *timeout < 1 || *timeout > 86400 {
		return errors.New("invalid timeout")
	}
	if (*stripeThreshold != 0 && (*stripeThreshold < 8<<20 || *stripeThreshold > 1<<50)) || *stripeStreams < 2 || *stripeStreams > 8 {
		return errors.New("invalid stripe threshold or stream limit")
	}
	if *stripePiece < 1<<20 || *stripePiece > 64<<20 || *stripePiece&(*stripePiece-1) != 0 {
		return errors.New("stripe piece size must be a power of two between 1 and 64 MiB")
	}
	if *inventoryPath != "" {
		f, err := os.Open(*inventoryPath)
		if err != nil {
			return err
		}
		raw, readErr := image.ReadBounded(f, image.MaxMetadata)
		closeErr := f.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		options.Inventory = &engine.Inventory{}
		if err := json.Unmarshal(raw, options.Inventory); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(*timeout)*time.Second)
	defer cancel()
	creds, err := peer.LoadCredentials(*session)
	if err != nil {
		return err
	}
	var connection net.Conn
	if *stdio {
		connection = &peer.StdioConn{In: os.Stdin, Out: os.Stdout}
	}
	var c *peer.Client
	if len(paths) != 0 {
		if *stdio || *endpoint != "" {
			return errors.New("--path cannot be combined with --stdio or --endpoint")
		}
		c, err = peer.NewPathClientConnections(paths, creds, *connections)
	} else {
		if *connections != 1 {
			return errors.New("--connections-per-path requires --path")
		}
		c, err = peer.NewClient(*endpoint, creds, connection)
	}
	if err != nil {
		return err
	}
	defer c.Close()
	c.StripeThreshold, c.StripeStreams = *stripeThreshold, *stripeStreams
	c.StripePieceBytes = *stripePiece
	if err := c.ConfigureHTTP2Window(*http2Window); err != nil {
		return err
	}
	if *check {
		im, err := c.Image(ctx)
		if err != nil {
			return err
		}
		out := os.Stdout
		if *stdio {
			out = os.Stderr
		}
		return json.NewEncoder(out).Encode(map[string]any{"state": "READY", "version": peer.ProtocolVersion, "manifest_digest": im.Digest})
	}
	var result peer.Result
	var pullErr error
	if *verify {
		result, pullErr = peer.Verify(ctx, c, options)
	} else {
		result, pullErr = peer.Pull(ctx, c, options)
	}
	reportCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	reportErr := c.Report(reportCtx, result)
	var out io.Writer = os.Stdout
	if *stdio {
		out = os.Stderr
	}
	if err = json.NewEncoder(out).Encode(result); err != nil {
		return err
	}
	if pullErr != nil {
		return pullErr
	}
	return reportErr
}

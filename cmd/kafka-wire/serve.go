package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/csnyder256/kafka-wire/internal/admin"
	"github.com/csnyder256/kafka-wire/internal/broker"
	"github.com/csnyder256/kafka-wire/internal/config"
	"github.com/csnyder256/kafka-wire/internal/metrics"
	"github.com/csnyder256/kafka-wire/internal/objstore"
	"github.com/csnyder256/kafka-wire/internal/storage"
	"github.com/csnyder256/kafka-wire/internal/tiering"
	"github.com/csnyder256/kafka-wire/internal/wire"
)

// bootstrapFlags are the handful of settings someone needs before a config
// file is in play. Everything else is set in the file or the environment;
// mirroring 40 settings as 40 flags is how a CLI becomes unreadable.
func bootstrapFlags(fs *flag.FlagSet) map[string]*string {
	return map[string]*string{
		"storage.datadir":    fs.String("data-dir", "", "directory for topics, groups, and metadata"),
		"listeners.kafka":    fs.String("kafka-listen", "", "address for the Kafka protocol listener"),
		"listeners.admin":    fs.String("admin-listen", "", "address for the HTTP admin and metrics listener"),
		"log.level":          fs.String("log-level", "", "debug, info, warn, or error"),
	}
}

func loadForCommand(fs *flag.FlagSet, args []string) (*config.Loaded, error) {
	cfgPath := fs.String("config", "", "path to a configuration file")
	flags := bootstrapFlags(fs)
	if err := fs.Parse(permute(fs, args)); err != nil {
		return nil, err
	}
	overrides := map[string]string{}
	for path, v := range flags {
		if *v != "" {
			overrides[path] = *v
		}
	}
	return config.Load(config.Options{File: *cfgPath, Overrides: overrides})
}

func cmdServe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `kafka-wire serve - run the broker

  Reads configuration from, in increasing order of precedence: compiled-in
  defaults, a config file, KAFKA_WIRE_* environment variables, then the flags
  below. Run "kafka-wire config print" to see the result and where each value
  came from.

FLAGS
`)
		fs.PrintDefaults()
	}
	loaded, err := loadForCommand(fs, args)
	if err != nil {
		return err
	}
	cfg := loaded.Config

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	if cfg.Limits.MemoryBytes > 0 {
		// Reserve 20 percent as headroom; the rest is the GC budget.
		debug.SetMemoryLimit(int64(float64(cfg.Limits.MemoryBytes) * 0.80))
	}

	store, err := storage.Open(storage.Config{
		DataDir:       cfg.Storage.DataDir,
		SegmentBytes:  cfg.Storage.SegmentBytes,
		SegmentMS:     cfg.Storage.SegmentAge.Milliseconds(),
		IndexInterval: cfg.Storage.IndexInterval,
	})
	if err != nil {
		return fmt.Errorf("opening the data directory %s: %w\n"+
			"  The broker needs a writable directory. Set storage.datadir or --data-dir",
			cfg.Storage.DataDir, err)
	}
	defer store.Close()

	mreg := metrics.New()

	// A full disk is the failure that turns into silent write errors or torn
	// batches, so the guard pauses appends before the filesystem does it for
	// us. Setting storage.diskfreemin to 0 opts out.
	if cfg.Storage.DiskFreeMin > 0 {
		guard := storage.NewDiskGuard(cfg.Storage.DataDir, cfg.Storage.DiskFreeMin)
		guard.OnState = mreg.SetDiskState
		store.AttachDiskGuard(guard)
		go guard.Run()
		defer guard.Stop()
	}

	advHost, advPort := advertised(cfg)
	brk := broker.New(broker.Config{
		BrokerID:        cfg.Cluster.BrokerID,
		ClusterID:       cfg.Cluster.ID,
		AdvertisedHost:  advHost,
		AdvertisedPort:  advPort,
		DataDir:         cfg.Storage.DataDir,
		MaxRequestBytes: cfg.Limits.MaxRequestBytes,
		Storage:         store,
		Metrics:         mreg,
	})
	if err := brk.LoadState(); err != nil {
		return fmt.Errorf("recovering broker state from %s: %w", cfg.Storage.DataDir, err)
	}

	go storage.RunRetention(brk.Topics(), storage.RetentionConfig{
		RetentionMS:    cfg.Storage.RetentionAge.Milliseconds(),
		RetentionBytes: cfg.Storage.RetentionSize,
		Tick:           60 * time.Second,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Cold storage. Absent by default, and when present it is chosen purely
	// by configuration: nothing above this call knows which backend won.
	backend, err := openArchive(runCtx, cfg)
	if err != nil {
		return err
	}
	if backend != nil {
		manifest, err := tiering.OpenManifest(cfg.Storage.DataDir)
		if err != nil {
			return fmt.Errorf("opening the archive manifest: %w", err)
		}
		cache, err := tiering.NewCache(filepath.Join(cfg.Storage.DataDir, "cache"), cfg.Archive.CacheBytes)
		if err != nil {
			return fmt.Errorf("opening the restore cache: %w", err)
		}
		uploader := tiering.NewUploader(tiering.Config{
			Prefix:         cfg.Archive.Prefix,
			ArchiveAge:     cfg.Archive.Age,
			LocalRetention: cfg.Archive.LocalRetention,
			PartSize:       cfg.Archive.S3.PartSize,
			Tick:           30 * time.Second,
			Concurrency:    cfg.Archive.Concurrency,
		}, backend, manifest, mreg)
		go uploader.Run(runCtx, brk.Topics())
		brk.AttachRestorer(tiering.NewRestorer("", cache, manifest, backend, mreg), manifest, cache)
		logger.Info("archive.enabled", "backend", backend.Name(), "prefix", cfg.Archive.Prefix)
	}

	listener, err := kafkaListener(cfg)
	if err != nil {
		return err
	}
	defer listener.Close()

	dispatcher := wire.NewDispatcher(brk, mreg, wire.Config{
		MaxRequestBytes: cfg.Limits.MaxRequestBytes,
		SaslEnabled:     cfg.Auth.SASLEnabled,
		UsersFile:       cfg.Auth.UsersFile,
		MaxConnections:  cfg.Limits.MaxConnections,
	})
	go acceptLoop(runCtx, listener, dispatcher)

	var httpSrv *http.Server
	if cfg.Admin.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", mreg.Handler())
		admin.Register(mux, brk, mreg, admin.Config{AdminToken: cfg.Admin.Token})
		httpSrv = &http.Server{
			Addr:              cfg.Listeners.Admin,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin.listen_failed", "err", err, "addr", cfg.Listeners.Admin)
			}
		}()
	}

	printBanner(cfg, loaded, advHost, advPort, backend)

	<-runCtx.Done()
	logger.Info("shutdown.signalled")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.Shutdown.Grace)
	defer shutCancel()
	listener.Close()
	cancel()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutCtx)
	}
	if err := brk.Drain(shutCtx); err != nil {
		logger.Warn("shutdown.drain_incomplete", "err", err)
	}
	logger.Info("shutdown.complete")
	return nil
}

// openArchive turns configuration into a backend, or nil when cold storage is
// off. This is the only place in the program that maps a backend name onto an
// implementation.
func openArchive(ctx context.Context, cfg config.Config) (objstore.Store, error) {
	switch cfg.Archive.Backend {
	case "none", "":
		return nil, nil
	case "fs":
		st, err := objstore.NewFS(cfg.Archive.FS.Path)
		if err != nil {
			return nil, fmt.Errorf("archive.backend is fs: %w", err)
		}
		return st, nil
	case "s3":
		st, err := objstore.NewS3(ctx, objstore.S3Config{
			Bucket:       cfg.Archive.S3.Bucket,
			Endpoint:     cfg.Archive.S3.Endpoint,
			Region:       cfg.Archive.S3.Region,
			Addressing:   cfg.Archive.S3.Addressing,
			AccessKey:    cfg.Archive.S3.AccessKey,
			SecretKey:    cfg.Archive.S3.SecretKey,
			SessionToken: cfg.Archive.S3.SessionToken,
			Insecure:     cfg.Archive.S3.Insecure,
			CAFile:       cfg.Archive.S3.CAFile,
			SkipVerify:   cfg.Archive.S3.SkipVerify,
			StorageClass: cfg.Archive.S3.StorageClass,
		})
		if err != nil {
			return nil, err
		}
		return st, nil
	default:
		return nil, fmt.Errorf("archive.backend is %q; valid values are none, fs, s3", cfg.Archive.Backend)
	}
}

func kafkaListener(cfg config.Config) (net.Listener, error) {
	if cfg.TLS.CertFile == "" {
		l, err := net.Listen("tcp", cfg.Listeners.Kafka)
		if err != nil {
			return nil, listenError("listeners.kafka", cfg.Listeners.Kafka, err)
		}
		return l, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading the TLS keypair (tls.certfile, tls.keyfile): %w", err)
	}
	tc := &tls.Config{Certificates: []tls.Certificate{cert}}
	if cfg.TLS.MinVersion == "1.3" {
		tc.MinVersion = tls.VersionTLS13
	} else {
		tc.MinVersion = tls.VersionTLS12
	}
	if cfg.TLS.ClientCA != "" {
		pem, err := os.ReadFile(cfg.TLS.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("reading tls.clientca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls.clientca %s contains no usable certificate", cfg.TLS.ClientCA)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	l, err := tls.Listen("tcp", cfg.Listeners.Kafka, tc)
	if err != nil {
		return nil, listenError("listeners.kafka", cfg.Listeners.Kafka, err)
	}
	return l, nil
}

// listenError turns the two failures that account for nearly every failed
// start into an explanation instead of a syscall name.
func listenError(setting, addr string, err error) error {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "address already in use") || strings.Contains(msg, "Only one usage of each socket address"):
		return fmt.Errorf("%s: %s is already in use.\n"+
			"  Something else is on that port. Either stop it, or choose another port:\n"+
			"    kafka-wire serve --kafka-listen 127.0.0.1:9192", setting, addr)
	case strings.Contains(msg, "permission denied"):
		return fmt.Errorf("%s: not allowed to bind %s.\n"+
			"  Ports below 1024 need elevated privileges on most systems. Pick a higher port", setting, addr)
	}
	return fmt.Errorf("%s: cannot listen on %s: %w", setting, addr, err)
}

func acceptLoop(ctx context.Context, l net.Listener, d *wire.Dispatcher) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("accept.failed", "err", err)
			continue
		}
		go d.Serve(ctx, conn)
	}
}

// advertised resolves what clients will be told to connect to.
//
// This is the setting that causes the most confusing failure in any Kafka
// deployment: a client connects to the bootstrap address, receives Metadata,
// throws away the address it used, and reconnects to whatever the broker
// advertised. If that is wrong, the client hangs or loops on connection
// refused while the broker log looks perfectly healthy.
func advertised(cfg config.Config) (string, int32) {
	host := cfg.Listeners.AdvertisedHost
	port := int32(cfg.Listeners.AdvertisedPort)
	bindHost, bindPort := splitHostPort(cfg.Listeners.Kafka)
	if host == "" {
		host = bindHost
		if host == "0.0.0.0" || host == "" || host == "::" {
			host = "localhost"
		}
	}
	if port == 0 {
		port = bindPort
	}
	return host, port
}

func splitHostPort(addr string) (string, int32) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 9092
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return h, 9092
	}
	return h, int32(n)
}

func newLogger(c config.LogConfig) *slog.Logger {
	var lvl slog.Level
	switch c.Level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	if c.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}

// printBanner tells the operator the one thing they need next: the address to
// paste into their client. A broker that starts silently leaves the user
// guessing whether it worked.
func printBanner(cfg config.Config, loaded *config.Loaded, advHost string, advPort int32, backend objstore.Store) {
	bootstrap := net.JoinHostPort(advHost, strconv.Itoa(int(advPort)))
	archive := "off"
	if backend != nil {
		archive = backend.Name()
	}
	src := "built-in defaults"
	if loaded.File != "" {
		src = loaded.File
	}
	auth := "disabled"
	if cfg.Auth.SASLEnabled {
		auth = "SASL/SCRAM"
	}

	fmt.Printf(`
  kafka-wire %s  ready

  bootstrap servers   %s
  data directory      %s
  configuration       %s
  authentication      %s
  cold storage        %s

  Try it:
    kafka-wire topic create demo
    echo hello | kafka-wire produce demo
    kafka-wire consume demo --from-beginning

`, version, bootstrap, cfg.Storage.DataDir, src, auth, archive)

	if cfg.Auth.AllowAnon && !cfg.Auth.SASLEnabled {
		fmt.Printf("  Note: auth.allowanon is set and authentication is off, so anyone who can\n"+
			"  reach %s can read and write every topic. Keep it on a private network.\n\n",
			cfg.Listeners.Kafka)
	}
}

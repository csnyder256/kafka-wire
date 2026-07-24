package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/csnyder256/kafka-wire/internal/config"
	"github.com/csnyder256/kafka-wire/internal/objstore"
)

// cmdDoctor checks the things that actually go wrong, in the order they go
// wrong, and says what to do about each. It exists because "it does not work"
// is the most common issue report any self-hosted project receives, and most
// of those reports are answerable by four checks.
func cmdDoctor(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	loaded, err := loadForCommand(fs, args)
	if err != nil {
		// A configuration that will not load is itself the diagnosis, and
		// the loader already explained it.
		fmt.Println("configuration: FAIL")
		return err
	}
	cfg := loaded.Config

	var problems int
	check := func(name string, err error, hint string) {
		if err == nil {
			fmt.Printf("  ok    %s\n", name)
			return
		}
		problems++
		fmt.Printf("  FAIL  %s\n        %v\n", name, err)
		if hint != "" {
			fmt.Printf("        %s\n", hint)
		}
	}

	src := "built-in defaults"
	if loaded.File != "" {
		src = loaded.File
	}
	fmt.Printf("kafka-wire doctor\n\nconfiguration: %s\n\n", src)

	// 1. The data directory has to exist, be writable, and have room.
	check("data directory is writable", writableDir(cfg.Storage.DataDir),
		fmt.Sprintf("create it, or point storage.datadir somewhere writable (currently %s)", cfg.Storage.DataDir))

	// 2. The ports have to be free, or the broker cannot start.
	check("kafka port is free", portFree(cfg.Listeners.Kafka),
		"stop whatever is holding the port, or set listeners.kafka to another one")
	if cfg.Admin.Enabled {
		check("admin port is free", portFree(cfg.Listeners.Admin),
			"stop whatever is holding the port, or set listeners.admin to another one")
	}

	// 3. The advertised address is what clients will actually dial. Getting
	//    this wrong is the classic Kafka failure: the broker looks healthy
	//    and every client hangs.
	advHost, advPort := advertised(cfg)
	fmt.Printf("  info  clients will be told to connect to %s\n", net.JoinHostPort(advHost, fmt.Sprint(advPort)))
	if advHost == "localhost" || advHost == "127.0.0.1" {
		fmt.Printf("        Only clients on this machine can reach that. If the broker is in a\n" +
			"        container or on another host, set listeners.advertisedhost.\n")
	}

	// 4. Cold storage credentials and reachability, if configured.
	switch cfg.Archive.Backend {
	case "", "none":
		fmt.Printf("  info  cold storage is off (archive.backend: none)\n")
	case "fs":
		_, err := objstore.NewFS(cfg.Archive.FS.Path)
		check("archive directory is usable", err, "check archive.fs.path exists and is writable")
	case "s3":
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := objstore.NewS3(cctx, objstore.S3Config{
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
		})
		check("object store is reachable", err, "")
	}

	// 5. Security posture, reported rather than enforced, since the config
	//    validator already refuses the genuinely dangerous combinations.
	if !cfg.Auth.SASLEnabled {
		if cfg.Auth.AllowAnon {
			fmt.Printf("  warn  authentication is off and auth.allowanon is set.\n" +
				"        Anyone who can reach the Kafka port has full access.\n")
		} else {
			fmt.Printf("  info  authentication is off, and the broker is bound to loopback only\n")
		}
	}

	fmt.Println()
	if problems > 0 {
		return fmt.Errorf("%d check(s) failed", problems)
	}
	fmt.Println("no problems found")
	return nil
}

func writableDir(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	probe := filepath.Join(dir, ".kafka-wire-doctor")
	if err := os.WriteFile(probe, []byte("ok"), 0o640); err != nil {
		return err
	}
	return os.Remove(probe)
}

func portFree(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return l.Close()
}

// ensure the config import is used even if checks are trimmed later
var _ = config.EnvPrefix

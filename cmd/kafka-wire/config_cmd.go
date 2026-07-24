package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/csnyder256/kafka-wire/internal/config"
)

const configUsage = `kafka-wire config - inspect and generate configuration

  config init [-o FILE]    write a starter configuration file
  config print [--format]  show the resolved configuration and where each value came from
  config validate          load the configuration and report any problems
  config reference         print every setting, its type, default, and meaning

  Formats for "print": yaml (default), env, table
`

func cmdConfig(ctx context.Context, args []string) error {
	if len(args) == 0 {
		fmt.Print(configUsage)
		return nil
	}
	switch args[0] {
	case "init":
		return configInit(args[1:])
	case "print":
		return configPrint(args[1:])
	case "validate":
		return configValidate(args[1:])
	case "reference":
		return configReference()
	case "-h", "--help", "help":
		fmt.Print(configUsage)
		return nil
	}
	return fmt.Errorf("kafka-wire config: unknown subcommand %q\n\n%s", args[0], configUsage)
}

// starterConfig is deliberately short. The argument against a configuration
// file that lists every possible setting is that nobody reads it and everyone
// copies it wholesale, so a value they never chose ends up in production.
// Everything not written here has a documented default; run
// "kafka-wire config reference" to see all of them.
const starterConfig = `# kafka-wire configuration
#
# Every setting here has an environment variable equivalent: uppercase the
# dotted path, replace each "." with "_", and prefix it with KAFKA_WIRE_.
#   storage.datadir  ->  KAFKA_WIRE_STORAGE_DATADIR
# Environment variables win over this file, so an orchestrator can always
# override what is baked into an image.
#
# Full reference:  kafka-wire config reference
# What is in use:  kafka-wire config print

listeners:
  # Bind to localhost by default. To accept clients from other machines,
  # change this AND turn on authentication (see auth below), or the broker
  # will refuse to start.
  kafka: "127.0.0.1:9092"
  admin: "127.0.0.1:8080"

  # Clients reconnect to whatever the broker advertises, not to the address
  # they dialed. Set this when the broker is behind NAT, a load balancer, or
  # a container port mapping.
  # advertisedhost: "kafka.example.com"
  # advertisedport: 9092

storage:
  datadir: "./kafka-wire-data"
  # retentionage: 168h
  # segmentbytes: 1GiB

# Cold storage. Leave the backend as "none" and everything stays on local
# disk. "fs" archives to a directory, which can be an NFS or SMB mount.
# "s3" is any S3-compatible store, not just AWS.
# archive:
#   backend: s3
#   prefix: "kafka-wire/"
#   s3:
#     bucket: "my-bucket"
#     endpoint: "http://minio:9000"   # omit for AWS
#     region: "us-east-1"             # "auto" for Cloudflare R2 and Tigris
#     addressing: "path"              # MinIO, Ceph and SeaweedFS want path
#     # Prefer KAFKA_WIRE_ARCHIVE_S3_SECRETKEY_FILE over writing secrets here.

# auth:
#   saslenabled: true
#   usersfile: "./users.json"
`

func configInit(args []string) error {
	fs := flag.NewFlagSet("config init", flag.ContinueOnError)
	out := fs.String("o", "kafka-wire.yaml", "file to write")
	force := fs.Bool("force", false, "overwrite an existing file")
	if err := fs.Parse(permute(fs, args)); err != nil {
		return err
	}
	if _, err := os.Stat(*out); err == nil && !*force {
		return fmt.Errorf("%s already exists. Pass -force to overwrite it, or -o to choose another path", *out)
	}
	if err := os.WriteFile(*out, []byte(starterConfig), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s\n\nNext:\n  kafka-wire config validate --config %s\n  kafka-wire serve --config %s\n", *out, *out, *out)
	return nil
}

func configPrint(args []string) error {
	fs := flag.NewFlagSet("config print", flag.ContinueOnError)
	format := fs.String("format", "yaml", "yaml, env, or table")
	showSecrets := fs.Bool("show-secrets", false, "print secret values instead of redacting them")
	loaded, err := loadForCommand(fs, args)
	if err != nil {
		return err
	}

	switch *format {
	case "env":
		for _, f := range config.Fields() {
			fmt.Printf("%s=%s\n", f.Env, valueOf(loaded, f, *showSecrets))
		}
	case "table":
		fmt.Printf("%-40s %-28s %s\n", "SETTING", "VALUE", "SOURCE")
		for _, f := range config.Fields() {
			fmt.Printf("%-40s %-28s %s\n", f.Path, valueOf(loaded, f, *showSecrets), loaded.Sources[f.Path])
		}
	case "yaml":
		printYAML(loaded, *showSecrets)
	default:
		return fmt.Errorf("unknown format %q; valid formats are yaml, env, table", *format)
	}
	return nil
}

// printYAML emits the resolved configuration as valid YAML, with the
// provenance of every value in a trailing comment. That last part is the
// point: "why is this setting what it is" should be answerable by reading the
// output, not by reasoning about which layer won.
func printYAML(loaded *config.Loaded, showSecrets bool) {
	// Emit each path segment the first time it is seen. Fields() returns
	// declaration order, which keeps every group contiguous, so a seen-set is
	// enough to place the headers correctly.
	seen := map[string]bool{}
	for _, f := range config.Fields() {
		parts := strings.Split(f.Path, ".")
		for depth := 0; depth < len(parts)-1; depth++ {
			prefix := strings.Join(parts[:depth+1], ".")
			if !seen[prefix] {
				seen[prefix] = true
				fmt.Printf("%s%s:\n", strings.Repeat("  ", depth), parts[depth])
			}
		}
		indent := strings.Repeat("  ", len(parts)-1)
		leaf := parts[len(parts)-1]
		fmt.Printf("%s%s: %-26s # %s\n",
			indent, leaf, quoteIfNeeded(valueOf(loaded, f, showSecrets)), loaded.Sources[f.Path])
	}
}

func quoteIfNeeded(v string) string {
	if v == "" {
		return `""`
	}
	if strings.ContainsAny(v, ": #") {
		return `"` + v + `"`
	}
	return v
}

func valueOf(loaded *config.Loaded, f config.Field, showSecrets bool) string {
	v := reflect.ValueOf(loaded.Config)
	for _, seg := range strings.Split(f.Path, ".") {
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Tag.Get("yaml") == seg {
				v = v.Field(i)
				break
			}
		}
	}
	if f.Secret && !showSecrets {
		if v.Kind() == reflect.String && v.String() == "" {
			return ""
		}
		return "[redacted]"
	}
	if v.Type() == reflect.TypeOf(time.Duration(0)) {
		return time.Duration(v.Int()).String()
	}
	return fmt.Sprintf("%v", v.Interface())
}

func configValidate(args []string) error {
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	loaded, err := loadForCommand(fs, args)
	if err != nil {
		return err
	}
	where := "built-in defaults"
	if loaded.File != "" {
		where = loaded.File
	}
	fmt.Printf("configuration is valid (%s)\n", where)
	return nil
}

func configReference() error {
	fmt.Println("Every setting kafka-wire understands.")
	fmt.Println("Set any of them in the config file by its path, or in the environment by its variable.")
	fmt.Println()
	var section string
	for _, f := range config.Fields() {
		if s := strings.SplitN(f.Path, ".", 2)[0]; s != section {
			section = s
			fmt.Printf("\n%s\n%s\n", strings.ToUpper(section), strings.Repeat("-", len(section)))
		}
		def := f.Default
		if def == "" {
			def = `""`
		}
		fmt.Printf("\n  %s  (%s, default %s)\n    %s\n    env: %s\n", f.Path, f.Kind, def, f.Doc, f.Env)
	}
	return nil
}

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Options controls one Load call.
type Options struct {
	// File is an explicit config path. Empty means consult $KAFKA_WIRE_CONFIG
	// and then SearchPaths(). A path given explicitly must exist; a path
	// found by searching may legitimately be absent.
	File string

	// Overrides are the bootstrap flags, keyed by dotted path. Highest
	// precedence.
	Overrides map[string]string

	// LookupEnv is os.LookupEnv by default. Injected so tests do not have to
	// mutate the process environment.
	LookupEnv func(string) (string, bool)

	// ReadFile is os.ReadFile by default. Injected for the same reason.
	ReadFile func(string) ([]byte, error)
}

// Load resolves the full configuration and reports where every value came
// from. It returns an error listing every problem found, not just the first,
// because a user fixing a config wants the whole list in one run.
func Load(opts Options) (*Loaded, error) {
	if opts.LookupEnv == nil {
		opts.LookupEnv = os.LookupEnv
	}
	if opts.ReadFile == nil {
		opts.ReadFile = os.ReadFile
	}

	cfg := Default()
	sources := map[string]string{}
	for _, f := range Fields() {
		sources[f.Path] = "default"
	}

	var problems []string

	// ---- layer 2: config file -------------------------------------------
	filePath, mustExist := opts.File, opts.File != ""
	if filePath == "" {
		if v, ok := opts.LookupEnv(EnvPrefix + "_CONFIG"); ok && v != "" {
			filePath, mustExist = v, true
		}
	}
	usedFile := ""
	if filePath == "" {
		for _, p := range SearchPaths() {
			if _, err := os.Stat(p); err == nil {
				filePath = p
				break
			}
		}
	}
	if filePath != "" {
		raw, err := opts.ReadFile(filePath)
		switch {
		case err != nil && mustExist:
			return nil, fmt.Errorf("config file %s: %w", filePath, err)
		case err != nil:
			// A file found by searching that vanished underneath us is not
			// worth failing over.
		default:
			flat, ferr := flattenYAML(raw)
			if ferr != nil {
				return nil, fmt.Errorf("config file %s: %w", filePath, ferr)
			}
			usedFile = filePath
			known := knownPaths()
			for _, path := range sortedKeys(flat) {
				if !known[path] {
					problems = append(problems, fmt.Sprintf(
						"%s: unknown setting %q%s", filePath, path, suggest(path, known)))
					continue
				}
				fv, _ := fieldByPath(&cfg, path)
				if err := setField(fv, flat[path]); err != nil {
					problems = append(problems, fmt.Sprintf("%s: %s: %v", filePath, path, err))
					continue
				}
				sources[path] = "file:" + filepath.Base(filePath)
			}
		}
	}

	// ---- layers 3 and 4: environment ------------------------------------
	for _, f := range Fields() {
		// _FILE indirection first so that setting both forms is detectable.
		fileVal, hasFileVar := opts.LookupEnv(f.Env + "_FILE")
		plainVal, hasPlain := opts.LookupEnv(f.Env)

		if hasFileVar && hasPlain {
			problems = append(problems, fmt.Sprintf(
				"%s and %s are both set; pick one", f.Env, f.Env+"_FILE"))
			continue
		}

		fv, _ := fieldByPath(&cfg, f.Path)
		switch {
		case hasPlain:
			if err := setField(fv, plainVal); err != nil {
				problems = append(problems, fmt.Sprintf("%s: %v", f.Env, err))
				continue
			}
			sources[f.Path] = "env:" + f.Env
		case hasFileVar:
			b, err := opts.ReadFile(fileVal)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s points at %s: %v", f.Env+"_FILE", fileVal, err))
				continue
			}
			// Trailing newlines are near-universal in mounted secret files
			// and are never part of the secret.
			if err := setField(fv, strings.TrimRight(string(b), "\r\n")); err != nil {
				problems = append(problems, fmt.Sprintf("%s (contents of %s): %v", f.Env+"_FILE", fileVal, err))
				continue
			}
			sources[f.Path] = "file-env:" + f.Env + "_FILE"
		}
	}

	// ---- layer 5: bootstrap flags ---------------------------------------
	for _, path := range sortedKeys(opts.Overrides) {
		fv, ok := fieldByPath(&cfg, path)
		if !ok {
			problems = append(problems, fmt.Sprintf("unknown setting %q", path))
			continue
		}
		if err := setField(fv, opts.Overrides[path]); err != nil {
			problems = append(problems, fmt.Sprintf("--%s: %v", path, err))
			continue
		}
		sources[path] = "flag"
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("configuration is not usable:\n  %s", strings.Join(problems, "\n  "))
	}

	l := &Loaded{Config: cfg, Sources: sources, File: usedFile}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return l, nil
}

// knownPaths is the set of every valid dotted key path.
func knownPaths() map[string]bool {
	m := map[string]bool{}
	for _, f := range Fields() {
		m[f.Path] = true
	}
	return m
}

// suggest offers the closest known key for a typo. Getting told
// "unknown setting archive.s3.buckett" is annoying; being told
// "did you mean archive.s3.bucket" is not.
func suggest(bad string, known map[string]bool) string {
	best, bestD := "", 1<<30
	for k := range known {
		if d := editDistance(bad, k); d < bestD {
			best, bestD = k, d
		}
	}
	if best != "" && bestD <= 3 {
		return fmt.Sprintf(" (did you mean %q?)", best)
	}
	return ""
}

func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min3(cur[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		copy(prev, cur)
	}
	return prev[len(b)]
}

func min3(a, b, c int) int {
	if b < a {
		a = b
	}
	if c < a {
		a = c
	}
	return a
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// flattenYAML turns nested YAML into dotted paths with textual values, so the
// file layer feeds the exact same setField parser as the env and flag layers.
// One parser means "1GiB" and "30s" behave identically wherever they appear.
func flattenYAML(raw []byte) (map[string]string, error) {
	var root map[string]any
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	out := map[string]string{}
	var rec func(prefix string, m map[string]any) error
	rec = func(prefix string, m map[string]any) error {
		for k, v := range m {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			switch tv := v.(type) {
			case map[string]any:
				if err := rec(path, tv); err != nil {
					return err
				}
			case nil:
				out[path] = ""
			case bool:
				out[path] = fmt.Sprintf("%t", tv)
			case int, int64, float64:
				out[path] = strings.TrimSuffix(fmt.Sprintf("%v", tv), ".0")
			case string:
				out[path] = tv
			default:
				return fmt.Errorf("%s: lists and nested documents are not valid here", path)
			}
		}
		return nil
	}
	if err := rec("", root); err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// Validate rejects configurations that are syntactically fine but cannot
// work, and refuses a few that would work but are dangerous. Every message
// says what is wrong, why it matters, and what to do about it.
func (l *Loaded) Validate() error {
	c := &l.Config
	var errs []string

	add := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	if c.Storage.DataDir == "" {
		add("storage.datadir is empty; the broker has nowhere to put data")
	}
	if c.Listeners.Kafka == "" {
		add("listeners.kafka is empty; there would be nothing to connect to")
	}
	if c.Listeners.Kafka == c.Listeners.Admin {
		add("listeners.kafka and listeners.admin are both %q; they are different protocols and cannot share a port", c.Listeners.Kafka)
	}

	switch c.Storage.FsyncMode {
	case "none", "interval", "always":
	default:
		add("storage.fsyncmode is %q; valid values are none, interval, always", c.Storage.FsyncMode)
	}
	if c.Storage.DiskFreeMin < 0 || c.Storage.DiskFreeMin >= 1 {
		add("storage.diskfreemin is %v; it is a fraction and must be between 0 and 1 (0.10 means pause at 10 percent free)", c.Storage.DiskFreeMin)
	}
	if c.Storage.SegmentBytes <= 0 {
		add("storage.segmentbytes is %d; it must be positive", c.Storage.SegmentBytes)
	}

	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		add("log.level is %q; valid values are debug, info, warn, error", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		add("log.format is %q; valid values are text, json", c.Log.Format)
	}
	switch c.TLS.MinVersion {
	case "1.2", "1.3":
	default:
		add("tls.minversion is %q; valid values are 1.2 and 1.3", c.TLS.MinVersion)
	}

	// TLS is all-or-nothing. Half-configured TLS silently serves plaintext,
	// which is worse than refusing to start.
	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		add("tls.certfile and tls.keyfile must be set together; setting only one would silently serve plaintext")
	}
	if c.TLS.ClientCA != "" && c.TLS.CertFile == "" {
		add("tls.clientca requires tls.certfile and tls.keyfile; mutual TLS cannot work without server TLS")
	}

	if c.Auth.SASLEnabled && c.Auth.UsersFile == "" {
		add("auth.saslenabled is true but auth.usersfile is empty; there would be no credentials to check against. Generate one with: kafka-wire user add")
	}

	// The open-broker guard. This is the failure that turns into an incident
	// report, so it is a startup error rather than a warning.
	if isPublicBind(c.Listeners.Kafka) && !c.Auth.SASLEnabled && !c.Auth.AllowAnon {
		add("listeners.kafka is %q, which accepts connections from other machines, but auth.saslenabled is false.\n"+
			"      Anyone who can reach that port could read and write every topic.\n"+
			"      Fix it one of these ways:\n"+
			"        - bind to localhost:      listeners.kafka: 127.0.0.1:9092\n"+
			"        - turn on authentication: auth.saslenabled: true  (see docs/security.md)\n"+
			"        - accept the risk:        auth.allowanon: true    (private network only)",
			c.Listeners.Kafka)
	}
	if isPublicBind(c.Listeners.Admin) && c.Admin.Enabled && c.Admin.Token == "" && !c.Auth.AllowAnon {
		add("listeners.admin is %q but admin.token is empty, leaving the admin API open to the network.\n"+
			"      Set admin.token (or KAFKA_WIRE_ADMIN_TOKEN_FILE), bind it to 127.0.0.1, or set auth.allowanon: true",
			c.Listeners.Admin)
	}

	// Archive backend coherence.
	switch c.Archive.Backend {
	case "none":
	case "fs":
		if c.Archive.FS.Path == "" {
			add("archive.backend is fs but archive.fs.path is empty; there is nowhere to archive to")
		}
	case "s3":
		if c.Archive.S3.Bucket == "" {
			add("archive.backend is s3 but archive.s3.bucket is empty")
		}
		if c.Archive.S3.PartSize < 5*1024*1024 {
			add("archive.s3.partsize is %d; almost every S3-compatible store rejects parts smaller than 5MiB except the final one", c.Archive.S3.PartSize)
		}
		switch c.Archive.S3.Addressing {
		case "auto", "path", "virtual":
		default:
			add("archive.s3.addressing is %q; valid values are auto, path, virtual", c.Archive.S3.Addressing)
		}
		// The 10,000-part ceiling is universal across S3 implementations.
		// Hitting it means the upload dies after transferring the whole
		// segment, which is the most expensive possible way to find out.
		if c.Archive.S3.PartSize > 0 {
			parts := c.Storage.SegmentBytes / c.Archive.S3.PartSize
			if parts > 10000 {
				add("storage.segmentbytes (%d) divided by archive.s3.partsize (%d) is %d parts, over the 10000-part limit every S3 store enforces.\n"+
					"      The upload would fail only after transferring the entire segment.\n"+
					"      Raise archive.s3.partsize to at least %s, or lower storage.segmentbytes",
					c.Storage.SegmentBytes, c.Archive.S3.PartSize, parts,
					humanSize(roundUpPow2(c.Storage.SegmentBytes/10000)))
			}
		}
		if c.Archive.S3.StorageClass != "" && isArchivalClass(c.Archive.S3.StorageClass) {
			add("archive.s3.storageclass is %q. Consumers fetch directly from this bucket, so an archival class makes reads fail with InvalidObjectState until each object is restored", c.Archive.S3.StorageClass)
		}
	default:
		add("archive.backend is %q; valid values are none, fs, s3", c.Archive.Backend)
	}

	if c.Archive.Backend != "none" && c.Archive.LocalRetention > 0 && c.Archive.LocalRetention < c.Archive.Age {
		add("archive.localretention (%s) is shorter than archive.age (%s); local segments would be deleted before they are eligible for upload, losing data",
			c.Archive.LocalRetention, c.Archive.Age)
	}

	if len(errs) > 0 {
		return errors.New("configuration is not usable:\n  " + strings.Join(errs, "\n  "))
	}
	return nil
}

func isArchivalClass(s string) bool {
	switch strings.ToUpper(s) {
	case "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "SNOW":
		return true
	}
	return false
}

// isPublicBind reports whether an address accepts connections from other
// machines. Anything that is not an explicit loopback address is treated as
// public, which errs toward warning too often rather than too rarely.
func isPublicBind(addr string) bool {
	host := addr
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
	}
	host = strings.Trim(host, "[]")
	switch host {
	case "127.0.0.1", "localhost", "::1", "[::1]":
		return false
	}
	return true
}

func roundUpPow2(n int64) int64 {
	p := int64(5 * 1024 * 1024)
	for p < n {
		p *= 2
	}
	return p
}

func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%dGiB", n>>30)
	case n >= 1<<20:
		return fmt.Sprintf("%dMiB", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dKiB", n>>10)
	}
	return fmt.Sprintf("%d", n)
}

// Package config defines every tunable kafka-wire has, in one place.
//
// There is exactly one source of truth for the shape, the defaults, and the
// documentation of a setting: the Config struct below. The YAML file, the
// environment variable names, the generated reference table and the
// `kafka-wire config print` output are all derived from it by reflection, so
// they cannot drift apart.
//
// Resolution order, lowest priority to highest:
//
//  1. compiled-in defaults        (Default())
//  2. config file                 (--config, $KAFKA_WIRE_CONFIG, or a search path)
//  3. KAFKA_WIRE_* env vars       (derived: uppercase the dotted path, "." -> "_")
//  4. KAFKA_WIRE_*_FILE env vars  (same slot, value read from a file; secrets)
//  5. bootstrap flags             (--data-dir, --kafka-listen, --admin-listen, --log-level)
//
// Environment beats file on purpose. The file is usually baked into a
// container image; the environment is what an orchestrator injects at run
// time. A file that could override the orchestrator would be unusable in a
// container.
//
// There is deliberately no ${VAR} interpolation inside the YAML. The env
// override layer plus _FILE covers every case interpolation covered, without
// the surprises that led other projects to remove it.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is prepended to every derived environment variable name.
const EnvPrefix = "KAFKA_WIRE"

// Config is the complete configuration surface.
//
// Every field carries three struct tags:
//
//	yaml: the key path segment. Segments never contain "_" or "-", which is
//	      what makes the env-name mapping unambiguous in both directions.
//	def:  the compiled-in default, in the same textual form a user would write.
//	doc:  one line of reference documentation, used by `config print` and by
//	      the generated docs/configuration.md.
type Config struct {
	Listeners ListenersConfig `yaml:"listeners"`
	Storage   StorageConfig   `yaml:"storage"`
	Archive   ArchiveConfig   `yaml:"archive"`
	Limits    LimitsConfig    `yaml:"limits"`
	Auth      AuthConfig      `yaml:"auth"`
	TLS       TLSConfig       `yaml:"tls"`
	Admin     AdminConfig     `yaml:"admin"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Shutdown  ShutdownConfig  `yaml:"shutdown"`
	Log       LogConfig       `yaml:"log"`
}

type ListenersConfig struct {
	Kafka string `yaml:"kafka" def:"127.0.0.1:9092" doc:"address the Kafka protocol listener binds to. Use 0.0.0.0:9092 to accept remote clients, but read the security note first"`
	Admin string `yaml:"admin" def:"127.0.0.1:8080" doc:"address the HTTP admin and Prometheus listener binds to"`

	// AdvertisedHost and AdvertisedPort are what the broker reports in
	// Metadata responses. Clients throw away the address they connected to
	// and reconnect to whatever Metadata says, so these being wrong is the
	// single most common cause of a client that connects and then hangs.
	AdvertisedHost string `yaml:"advertisedhost" def:"" doc:"hostname clients are told to reconnect to. Empty derives it from listeners.kafka. Set this behind NAT, a load balancer, or Docker port mapping"`
	AdvertisedPort int    `yaml:"advertisedport" def:"0" doc:"port clients are told to reconnect to. 0 derives it from listeners.kafka. Set this when the published port differs from the bound port"`
}

type StorageConfig struct {
	DataDir       string        `yaml:"datadir" def:"./kafka-wire-data" doc:"directory holding topics, consumer group state, and metadata. Must be on a durable filesystem"`
	SegmentBytes  int64         `yaml:"segmentbytes" def:"1GiB" doc:"roll to a new log segment once the active one exceeds this size"`
	SegmentAge    time.Duration `yaml:"segmentage" def:"168h" doc:"roll to a new log segment once the active one is this old, even if it is not full"`
	IndexInterval int64         `yaml:"indexinterval" def:"16KiB" doc:"add a sparse index entry every this many bytes of log. Lower means faster lookups and bigger indexes"`
	RetentionAge  time.Duration `yaml:"retentionage" def:"168h" doc:"delete log segments older than this. 0 disables age-based retention"`
	RetentionSize int64         `yaml:"retentionsize" def:"-1" doc:"delete oldest segments once a partition exceeds this many bytes. -1 means unlimited"`
	FsyncMode     string        `yaml:"fsyncmode" def:"interval" doc:"durability policy: none (fastest, relies on the OS), interval (fsync on a timer), always (fsync every append, slowest and safest)"`
	FsyncInterval time.Duration `yaml:"fsyncinterval" def:"5s" doc:"how often to fsync when storage.fsyncmode is interval"`
	DiskFreeMin   float64       `yaml:"diskfreemin" def:"0.10" doc:"pause writes when the fraction of free disk space drops below this. 0 disables the guard"`
}

type ArchiveConfig struct {
	Backend        string        `yaml:"backend" def:"none" doc:"cold storage tier: none, fs, or s3. s3 covers AWS plus every S3-compatible store"`
	Prefix         string        `yaml:"prefix" def:"kafka-wire/" doc:"key prefix for archived segments. Lets several brokers share one bucket"`
	Age            time.Duration `yaml:"age" def:"1h" doc:"a sealed segment becomes eligible for upload once it is this old"`
	LocalRetention time.Duration `yaml:"localretention" def:"24h" doc:"delete the local copy of an archived segment after this long. Its indexes are kept"`
	Concurrency    int           `yaml:"concurrency" def:"2" doc:"how many segment uploads may run at once. Multiply by archive.s3.partsize to budget memory"`
	CacheBytes     int64         `yaml:"cachebytes" def:"2GiB" doc:"size of the on-disk LRU cache holding segments restored from cold storage"`

	FS FSArchiveConfig `yaml:"fs"`
	S3 S3ArchiveConfig `yaml:"s3"`
}

type FSArchiveConfig struct {
	Path string `yaml:"path" def:"" doc:"directory to archive segments into when archive.backend is fs. Point this at an NFS or SMB mount for off-box durability"`
}

type S3ArchiveConfig struct {
	Bucket       string `yaml:"bucket" def:"" doc:"bucket name. Required when archive.backend is s3"`
	Endpoint     string `yaml:"endpoint" def:"" doc:"S3 API endpoint. Empty means AWS. Examples: http://minio:9000, https://ACCOUNT.r2.cloudflarestorage.com, https://storage.googleapis.com"`
	Region       string `yaml:"region" def:"us-east-1" doc:"region string. Cloudflare R2 and Tigris want auto. Many self-hosted stores ignore it but still require a value"`
	Addressing   string `yaml:"addressing" def:"auto" doc:"bucket addressing: auto, path, or virtual. MinIO, Ceph and SeaweedFS generally need path"`
	AccessKey    string `yaml:"accesskey" def:"" doc:"access key. Empty falls through to AWS_ACCESS_KEY_ID, then the shared credentials file, then the instance credential endpoint"`
	SecretKey    string `yaml:"secretkey" def:"" doc:"secret key. Prefer KAFKA_WIRE_ARCHIVE_S3_SECRETKEY_FILE over putting this in a config file"`
	SessionToken string `yaml:"sessiontoken" def:"" doc:"session token for temporary credentials"`
	Insecure     bool   `yaml:"insecure" def:"false" doc:"talk plain HTTP instead of HTTPS. Only for an in-cluster store on a trusted network"`
	CAFile       string `yaml:"cafile" def:"" doc:"PEM bundle for a store using a private certificate authority"`
	SkipVerify   bool   `yaml:"skipverify" def:"false" doc:"do not verify the store TLS certificate. Debugging only"`
	PartSize     int64  `yaml:"partsize" def:"8MiB" doc:"multipart part size. Minimum 5MiB on most stores. Set 64MiB for Storj. segmentbytes divided by this must stay under 10000"`
	StorageClass string `yaml:"storageclass" def:"" doc:"value for x-amz-storage-class. Leave empty to omit the header. Never use an archival class: consumers fetch from this bucket"`
}

type LimitsConfig struct {
	MaxRequestBytes int32 `yaml:"maxrequestbytes" def:"4MiB" doc:"largest single protocol request accepted. Must exceed the largest batch any producer sends"`
	MaxConnections  int   `yaml:"maxconnections" def:"1024" doc:"cap on concurrent client connections. 0 means unlimited"`
	MemoryBytes     int64 `yaml:"memorybytes" def:"0" doc:"soft heap ceiling handed to the Go runtime, with 20 percent reserved as headroom. 0 disables it"`
}

type AuthConfig struct {
	SASLEnabled bool   `yaml:"saslenabled" def:"false" doc:"require SASL authentication on the Kafka listener"`
	UsersFile   string `yaml:"usersfile" def:"" doc:"path to the JSON file holding SCRAM credentials. Required when auth.saslenabled is true"`
	ACLEnabled  bool   `yaml:"aclenabled" def:"false" doc:"enforce per-principal access control lists"`
	AllowAnon   bool   `yaml:"allowanon" def:"false" doc:"permit a non-loopback listener with authentication disabled. The broker refuses to start without this, on purpose"`
}

type TLSConfig struct {
	CertFile string `yaml:"certfile" def:"" doc:"PEM certificate for the Kafka listener. Set both certfile and keyfile to enable TLS"`
	KeyFile  string `yaml:"keyfile" def:"" doc:"PEM private key for the Kafka listener"`
	ClientCA string `yaml:"clientca" def:"" doc:"PEM bundle of client certificate authorities. Setting it requires and verifies client certificates (mutual TLS)"`
	MinVersion string `yaml:"minversion" def:"1.2" doc:"minimum TLS version: 1.2 or 1.3"`
}

type AdminConfig struct {
	Token   string `yaml:"token" def:"" doc:"bearer token required by the admin API. Empty leaves the admin API open, which is refused on a non-loopback bind"`
	Enabled bool   `yaml:"enabled" def:"true" doc:"serve the HTTP admin API and Prometheus metrics"`
}

type ShutdownConfig struct {
	Grace time.Duration `yaml:"grace" def:"25s" doc:"how long to finish in-flight work after a termination signal. Keep it below your orchestrator's kill timeout: Kubernetes terminationGracePeriodSeconds, Docker stop_grace_period, Nomad kill_timeout, systemd TimeoutStopSec"`
}

type ClusterConfig struct {
	ID       string `yaml:"id" def:"kafka-wire" doc:"cluster identifier reported to clients in Metadata"`
	BrokerID int32  `yaml:"brokerid" def:"1" doc:"numeric broker id reported to clients"`
}

type LogConfig struct {
	Level  string `yaml:"level" def:"info" doc:"debug, info, warn, or error"`
	Format string `yaml:"format" def:"text" doc:"text for a human at a terminal, json for a log pipeline"`
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

// Default returns the compiled-in configuration, built by walking the struct
// and parsing each `def` tag. Doing it this way rather than with a literal
// means the tag is the single source of truth: the docs, the generated YAML
// and the runtime default can never disagree.
func Default() Config {
	var c Config
	if err := applyDefaults(reflect.ValueOf(&c).Elem(), reflect.TypeOf(c)); err != nil {
		// A bad `def` tag is a programming error, caught by TestDefaultsParse.
		panic("config: bad default tag: " + err.Error())
	}
	return c
}

func applyDefaults(v reflect.Value, t reflect.Type) error {
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		fv := v.Field(i)
		if ft.Type.Kind() == reflect.Struct && ft.Type != reflect.TypeOf(time.Duration(0)) {
			if err := applyDefaults(fv, ft.Type); err != nil {
				return err
			}
			continue
		}
		def, ok := ft.Tag.Lookup("def")
		if !ok || def == "" {
			continue
		}
		if err := setField(fv, def); err != nil {
			return fmt.Errorf("%s: %w", ft.Name, err)
		}
	}
	return nil
}

// setField parses one textual value into one struct field. This is the only
// place that knows how to turn a string into a setting, so the file loader,
// the env loader and the flag loader all behave identically.
func setField(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Bool:
		b, err := parseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("want a number, got %q", raw)
		}
		fv.SetFloat(f)
	case reflect.Int, reflect.Int32, reflect.Int64:
		if fv.Type() == reflect.TypeOf(time.Duration(0)) {
			d, err := ParseDuration(raw)
			if err != nil {
				return err
			}
			fv.SetInt(int64(d))
			return nil
		}
		n, err := ParseSize(raw)
		if err != nil {
			return err
		}
		if fv.OverflowInt(n) {
			return fmt.Errorf("value %d does not fit in %s", n, fv.Type())
		}
		fv.SetInt(n)
	default:
		return fmt.Errorf("unsupported kind %s", fv.Kind())
	}
	return nil
}

func parseBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("want true or false, got %q", raw)
}

// ParseSize accepts a plain integer or a byte-size suffix. Both the IEC form
// (KiB, MiB, GiB, TiB) and the common shorthand (K, M, G, T, KB, MB, GB, TB)
// are read as powers of 1024, because that is what everyone means when they
// write "512MB" for a segment size.
func ParseSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	up := strings.ToUpper(s)
	mult := map[string]int64{
		"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "TIB": 1 << 40,
		"KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40,
		"K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40,
		"B": 1,
	}
	// Longest suffix first so "KIB" is not matched as "B".
	for _, suf := range []string{"KIB", "MIB", "GIB", "TIB", "KB", "MB", "GB", "TB", "K", "M", "G", "T", "B"} {
		if strings.HasSuffix(up, suf) {
			num := strings.TrimSpace(strings.TrimSuffix(up, suf))
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("want a size such as 512MiB, got %q", raw)
			}
			return int64(f * float64(mult[suf])), nil
		}
	}
	return 0, fmt.Errorf("want a size such as 1GiB or a plain byte count, got %q", raw)
}

// ParseDuration accepts Go duration syntax ("30s", "1h30m") and also a bare
// integer, which is read as milliseconds. The bare-integer case exists
// because Kafka's own settings are named in milliseconds and users copy
// values across from a Kafka config without thinking about units.
func ParseDuration(raw string) (time.Duration, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Duration(n) * time.Millisecond, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("want a duration such as 30s or 168h, got %q", raw)
	}
	return d, nil
}

// ---------------------------------------------------------------------------
// Key paths and environment variable names
// ---------------------------------------------------------------------------

// Field describes one leaf setting, flattened out of the nested struct.
type Field struct {
	Path    string // dotted key path, e.g. "archive.s3.bucket"
	Env     string // derived env var,  e.g. "KAFKA_WIRE_ARCHIVE_S3_BUCKET"
	Default string // the `def` tag, verbatim
	Doc     string // the `doc` tag
	Secret  bool   // true for fields that must never be printed
	Kind    string
}

var secretPaths = map[string]bool{
	"admin.token":            true,
	"archive.s3.secretkey":   true,
	"archive.s3.accesskey":   true,
	"archive.s3.sessiontoken": true,
}

// EnvName derives the environment variable for a key path. The rule is
// deliberately trivial and reversible: uppercase the path and replace each
// dot with an underscore. Key path segments never contain an underscore or a
// dash, which is what keeps the mapping unambiguous in both directions and
// avoids the escaping ladders other projects ended up with.
func EnvName(path string) string {
	return EnvPrefix + "_" + strings.ToUpper(strings.ReplaceAll(path, ".", "_"))
}

// Fields returns every leaf setting in declaration order.
func Fields() []Field {
	var out []Field
	walk(reflect.TypeOf(Config{}), "", &out)
	return out
}

func walk(t reflect.Type, prefix string, out *[]Field) {
	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		name := ft.Tag.Get("yaml")
		if name == "" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		if ft.Type.Kind() == reflect.Struct && ft.Type != reflect.TypeOf(time.Duration(0)) {
			walk(ft.Type, path, out)
			continue
		}
		*out = append(*out, Field{
			Path:    path,
			Env:     EnvName(path),
			Default: ft.Tag.Get("def"),
			Doc:     ft.Tag.Get("doc"),
			Secret:  secretPaths[path],
			Kind:    kindName(ft.Type),
		})
	}
}

func kindName(t reflect.Type) string {
	if t == reflect.TypeOf(time.Duration(0)) {
		return "duration"
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Float64:
		return "float"
	case reflect.Int, reflect.Int32, reflect.Int64:
		return "size"
	}
	return t.Kind().String()
}

// fieldByPath resolves a dotted key path to the addressable field.
func fieldByPath(c *Config, path string) (reflect.Value, bool) {
	v := reflect.ValueOf(c).Elem()
	t := v.Type()
	for _, seg := range strings.Split(path, ".") {
		found := false
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Tag.Get("yaml") == seg {
				v = v.Field(i)
				t = v.Type()
				found = true
				break
			}
		}
		if !found {
			return reflect.Value{}, false
		}
	}
	return v, true
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

// Source records where a setting's final value came from, so an operator can
// answer "why is this value what it is" without guessing. `config print`
// shows it, and validation errors name it.
type Source struct {
	Path   string
	Origin string // "default", "file:<name>", "env:<VAR>", "file-env:<VAR>", "flag:--x"
}

// Loaded is a resolved configuration plus its provenance.
type Loaded struct {
	Config  Config
	Sources map[string]string
	File    string // config file actually used, empty if none
}

// SearchPaths lists where the config file is looked for, in order.
func SearchPaths() []string {
	paths := []string{"kafka-wire.yaml", "kafka-wire.yml"}
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		paths = append(paths, filepath.Join(x, "kafka-wire", "kafka-wire.yaml"))
	}
	if h, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(h, ".config", "kafka-wire", "kafka-wire.yaml"))
	}
	paths = append(paths, filepath.Join("/etc", "kafka-wire", "kafka-wire.yaml"))
	return paths
}

package broker

import (
	"errors"
	"fmt"
)

// ErrInvalidName rejects a topic or group name that cannot be used safely.
var ErrInvalidName = errors.New("invalid name")

// MaxNameLength matches Apache Kafka's limit for topic names.
const MaxNameLength = 249

// ValidateName checks a client-supplied topic or consumer group name.
//
// This is a security boundary, not a style rule. Topic names become directory
// names under the data directory and object keys in cold storage, and group
// ids become file names, so a name containing a path separator or a parent
// reference lets an unauthenticated client create and delete directories
// outside the data directory entirely. The character set below is Apache
// Kafka's own, which has the useful property that nothing in it has meaning to
// a filesystem.
func ValidateName(kind, name string) error {
	if name == "" {
		return fmt.Errorf("%w: %s name is empty", ErrInvalidName, kind)
	}
	if len(name) > MaxNameLength {
		return fmt.Errorf("%w: %s name is %d characters, the limit is %d",
			ErrInvalidName, kind, len(name), MaxNameLength)
	}
	// "." and ".." are legal by the character rule below and are exactly the
	// two names that traverse, so they are refused by name.
	if name == "." || name == ".." {
		return fmt.Errorf("%w: %s name %q is a directory reference", ErrInvalidName, kind, name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: %s name %q contains %q; allowed characters are "+
				"letters, digits, and the symbols . _ -", ErrInvalidName, kind, name, string(c))
		}
	}
	return nil
}

// MaxPartitionsPerTopic bounds a single CreateTopics request.
//
// Partition creation happens under the topic registry's lock and creates three
// files per partition, so an unbounded count from the wire is both an inode
// exhaustion vector and a broker-wide stall. Kafka has no protocol-level cap,
// so this is a local policy: high enough that no real single-node workload
// notices, low enough that a hostile request fails immediately.
const MaxPartitionsPerTopic = 1024

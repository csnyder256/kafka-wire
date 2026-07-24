package broker

import (
	"strings"
	"testing"
)

// Topic and group names arrive from the network and become filesystem paths.
// These are the inputs that turned an unauthenticated client into arbitrary
// directory creation and recursive deletion outside the data directory.
func TestValidateNameRejectsTraversal(t *testing.T) {
	bad := []string{
		"../escaped",
		"../../escaped",
		"a/../../escaped",
		`..\escaped`,
		"/absolute",
		"C:/windows/path",
		"with/slash",
		"with\backslash",
		"..",
		".",
		"",
		"nul\x00byte",
		"spaces are out",
		strings.Repeat("x", MaxNameLength+1),
	}
	for _, name := range bad {
		if err := ValidateName("topic", name); err == nil {
			t.Errorf("ValidateName accepted %q, which reaches the filesystem", name)
		}
	}
}

func TestValidateNameAcceptsRealNames(t *testing.T) {
	good := []string{
		"demo",
		"orders.events",
		"orders_events",
		"orders-events-v2",
		"a",
		"UPPER.case_123",
		strings.Repeat("x", MaxNameLength),
	}
	for _, name := range good {
		if err := ValidateName("topic", name); err != nil {
			t.Errorf("ValidateName rejected the legitimate name %q: %v", name, err)
		}
	}
}

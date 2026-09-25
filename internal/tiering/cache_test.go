package tiering

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// Restored segments are cached by topic, partition and offset. A deleted
// topic's cache used to survive it, and a topic recreated under the same
// name was served the old segments.
func TestDropTopicRemovesEveryTenantsCopy(t *testing.T) {
	c, err := NewCache(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("segment bytes")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	put := func(tenant, topic string) {
		if err := c.PutTenant(tenant, topic, 0, 0, data, digest); err != nil {
			t.Fatal(err)
		}
	}
	put("", "gone")
	put("acme", "gone")
	put("", "kept")
	put("acme", "kept")

	if err := c.DropTopic("gone"); err != nil {
		t.Fatal(err)
	}
	if c.HasTenant("", "gone", 0, 0) || c.HasTenant("acme", "gone", 0, 0) {
		t.Error("the deleted topic's cached segments must be gone for every tenant")
	}
	if !c.HasTenant("", "kept", 0, 0) || !c.HasTenant("acme", "kept", 0, 0) {
		t.Error("other topics must keep their cached segments")
	}
}

package storage

import "hash/crc32"

// Kafka uses CRC32C with the Castagnoli polynomial, NOT the IEEE
// polynomial that's the stdlib default. Get this wrong and every
// produced batch is rejected as corrupt, silent data loss.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C returns Kafka's CRC over body (post-CRC bytes).
func CRC32C(body []byte) uint32 {
	return crc32.Checksum(body, castagnoliTable)
}

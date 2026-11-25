package gofakes3

import (
	"hash/crc64"
	"testing"
)

// TestCRC64NVME checks the CRC-64/NVME table against the standard
// check value.
func TestCRC64NVME(t *testing.T) {
	if got := crc64.Checksum([]byte("123456789"), crc64NVMETable); got != 0xae8b14860a799888 {
		t.Fatalf("CRC-64/NVME check value: want 0xae8b14860a799888 got %#x", got)
	}
}

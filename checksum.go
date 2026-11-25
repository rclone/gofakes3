package gofakes3

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strings"
)

// crc64NVMETable is the table for the CRC-64/NVME checksum S3 uses.
var crc64NVMETable = crc64.MakeTable(0x9a6c9329ac4bc9b5)

// checksumAlgorithms maps the names of the checksum algorithms S3
// supports, as in the x-amz-checksum-<name> headers, to their hashes.
var checksumAlgorithms = map[string]func() hash.Hash{
	"crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"crc64nvme": func() hash.Hash { return crc64.New(crc64NVMETable) },
	"sha1":      sha1.New,
	"sha256":    sha256.New,
}

// checksumHeaderPrefix starts the names of the headers with an object's
// checksum.
const checksumHeaderPrefix = "X-Amz-Checksum-"

// requestChecksum returns the algorithm and value of the checksum of
// the body in the x-amz-checksum-<algorithm> header in h, or "" if
// there isn't one.
func requestChecksum(h http.Header) (algorithm string, expected []byte, err error) {
	var value string
	for hk, hv := range h {
		if !strings.HasPrefix(hk, checksumHeaderPrefix) || len(hv) == 0 {
			continue
		}
		name := strings.ToLower(strings.TrimPrefix(hk, checksumHeaderPrefix))
		if name == "type" || name == "mode" {
			continue
		}
		if algorithm != "" {
			return "", nil, ErrorMessage(ErrInvalidRequest, "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.")
		}
		algorithm, value = name, hv[0]
	}
	if algorithm == "" {
		return "", nil, nil
	}
	newHash, ok := checksumAlgorithms[algorithm]
	if !ok {
		return "", nil, ErrorMessagef(ErrInvalidRequest, "The x-amz-checksum-%s header is not supported.", algorithm)
	}
	if sdk := h.Get("X-Amz-Sdk-Checksum-Algorithm"); sdk != "" && !strings.EqualFold(sdk, algorithm) {
		return "", nil, ErrorMessage(ErrInvalidRequest, "Value for x-amz-sdk-checksum-algorithm header is invalid.")
	}
	expected, err = base64.StdEncoding.DecodeString(value)
	if err != nil || len(expected) != newHash().Size() {
		return "", nil, ErrorMessagef(ErrInvalidRequest, "Value for x-amz-checksum-%s header is invalid.", algorithm)
	}
	return algorithm, expected, nil
}

// checksumReader passes through the data read from inner, failing with
// BadDigest at EOF if its checksum isn't the expected one.
type checksumReader struct {
	inner     io.Reader
	algorithm string
	expected  []byte
	hash      hash.Hash
}

func newChecksumReader(inner io.Reader, algorithm string, expected []byte) *checksumReader {
	return &checksumReader{
		inner:     inner,
		algorithm: algorithm,
		expected:  expected,
		hash:      checksumAlgorithms[algorithm](),
	}
}

func (c *checksumReader) Read(p []byte) (n int, err error) {
	n, err = c.inner.Read(p)
	_, _ = c.hash.Write(p[:n])
	if err == io.EOF && !bytes.Equal(c.hash.Sum(nil), c.expected) {
		return n, ErrorMessagef(ErrBadDigest, "The %s you specified did not match the calculated checksum.", strings.ToUpper(c.algorithm))
	}
	return n, err
}

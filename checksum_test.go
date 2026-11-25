package gofakes3_test

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"hash/crc32"
	"hash/crc64"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestChecksums checks the body of a PutObject is checked against its
// x-amz-checksum-* header and the checksum is returned when asked for.
func TestChecksums(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	body := "hello"

	be32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	be64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	sha1Sum := sha1.Sum([]byte(body))
	sha256Sum := sha256.Sum256([]byte(body))
	checksums := map[string][]byte{
		"crc32":     be32(crc32.ChecksumIEEE([]byte(body))),
		"crc32c":    be32(crc32.Checksum([]byte(body), crc32.MakeTable(crc32.Castagnoli))),
		"crc64nvme": be64(crc64.Checksum([]byte(body), crc64.MakeTable(0x9a6c9329ac4bc9b5))),
		"sha1":      sha1Sum[:],
		"sha256":    sha256Sum[:],
	}

	do := func(method, key string, headers map[string]string) (*http.Response, string) {
		t.Helper()
		var rdr io.Reader
		if method == "PUT" {
			rdr = strings.NewReader(body)
		}
		rq, err := http.NewRequest(method, ts.url("/"+defaultBucket+"/"+key), rdr)
		ts.OK(err)
		for k, v := range headers {
			rq.Header.Set(k, v)
		}
		rs, err := httpClient().Do(rq)
		ts.OK(err)
		defer func() { _ = rs.Body.Close() }()
		b, err := io.ReadAll(rs.Body)
		ts.OK(err)
		return rs, string(b)
	}
	errorCode := func(b string) string {
		var e struct{ Code string }
		_ = xml.Unmarshal([]byte(b), &e)
		return e.Code
	}

	for algorithm, sum := range checksums {
		t.Run(algorithm, func(t *testing.T) {
			header := "X-Amz-Checksum-" + algorithm
			value := base64.StdEncoding.EncodeToString(sum)

			rs, b := do("PUT", algorithm, map[string]string{header: value})
			if rs.StatusCode != http.StatusOK {
				t.Fatalf("PUT: status %d: %s", rs.StatusCode, b)
			}
			if got := rs.Header.Get(header); got != value {
				t.Errorf("PUT response checksum: want %q got %q", value, got)
			}

			rs, _ = do("GET", algorithm, nil)
			if got := rs.Header.Get(header); got != "" {
				t.Errorf("checksum returned without checksum mode: %q", got)
			}
			rs, _ = do("GET", algorithm, map[string]string{"X-Amz-Checksum-Mode": "ENABLED"})
			if got := rs.Header.Get(header); got != value {
				t.Errorf("GET checksum: want %q got %q", value, got)
			}
			rs, _ = do("HEAD", algorithm, map[string]string{"X-Amz-Checksum-Mode": "ENABLED"})
			if got := rs.Header.Get(header); got != value {
				t.Errorf("HEAD checksum: want %q got %q", value, got)
			}
			rs, _ = do("GET", algorithm, map[string]string{"X-Amz-Checksum-Mode": "ENABLED", "Range": "bytes=1-2"})
			if got := rs.Header.Get(header); got != "" {
				t.Errorf("checksum returned for range: %q", got)
			}
			for _, cond := range []string{"If-None-Match", "If-Match"} {
				rs, _ = do("GET", algorithm, map[string]string{"X-Amz-Checksum-Mode": "ENABLED", cond: `"5d41402abc4b2a76b9719d911017c592"`})
				if cond == "If-Match" {
					rs, _ = do("GET", algorithm, map[string]string{"X-Amz-Checksum-Mode": "ENABLED", cond: `"other"`})
				}
				if rs.StatusCode < 300 {
					t.Errorf("%s: expected failure, got %d", cond, rs.StatusCode)
				}
				if got := rs.Header.Get(header); got != "" {
					t.Errorf("checksum returned with failed %s: %q", cond, got)
				}
			}

			// A wrong checksum of the right length is refused
			wrong := make([]byte, len(sum))
			rs, b = do("PUT", algorithm+"-wrong", map[string]string{header: base64.StdEncoding.EncodeToString(wrong)})
			if rs.StatusCode != http.StatusBadRequest || errorCode(b) != "BadDigest" {
				t.Errorf("wrong checksum: want 400 BadDigest got %d: %s", rs.StatusCode, b)
			}
			if ts.backendObjectExists(defaultBucket, algorithm+"-wrong") {
				t.Errorf("object with wrong checksum stored")
			}
		})
	}

	for _, test := range []struct {
		name    string
		headers map[string]string
	}{
		{"malformed", map[string]string{"X-Amz-Checksum-Sha256": "bad"}},
		{"unknown algorithm", map[string]string{"X-Amz-Checksum-Md4": "AAAA"}},
		{"two checksums", map[string]string{
			"X-Amz-Checksum-Crc32":  base64.StdEncoding.EncodeToString(checksums["crc32"]),
			"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(checksums["sha256"]),
		}},
		{"algorithm mismatch", map[string]string{
			"X-Amz-Checksum-Crc32":         base64.StdEncoding.EncodeToString(checksums["crc32"]),
			"X-Amz-Sdk-Checksum-Algorithm": "SHA256",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rs, b := do("PUT", "invalid", test.headers)
			if rs.StatusCode != http.StatusBadRequest || errorCode(b) != "InvalidRequest" {
				t.Errorf("want 400 InvalidRequest got %d: %s", rs.StatusCode, b)
			}
		})
	}
}

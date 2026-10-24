package gofakes3_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestBodyDigests checks the bodies of the requests which aren't object
// data are checked against their Content-MD5 and x-amz-checksum-*
// headers.
func TestBodyDigests(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	do := func(method, path, body string, headers map[string]string) (int, string) {
		t.Helper()
		rq, err := http.NewRequest(method, ts.url(path), strings.NewReader(body))
		ts.OK(err)
		for k, v := range headers {
			rq.Header.Set(k, v)
		}
		rs, err := httpClient().Do(rq)
		ts.OK(err)
		defer func() { _ = rs.Body.Close() }()
		b, err := io.ReadAll(rs.Body)
		ts.OK(err)
		var e struct{ Code string }
		_ = xml.Unmarshal(b, &e)
		return rs.StatusCode, e.Code
	}
	md5Of := func(s string) string {
		sum := md5.Sum([]byte(s))
		return base64.StdEncoding.EncodeToString(sum[:])
	}
	crc32Of := func(s string) string {
		return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE([]byte(s))))
	}

	bucketPath := "/" + defaultBucket
	for _, test := range []struct {
		name         string
		method, path string
		body         string
	}{
		{"DeleteObjects", "POST", bucketPath + "?delete", `<Delete><Object><Key>missing</Key></Object></Delete>`},
		{"PutObjectTagging", "PUT", bucketPath + "/object?tagging", `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`},
		{"PutBucketVersioning", "PUT", bucketPath + "?versioning", `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`},
		{"PutBucketAcl", "PUT", bucketPath + "?acl", `<AccessControlPolicy><Owner><ID>fe7272ea58be830e56fe1663b10fafef</ID></Owner><AccessControlList><Grant><Grantee><ID>fe7272ea58be830e56fe1663b10fafef</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`},
		{"CreateBucket", "PUT", "/digest-bucket", `<CreateBucketConfiguration><LocationConstraint>us-east-1</LocationConstraint></CreateBucketConfiguration>`},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, c := range []struct {
				name    string
				headers map[string]string
				code    string
			}{
				{"bad md5", map[string]string{"Content-MD5": md5Of("other")}, "BadDigest"},
				{"invalid md5", map[string]string{"Content-MD5": "not base64"}, "InvalidDigest"},
				{"bad checksum", map[string]string{"X-Amz-Checksum-Crc32": crc32Of("other")}, "BadDigest"},
			} {
				status, code := do(test.method, test.path, test.body, c.headers)
				if status != http.StatusBadRequest || code != c.code {
					t.Errorf("%s: want 400 %s, got %d %q", c.name, c.code, status, code)
				}
			}
			status, code := do(test.method, test.path, test.body, map[string]string{
				"Content-MD5":          md5Of(test.body),
				"X-Amz-Checksum-Crc32": crc32Of(test.body),
			})
			if status >= 300 {
				t.Errorf("good digests: got %d %q", status, code)
			}
		})
	}

	t.Run("CompleteMultipartUpload", func(t *testing.T) {
		uploadID := ts.createMultipartUpload(defaultBucket, "mp", nil)
		part := ts.uploadPart(defaultBucket, "mp", uploadID, 1, []byte("hello"))
		path := bucketPath + "/mp?uploadId=" + uploadID
		body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + *part.ETag + `</ETag></Part></CompleteMultipartUpload>`
		status, code := do("POST", path, body, map[string]string{"Content-MD5": md5Of("other")})
		if status != http.StatusBadRequest || code != "BadDigest" {
			t.Errorf("bad md5: want 400 BadDigest, got %d %q", status, code)
		}
		// The checksum header has the checksum of the object, not of
		// the body
		status, code = do("POST", path, body, map[string]string{
			"Content-MD5":          md5Of(body),
			"X-Amz-Checksum-Crc32": crc32Of("hello"),
		})
		if status != http.StatusOK {
			t.Errorf("good md5: got %d %q", status, code)
		}
	})
}

// TestCopySourcePlus checks a "+" in x-amz-copy-source is a space, as
// on AWS S3, so a "+" in the key must be sent as "%2B".
func TestCopySourcePlus(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "a+b", nil, "plus")
	ts.backendPutString(defaultBucket, "a b", nil, "space")

	for _, test := range []struct{ source, want string }{
		{"a+b", "space"},
		{"a%2Bb", "plus"},
		{"a%20b", "space"},
	} {
		rq, err := http.NewRequest("PUT", ts.url("/"+defaultBucket+"/dst"), nil)
		ts.OK(err)
		rq.Header.Set("X-Amz-Copy-Source", defaultBucket+"/"+test.source)
		rs, err := httpClient().Do(rq)
		ts.OK(err)
		_ = rs.Body.Close()
		if rs.StatusCode != http.StatusOK {
			t.Fatalf("copy %q: status %d", test.source, rs.StatusCode)
		}
		if got := ts.backendGetString(defaultBucket, "dst", nil); got != test.want {
			t.Errorf("copy %q: want %q got %q", test.source, test.want, got)
		}
	}
}

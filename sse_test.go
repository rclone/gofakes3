package gofakes3_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestServerSideEncryption checks server side encryption and object
// lock headers on writes are checked and the unsupported ones refused.
func TestServerSideEncryption(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64 := base64.StdEncoding.EncodeToString(key)
	sum := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(sum[:])
	sseC := map[string]string{
		"X-Amz-Server-Side-Encryption-Customer-Algorithm": "AES256",
		"X-Amz-Server-Side-Encryption-Customer-Key":       keyB64,
		"X-Amz-Server-Side-Encryption-Customer-Key-Md5":   keyMD5,
	}
	with := func(base map[string]string, k, v string) map[string]string {
		out := map[string]string{}
		for bk, bv := range base {
			out[bk] = bv
		}
		if v == "" {
			delete(out, k)
		} else {
			out[k] = v
		}
		return out
	}

	for _, test := range []struct {
		name    string
		method  string
		path    string
		headers map[string]string
		status  int
		code    string
	}{
		{"none", "PUT", "/obj", nil, http.StatusOK, ""},
		{"SSE-S3", "PUT", "/obj", map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}, http.StatusOK, ""},
		{"SSE-KMS", "PUT", "/obj", map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms"}, http.StatusNotImplemented, "NotImplemented"},
		{"SSE-KMS key", "PUT", "/obj", map[string]string{"X-Amz-Server-Side-Encryption": "aws:kms", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "k"}, http.StatusNotImplemented, "NotImplemented"},
		{"KMS key without SSE-KMS", "PUT", "/obj", map[string]string{"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "k"}, http.StatusBadRequest, "InvalidArgument"},
		{"bad SSE", "PUT", "/obj", map[string]string{"X-Amz-Server-Side-Encryption": "rot13"}, http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C", "PUT", "/obj", sseC, http.StatusNotImplemented, "NotImplemented"},
		{"SSE-C on multipart upload", "POST", "/obj?uploads", sseC, http.StatusNotImplemented, "NotImplemented"},
		{"SSE-C and SSE-S3", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption", "AES256"), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C bad algorithm", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Algorithm", "DES"), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C no algorithm", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Algorithm", ""), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C no key", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Key", ""), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C short key", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Key", "c2hvcnQ="), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C no MD5", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Key-Md5", ""), http.StatusBadRequest, "InvalidArgument"},
		{"SSE-C bad MD5", "PUT", "/obj", with(sseC, "X-Amz-Server-Side-Encryption-Customer-Key-Md5", "AAAAAAAAAAAAAAAAAAAAAA=="), http.StatusBadRequest, "InvalidArgument"},
		{"object lock", "PUT", "/obj", map[string]string{"X-Amz-Object-Lock-Mode": "GOVERNANCE", "X-Amz-Object-Lock-Retain-Until-Date": "2094-01-01T00:00:00Z"}, http.StatusBadRequest, "InvalidRequest"},
		{"legal hold", "PUT", "/obj", map[string]string{"X-Amz-Object-Lock-Legal-Hold": "ON"}, http.StatusBadRequest, "InvalidRequest"},
		{"bucket with object lock", "PUT", "/newbucket", map[string]string{"X-Amz-Bucket-Object-Lock-Enabled": "true"}, http.StatusNotImplemented, "NotImplemented"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key := strings.ReplaceAll(test.name, " ", "-")
			path := "/" + defaultBucket + strings.Replace(test.path, "/obj", "/"+key, 1)
			if strings.HasPrefix(test.path, "/newbucket") {
				path = test.path
			}
			rq, err := http.NewRequest(test.method, ts.url(path), strings.NewReader("hello"))
			ts.OK(err)
			for k, v := range test.headers {
				rq.Header.Set(k, v)
			}
			rs, err := httpClient().Do(rq)
			ts.OK(err)
			defer func() { _ = rs.Body.Close() }()
			body, err := io.ReadAll(rs.Body)
			ts.OK(err)
			if rs.StatusCode != test.status {
				t.Fatalf("status: want %d got %d: %s", test.status, rs.StatusCode, body)
			}
			if test.code != "" {
				var e struct{ Code string }
				ts.OK(xml.Unmarshal(body, &e))
				if e.Code != test.code {
					t.Fatalf("code: want %s got %s", test.code, e.Code)
				}
			}
			if rs.Header.Get("X-Amz-Server-Side-Encryption") != "" {
				t.Errorf("encryption claimed in response: %v", rs.Header)
			}
			if test.method == "PUT" && test.path == "/obj" && test.status != http.StatusOK && ts.backendObjectExists(defaultBucket, key) {
				t.Errorf("object written despite refusal")
			}
		})
	}
}

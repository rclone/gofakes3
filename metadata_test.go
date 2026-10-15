package gofakes3_test

import (
	"net/http"
	"strings"
	"testing"
)

// TestContentEncodingAWSChunked checks aws-chunked is removed from the
// Content-Encoding stored, leaving the rest unchanged.
func TestContentEncodingAWSChunked(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	for _, test := range []struct{ in, want string }{
		{"gzip", "gzip"},
		{"deflate, gzip", "deflate, gzip"},
		{"deflate,gzip", "deflate,gzip"},
		{"gzip, aws-chunked", "gzip"},
		{"aws-chunked, gzip", "gzip"},
		{"aws-chunked,deflate,gzip", "deflate, gzip"},
		{"aws-chunked", ""},
		{"aws-chunked, aws-chunked", ""},
	} {
		key := "/" + defaultBucket + "/" + test.in
		rq, err := http.NewRequest("PUT", ts.url(key), strings.NewReader("hello"))
		ts.OK(err)
		rq.Header.Set("Content-Encoding", test.in)
		rs, err := httpClient().Do(rq)
		ts.OK(err)
		_ = rs.Body.Close()
		head, err := httpClient().Head(ts.url(key))
		ts.OK(err)
		if got := head.Header.Get("Content-Encoding"); got != test.want {
			t.Errorf("%q: want %q got %q", test.in, test.want, got)
		}
	}
}

package gofakes3_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"
)

// getXML GETs path from ts and decodes the XML response into out,
// returning the raw body.
func getXML(ts *testServer, path string, out interface{}) string {
	ts.Helper()
	rs, err := httpClient().Get(ts.url(path))
	ts.OK(err)
	defer func() { _ = rs.Body.Close() }()
	body, err := io.ReadAll(rs.Body)
	ts.OK(err)
	if rs.StatusCode != http.StatusOK {
		ts.Fatalf("GET %s: status %d: %s", path, rs.StatusCode, body)
	}
	ts.OK(xml.Unmarshal(body, out))
	return string(body)
}

type listResult struct {
	Prefix         string
	Delimiter      string
	IsTruncated    bool
	CommonPrefixes []struct{ Prefix string }
	Contents       []struct {
		Key   string
		Owner *struct{ ID string }
	}
}

// TestListEncodingURL checks listings with encoding-type=url encode
// as S3 does.
func TestListEncodingURL(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	for _, key := range []string{"foo+1/x", "quux ab/x", "a b"} {
		ts.backendPutString(defaultBucket, key, nil, "x")
	}

	var v1, v2 listResult
	getXML(ts, "/"+defaultBucket+"?delimiter=/&encoding-type=url&prefix=", &v1)
	getXML(ts, "/"+defaultBucket+"?list-type=2&delimiter=/&encoding-type=url&prefix=", &v2)
	for _, result := range []listResult{v1, v2} {
		var prefixes []string
		for _, p := range result.CommonPrefixes {
			prefixes = append(prefixes, p.Prefix)
		}
		if got, want := strings.Join(prefixes, ","), "foo%2B1/,quux%20ab/"; got != want {
			t.Errorf("CommonPrefixes: want %q got %q", want, got)
		}
		if len(result.Contents) != 1 || result.Contents[0].Key != "a%20b" {
			t.Errorf("Contents: want a%%20b got %+v", result.Contents)
		}
	}

	// The prefix is encoded in V2 listings but not V1
	getXML(ts, "/"+defaultBucket+"?encoding-type=url&prefix=a%20", &v1)
	getXML(ts, "/"+defaultBucket+"?list-type=2&encoding-type=url&prefix=a%20", &v2)
	if v1.Prefix != "a " {
		t.Errorf("V1 Prefix: want %q got %q", "a ", v1.Prefix)
	}
	if v2.Prefix != "a%20" {
		t.Errorf("V2 Prefix: want %q got %q", "a%20", v2.Prefix)
	}
}

// TestListMaxKeysZero checks asking for no keys gets none.
func TestListMaxKeysZero(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "object", nil, "x")

	for _, query := range []string{"?max-keys=0", "?list-type=2&max-keys=0"} {
		var result listResult
		getXML(ts, "/"+defaultBucket+query, &result)
		if len(result.Contents) != 0 || result.IsTruncated {
			t.Errorf("%s: want no keys and not truncated, got %+v", query, result)
		}
	}
}

// TestListOwner checks the owner is in V1 listings, and V2 listings
// only when asked for.
func TestListOwner(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "object", nil, "x")

	for _, test := range []struct {
		query string
		owner bool
	}{
		{"", true},
		{"?list-type=2", false},
		{"?list-type=2&fetch-owner=false", false},
		{"?list-type=2&fetch-owner=true", true},
	} {
		var result listResult
		getXML(ts, "/"+defaultBucket+test.query, &result)
		if len(result.Contents) != 1 {
			t.Fatalf("%q: want 1 key got %+v", test.query, result)
		}
		if got := result.Contents[0].Owner != nil && result.Contents[0].Owner.ID != ""; got != test.owner {
			t.Errorf("%q: want owner %v got %v", test.query, test.owner, got)
		}
	}
}

// TestListContinuationTokenEmpty checks an empty continuation token is
// returned in the response.
func TestListContinuationTokenEmpty(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "object", nil, "x")

	var result listResult
	body := getXML(ts, "/"+defaultBucket+"?list-type=2&continuation-token=", &result)
	if !strings.Contains(body, "<ContinuationToken></ContinuationToken>") {
		t.Errorf("empty ContinuationToken missing:\n%s", body)
	}
	body = getXML(ts, "/"+defaultBucket+"?list-type=2", &result)
	if strings.Contains(body, "ContinuationToken") {
		t.Errorf("unexpected ContinuationToken:\n%s", body)
	}
}

// TestListBucketsPaginated checks ListBuckets with max-buckets,
// continuation-token and prefix.
func TestListBucketsPaginated(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	for _, bucket := range []string{"bucket-c", "bucket-a", "bucket-b", "other"} {
		ts.backendCreateBucket(bucket)
	}

	type bucketsResult struct {
		Buckets           []struct{ Name string } `xml:"Buckets>Bucket"`
		ContinuationToken string
		Prefix            string
	}
	names := func(r bucketsResult) string {
		var out []string
		for _, b := range r.Buckets {
			out = append(out, b.Name)
		}
		return strings.Join(out, ",")
	}

	var r bucketsResult
	getXML(ts, "/", &r)
	if got := names(r); got != "bucket-a,bucket-b,bucket-c,other" || r.ContinuationToken != "" {
		t.Errorf("all: got %q token %q", got, r.ContinuationToken)
	}

	var seen []string
	token := ""
	for i := 0; i < 10; i++ {
		r = bucketsResult{}
		getXML(ts, "/?max-buckets=1&prefix=bucket-&continuation-token="+token, &r)
		seen = append(seen, names(r))
		if r.Prefix != "bucket-" {
			t.Errorf("Prefix: want bucket- got %q", r.Prefix)
		}
		token = r.ContinuationToken
		if token == "" {
			break
		}
	}
	if got := strings.Join(seen, ","); got != "bucket-a,bucket-b,bucket-c" {
		t.Errorf("pages: got %q", got)
	}
}

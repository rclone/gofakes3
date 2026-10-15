package gofakes3_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rclone/gofakes3"
)

// TestConditionalGet checks the conditional headers of GET and HEAD.
func TestConditionalGet(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ctx := context.Background()
	_, err := ts.s3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	ts.OK(err)

	const (
		etag   = `"5d41402abc4b2a76b9719d911017c592"` // md5("hello")
		other  = `"00000000000000000000000000000000"`
		before = "Sun, 01 Jan 2017 12:00:00 GMT"
		after  = "Tue, 01 Jan 2019 12:00:00 GMT"
	)
	modTime := defaultDate.Format(http.TimeFormat)

	for _, test := range []struct {
		name    string
		headers map[string]string
		status  int
	}{
		{"none", nil, http.StatusOK},
		{"If-Match", map[string]string{"If-Match": etag}, http.StatusOK},
		{"If-Match star", map[string]string{"If-Match": "*"}, http.StatusOK},
		{"If-Match list", map[string]string{"If-Match": other + ", " + etag}, http.StatusOK},
		{"If-Match fail", map[string]string{"If-Match": other}, http.StatusPreconditionFailed},
		{"If-None-Match", map[string]string{"If-None-Match": other}, http.StatusOK},
		{"If-None-Match fail", map[string]string{"If-None-Match": etag}, http.StatusNotModified},
		{"If-None-Match star", map[string]string{"If-None-Match": "*"}, http.StatusNotModified},
		{"If-None-Match unquoted", map[string]string{"If-None-Match": etag[1 : len(etag)-1]}, http.StatusNotModified},
		{"If-Modified-Since", map[string]string{"If-Modified-Since": before}, http.StatusOK},
		{"If-Modified-Since fail", map[string]string{"If-Modified-Since": after}, http.StatusNotModified},
		{"If-Modified-Since same", map[string]string{"If-Modified-Since": modTime}, http.StatusNotModified},
		{"If-Modified-Since invalid", map[string]string{"If-Modified-Since": "rubbish"}, http.StatusOK},
		{"If-Unmodified-Since", map[string]string{"If-Unmodified-Since": after}, http.StatusOK},
		{"If-Unmodified-Since same", map[string]string{"If-Unmodified-Since": modTime}, http.StatusOK},
		{"If-Unmodified-Since fail", map[string]string{"If-Unmodified-Since": before}, http.StatusPreconditionFailed},
		{"If-Match beats If-Unmodified-Since", map[string]string{"If-Match": etag, "If-Unmodified-Since": before}, http.StatusOK},
		{"If-None-Match beats If-Modified-Since", map[string]string{"If-None-Match": other, "If-Modified-Since": after}, http.StatusOK},
	} {
		for _, method := range []string{"GET", "HEAD"} {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				rq, err := http.NewRequest(method, ts.url("/"+defaultBucket+"/object"), nil)
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
					t.Errorf("status: want %d got %d: %s", test.status, rs.StatusCode, body)
				}
				if test.status == http.StatusNotModified && len(body) != 0 {
					t.Errorf("unexpected body with 304: %q", body)
				}
				if test.status == http.StatusOK && method == "GET" && string(body) != "hello" {
					t.Errorf("wrong body %q", body)
				}
			})
		}
	}
}

// TestConditionalCopy checks the conditions on the source of a copy.
func TestConditionalCopy(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ctx := context.Background()
	svc := ts.s3Client()
	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("src"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	ts.OK(err)

	const (
		etag  = `"5d41402abc4b2a76b9719d911017c592"` // md5("hello")
		other = `"00000000000000000000000000000000"`
	)
	before := time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC)
	after := time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC)

	for _, test := range []struct {
		name string
		in   s3.CopyObjectInput
		fail bool
	}{
		{"none", s3.CopyObjectInput{}, false},
		{"if-match", s3.CopyObjectInput{CopySourceIfMatch: aws.String(etag)}, false},
		{"if-match fail", s3.CopyObjectInput{CopySourceIfMatch: aws.String(other)}, true},
		{"if-none-match", s3.CopyObjectInput{CopySourceIfNoneMatch: aws.String(other)}, false},
		{"if-none-match fail", s3.CopyObjectInput{CopySourceIfNoneMatch: aws.String(etag)}, true},
		{"if-modified-since", s3.CopyObjectInput{CopySourceIfModifiedSince: &before}, false},
		{"if-modified-since fail", s3.CopyObjectInput{CopySourceIfModifiedSince: &after}, true},
		{"if-unmodified-since", s3.CopyObjectInput{CopySourceIfUnmodifiedSince: &after}, false},
		{"if-unmodified-since fail", s3.CopyObjectInput{CopySourceIfUnmodifiedSince: &before}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			in := test.in
			in.Bucket = aws.String(defaultBucket)
			in.Key = aws.String("dst-" + test.name)
			in.CopySource = aws.String(defaultBucket + "/src")
			_, err := svc.CopyObject(ctx, &in)
			if test.fail {
				if !s3HasErrorCode(err, gofakes3.ErrPreconditionFailed) {
					t.Fatal("expected PreconditionFailed, got", err)
				}
				if ts.backendObjectExists(defaultBucket, "dst-"+test.name) {
					t.Fatal("object copied despite failed condition")
				}
			} else {
				ts.OK(err)
				ts.assertObject(defaultBucket, "dst-"+test.name, nil, "hello")
			}
		})
	}
}

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
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
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

// TestConditionalPut checks the If-Match and If-None-Match conditions
// on PutObject.
func TestConditionalPut(t *testing.T) {
	ctx := context.Background()
	const etag = `"5d41402abc4b2a76b9719d911017c592"` // md5("hello")

	for _, test := range []struct {
		name        string
		existing    bool
		ifMatch     string
		ifNoneMatch string
		code        gofakes3.ErrorCode
	}{
		{"if-none-match * new", false, "", "*", ""},
		{"if-none-match * existing", true, "", "*", gofakes3.ErrPreconditionFailed},
		{"if-none-match etag existing", true, "", etag, gofakes3.ErrPreconditionFailed},
		{"if-none-match other etag existing", true, "", `"other"`, ""},
		{"if-match existing", true, etag, "", ""},
		{"if-match other etag existing", true, `"other"`, "", gofakes3.ErrPreconditionFailed},
		{"if-match new", false, etag, "", gofakes3.ErrNoSuchKey},
	} {
		t.Run(test.name, func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()
			if test.existing {
				ts.backendPutString(defaultBucket, "object", nil, "hello")
			}
			in := &s3.PutObjectInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
				Body:   bytes.NewReader([]byte("new")),
			}
			if test.ifMatch != "" {
				in.IfMatch = aws.String(test.ifMatch)
			}
			if test.ifNoneMatch != "" {
				in.IfNoneMatch = aws.String(test.ifNoneMatch)
			}
			_, err := ts.s3Client().PutObject(ctx, in)
			if test.code == "" {
				ts.OK(err)
				ts.assertObject(defaultBucket, "object", nil, "new")
				return
			}
			if !s3HasErrorCode(err, test.code) {
				t.Fatal("expected", test.code, "got", err)
			}
			if test.existing {
				ts.assertObject(defaultBucket, "object", nil, "hello")
			} else if ts.backendObjectExists(defaultBucket, "object") {
				t.Fatal("object created despite failed condition")
			}
		})
	}
}

// TestConditionalCompleteMultipartUpload checks a failed condition on
// CompleteMultipartUpload leaves the upload to be completed again.
func TestConditionalCompleteMultipartUpload(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	uploadID := ts.createMultipartUpload(defaultBucket, "object", nil)
	part := ts.uploadPart(defaultBucket, "object", uploadID, 1, []byte("new"))
	in := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("object"),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}},
		IfNoneMatch:     aws.String("*"),
	}
	_, err := svc.CompleteMultipartUpload(ctx, in)
	if !s3HasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Fatal("expected PreconditionFailed, got", err)
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")

	in.IfNoneMatch = nil
	_, err = svc.CompleteMultipartUpload(ctx, in)
	ts.OK(err)
	ts.assertObject(defaultBucket, "object", nil, "new")
}

// conditionCheckingBackend checks the write conditions when an object
// is written as a backend which does so atomically would.
type conditionCheckingBackend struct {
	*s3mem.Backend
	hasConditions bool
	checkErr      error
}

func (b *conditionCheckingBackend) PutObject(ctx context.Context, bucketName, key string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	b.hasConditions = gofakes3.HasWriteConditions(ctx)
	// Pretend the object changed since GoFakeS3 checked it
	b.checkErr = gofakes3.CheckWriteConditions(ctx, true, "changed")
	if b.checkErr != nil {
		return gofakes3.PutObjectResult{}, b.checkErr
	}
	return b.Backend.PutObject(ctx, bucketName, key, meta, input, size)
}

// TestCheckWriteConditions checks the write conditions are passed to
// the backend to check.
func TestCheckWriteConditions(t *testing.T) {
	ctx := context.Background()
	backend := &conditionCheckingBackend{Backend: s3mem.New()}
	ts := newTestServer(t, withBackend(backend))
	defer ts.Close()

	// Without conditions there is nothing to check
	_, err := ts.s3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	ts.OK(err)
	ts.OK(backend.checkErr)
	if backend.hasConditions {
		t.Error("HasWriteConditions true without conditions")
	}

	// The object matches when GoFakeS3 checks, but not when the
	// backend does
	_, err = ts.s3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("object"),
		Body:    bytes.NewReader([]byte("new")),
		IfMatch: aws.String(`"5d41402abc4b2a76b9719d911017c592"`),
	})
	if !s3HasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Fatal("expected PreconditionFailed, got", err)
	}
	if !backend.hasConditions {
		t.Error("HasWriteConditions false with conditions")
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")
}

// TestConditionalDelete checks the conditions on DeleteObject and
// DeleteObjects.
func TestConditionalDelete(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	const etag = `"5d41402abc4b2a76b9719d911017c592"` // md5("hello")
	put := func() {
		_, err := svc.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String("object"),
			Body:   bytes.NewReader([]byte("hello")),
		})
		ts.OK(err)
	}
	wrongTime := defaultDate.Add(time.Hour)

	for _, test := range []struct {
		name string
		in   s3.DeleteObjectInput
		fail bool
	}{
		{"if-match", s3.DeleteObjectInput{IfMatch: aws.String(etag)}, false},
		{"if-match star", s3.DeleteObjectInput{IfMatch: aws.String("*")}, false},
		{"if-match fail", s3.DeleteObjectInput{IfMatch: aws.String(`"bad"`)}, true},
		{"if-match-size", s3.DeleteObjectInput{IfMatchSize: aws.Int64(5)}, false},
		{"if-match-size fail", s3.DeleteObjectInput{IfMatchSize: aws.Int64(9999)}, true},
		{"if-match-last-modified-time", s3.DeleteObjectInput{IfMatchLastModifiedTime: &defaultDate}, false},
		{"if-match-last-modified-time fail", s3.DeleteObjectInput{IfMatchLastModifiedTime: &wrongTime}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			put()
			in := test.in
			in.Bucket, in.Key = aws.String(defaultBucket), aws.String("object")
			_, err := svc.DeleteObject(ctx, &in)
			if test.fail {
				if !s3HasErrorCode(err, gofakes3.ErrPreconditionFailed) {
					t.Fatal("expected PreconditionFailed, got", err)
				}
				ts.assertObject(defaultBucket, "object", nil, "hello")
				return
			}
			ts.OK(err)
			if ts.backendObjectExists(defaultBucket, "object") {
				t.Fatal("object not deleted")
			}
			// Deleting a missing object succeeds whatever the conditions
			_, err = svc.DeleteObject(ctx, &in)
			ts.OK(err)
		})
	}

	// DeleteObjects refuses the objects whose conditions fail
	put()
	ts.backendPutString(defaultBucket, "other", nil, "x")
	out, err := svc.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(defaultBucket),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("object"), ETag: aws.String(`"bad"`)},
			{Key: aws.String("other"), Size: aws.Int64(1)},
			{Key: aws.String("missing"), ETag: aws.String(`"bad"`)},
		}},
	})
	ts.OK(err)
	if len(out.Errors) != 1 || aws.ToString(out.Errors[0].Key) != "object" || aws.ToString(out.Errors[0].Code) != "PreconditionFailed" {
		t.Errorf("unexpected errors %+v", out.Errors)
	}
	if len(out.Deleted) != 2 {
		t.Errorf("unexpected deleted %+v", out.Deleted)
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")
	if ts.backendObjectExists(defaultBucket, "other") {
		t.Error("other not deleted")
	}
}

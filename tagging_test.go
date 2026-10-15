package gofakes3_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

// tagsOf returns the tags of an object as "k=v,..." sorted by key.
func tagsOf(ts *testServer, key string) string {
	ts.Helper()
	out, err := ts.s3Client().GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String(key),
	})
	ts.OK(err)
	var tags []string
	for _, tag := range out.TagSet {
		tags = append(tags, aws.ToString(tag.Key)+"="+aws.ToString(tag.Value))
	}
	return strings.Join(tags, ",")
}

// TestObjectTagging checks the tags of objects can be set, read and
// removed.
func TestObjectTagging(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	put := func(key, tagging string) error {
		in := &s3.PutObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("hello")),
		}
		if tagging != "" {
			in.Tagging = aws.String(tagging)
		}
		_, err := svc.PutObject(ctx, in)
		return err
	}

	// Tags set when the object is created
	ts.OK(put("object", "b=2&a=1"))
	if got := tagsOf(ts, "object"); got != "a=1,b=2" {
		t.Errorf("tags: got %q", got)
	}
	head, err := http.Head(ts.url("/" + defaultBucket + "/object"))
	ts.OK(err)
	if got := head.Header.Get("X-Amz-Tagging-Count"); got != "2" {
		t.Errorf("x-amz-tagging-count: want 2 got %q", got)
	}
	if got := head.Header.Get("X-Amz-Tagging"); got != "" {
		t.Errorf("x-amz-tagging returned: %q", got)
	}

	// Replaced and removed
	_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("object"),
		Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("c"), Value: aws.String("3")}}},
	})
	ts.OK(err)
	if got := tagsOf(ts, "object"); got != "c=3" {
		t.Errorf("tags after put: got %q", got)
	}
	_, err = svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
	})
	ts.OK(err)
	if got := tagsOf(ts, "object"); got != "" {
		t.Errorf("tags after delete: got %q", got)
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")

	// Copies copy the tags unless replaced
	ts.OK(put("tagged", "a=1"))
	_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("copy"),
		CopySource: aws.String(defaultBucket + "/tagged"),
	})
	ts.OK(err)
	if got := tagsOf(ts, "copy"); got != "a=1" {
		t.Errorf("tags of copy: got %q", got)
	}
	_, err = svc.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:           aws.String(defaultBucket),
		Key:              aws.String("copy-replaced"),
		CopySource:       aws.String(defaultBucket + "/tagged"),
		TaggingDirective: types.TaggingDirectiveReplace,
		Tagging:          aws.String("z=26"),
	})
	ts.OK(err)
	if got := tagsOf(ts, "copy-replaced"); got != "z=26" {
		t.Errorf("tags of copy with REPLACE: got %q", got)
	}

	// Multipart uploads are created with their tags
	mpu, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("multipart"),
		Tagging: aws.String("m=1"),
	})
	ts.OK(err)
	part := ts.uploadPart(defaultBucket, "multipart", *mpu.UploadId, 1, []byte("abc"))
	ts.assertCompleteUpload(defaultBucket, "multipart", *mpu.UploadId, []types.CompletedPart{part}, []byte("abc"))
	if got := tagsOf(ts, "multipart"); got != "m=1" {
		t.Errorf("tags of multipart upload: got %q", got)
	}

	// Invalid tags are refused
	var tooMany []types.Tag
	var tooManyHeader []string
	for i := range 11 {
		tooMany = append(tooMany, types.Tag{Key: aws.String(fmt.Sprint("k", i)), Value: aws.String("v")})
		tooManyHeader = append(tooManyHeader, fmt.Sprintf("k%d=v", i))
	}
	for _, tags := range [][]types.Tag{
		tooMany,
		{{Key: aws.String(strings.Repeat("k", 129)), Value: aws.String("v")}},
		{{Key: aws.String("k"), Value: aws.String(strings.Repeat("v", 257))}},
		{{Key: aws.String("k"), Value: aws.String("1")}, {Key: aws.String("k"), Value: aws.String("2")}},
	} {
		_, err = svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket:  aws.String(defaultBucket),
			Key:     aws.String("object"),
			Tagging: &types.Tagging{TagSet: tags},
		})
		if !s3HasErrorCode(err, gofakes3.ErrInvalidTag) {
			t.Errorf("expected InvalidTag, got %v", err)
		}
	}
	if err := put("too-many", strings.Join(tooManyHeader, "&")); !s3HasErrorCode(err, gofakes3.ErrInvalidTag) {
		t.Errorf("expected InvalidTag, got %v", err)
	}
	if ts.backendObjectExists(defaultBucket, "too-many") {
		t.Errorf("object with invalid tags created")
	}

	// The object must exist
	_, err = svc.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("missing"),
	})
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("expected NoSuchKey, got %v", err)
	}
}

// backendOnly hides the optional interfaces of a Backend.
type backendOnly struct {
	gofakes3.Backend
}

// TestObjectTaggingUnsupported checks tags can be read but not changed
// with a backend which doesn't implement ObjectTaggingBackend.
func TestObjectTaggingUnsupported(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, withBackend(backendOnly{s3mem.New()}))
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(defaultBucket),
		Key:     aws.String("object"),
		Body:    bytes.NewReader([]byte("hello")),
		Tagging: aws.String("a=1"),
	})
	ts.OK(err)
	if got := tagsOf(ts, "object"); got != "a=1" {
		t.Errorf("tags: got %q", got)
	}
	_, err = svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
	})
	if !s3HasErrorCode(err, gofakes3.ErrNotImplemented) {
		t.Errorf("expected NotImplemented, got %v", err)
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")
}

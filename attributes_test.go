package gofakes3_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

// partSizesBackend reports the object "multi" as made of parts.
type partSizesBackend struct {
	*s3mem.Backend
}

var multiPartSizes = []int64{3, 3, 2}

func (b *partSizesBackend) HeadObject(ctx context.Context, bucketName, objectName string) (*gofakes3.Object, error) {
	obj, err := b.Backend.HeadObject(ctx, bucketName, objectName)
	if err == nil && objectName == "multi" {
		obj.PartSizes = multiPartSizes
	}
	return obj, err
}

func (b *partSizesBackend) GetObject(ctx context.Context, bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	obj, err := b.Backend.GetObject(ctx, bucketName, objectName, rangeRequest)
	if err == nil && objectName == "multi" {
		obj.PartSizes = multiPartSizes
	}
	return obj, err
}

// TestGetObjectPartNumber checks GET and HEAD of a part of an object.
func TestGetObjectPartNumber(t *testing.T) {
	ts := newTestServer(t, withBackend(&partSizesBackend{s3mem.New()}))
	defer ts.Close()
	ts.backendPutString(defaultBucket, "multi", nil, "aaabbbcc")
	ts.backendPutString(defaultBucket, "single", nil, "body")

	for _, test := range []struct {
		method     string
		key        string
		query      string
		headers    map[string]string
		status     int
		body       string
		length     string
		partsCount string
	}{
		{"GET", "multi", "?partNumber=1", nil, http.StatusPartialContent, "aaa", "3", "3"},
		{"GET", "multi", "?partNumber=2", nil, http.StatusPartialContent, "bbb", "3", "3"},
		{"GET", "multi", "?partNumber=3", nil, http.StatusPartialContent, "cc", "2", "3"},
		{"HEAD", "multi", "?partNumber=3", nil, http.StatusPartialContent, "", "2", "3"},
		{"GET", "multi", "?partNumber=4", nil, http.StatusBadRequest, "", "", ""},
		{"GET", "multi", "?partNumber=0", nil, http.StatusBadRequest, "", "", ""},
		{"GET", "multi", "?partNumber=1", map[string]string{"Range": "bytes=0-1"}, http.StatusBadRequest, "", "", ""},
		{"GET", "single", "?partNumber=1", nil, http.StatusOK, "body", "4", ""},
		{"GET", "single", "?partNumber=2", nil, http.StatusBadRequest, "", "", ""},
		{"GET", "missing", "?partNumber=1", nil, http.StatusNotFound, "", "", ""},
	} {
		t.Run(test.method+" "+test.key+test.query, func(t *testing.T) {
			rq, err := http.NewRequest(test.method, ts.url("/"+defaultBucket+"/"+test.key+test.query), nil)
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
			if test.status >= 300 {
				return
			}
			if string(body) != test.body {
				t.Errorf("body: want %q got %q", test.body, body)
			}
			if got := rs.Header.Get("Content-Length"); got != test.length {
				t.Errorf("Content-Length: want %q got %q", test.length, got)
			}
			if got := rs.Header.Get("X-Amz-Mp-Parts-Count"); got != test.partsCount {
				t.Errorf("x-amz-mp-parts-count: want %q got %q", test.partsCount, got)
			}
		})
	}
}

// TestGetObjectAttributes checks GetObjectAttributes.
func TestGetObjectAttributes(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t, withBackend(&partSizesBackend{s3mem.New()}))
	defer ts.Close()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "multi", nil, "aaabbbcc")

	// An object with a checksum
	rq, err := http.NewRequest("PUT", ts.url("/"+defaultBucket+"/single"), strings.NewReader("hello"))
	ts.OK(err)
	rq.Header.Set("X-Amz-Checksum-Crc32", "NhCmhg==") // crc32("hello")
	rs, err := httpClient().Do(rq)
	ts.OK(err)
	_ = rs.Body.Close()

	all := []types.ObjectAttributes{
		types.ObjectAttributesEtag,
		types.ObjectAttributesChecksum,
		types.ObjectAttributesObjectParts,
		types.ObjectAttributesStorageClass,
		types.ObjectAttributesObjectSize,
	}
	out, err := svc.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String(defaultBucket),
		Key:              aws.String("single"),
		ObjectAttributes: all,
	})
	ts.OK(err)
	if aws.ToString(out.ETag) != "5d41402abc4b2a76b9719d911017c592" { // md5("hello")
		t.Errorf("ETag: got %q", aws.ToString(out.ETag))
	}
	if aws.ToInt64(out.ObjectSize) != 5 || out.StorageClass != types.StorageClassStandard || out.ObjectParts != nil {
		t.Errorf("unexpected attributes %+v", out)
	}
	if out.Checksum == nil || aws.ToString(out.Checksum.ChecksumCRC32) != "NhCmhg==" {
		t.Errorf("Checksum: got %+v", out.Checksum)
	}

	out, err = svc.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String(defaultBucket),
		Key:              aws.String("multi"),
		ObjectAttributes: all,
		MaxParts:         aws.Int32(1),
		PartNumberMarker: aws.String("1"),
	})
	ts.OK(err)
	parts := out.ObjectParts
	if parts == nil || aws.ToInt32(parts.TotalPartsCount) != 3 || !aws.ToBool(parts.IsTruncated) ||
		aws.ToString(parts.NextPartNumberMarker) != "2" || len(parts.Parts) != 1 ||
		aws.ToInt32(parts.Parts[0].PartNumber) != 2 || aws.ToInt64(parts.Parts[0].Size) != 3 {
		t.Errorf("unexpected ObjectParts %+v", parts)
	}
	if out.ETag == nil || out.Checksum != nil {
		t.Errorf("unexpected attributes %+v", out)
	}

	_, err = svc.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String(defaultBucket),
		Key:              aws.String("missing"),
		ObjectAttributes: all,
	})
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("expected NoSuchKey got %v", err)
	}
}

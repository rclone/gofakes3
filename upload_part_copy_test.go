package gofakes3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

// TestUploadPartCopy checks parts of a multipart upload can be copied
// from existing objects, both with GoFakeS3's own uploader and with a
// MultipartBackend.
func TestUploadPartCopy(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "uploader"
		if streaming {
			name = "streaming"
		}
		t.Run(name, func(t *testing.T) {
			var opts []testServerOption
			if streaming {
				opts = append(opts, withBackend(newStreamingBackend(s3mem.New())))
			}
			ts := newTestServer(t, opts...)
			defer ts.Close()
			testUploadPartCopy(t, ts)
		})
	}
}

func testUploadPartCopy(t *testing.T, ts *testServer) {
	ctx := context.Background()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "src", nil, "0123456789")
	source := aws.String(defaultBucket + "/src")

	id := ts.createMultipartUpload(defaultBucket, "dst", nil)
	copyPart := func(partNumber int32, rnge string, in *s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error) {
		if in == nil {
			in = &s3.UploadPartCopyInput{}
		}
		in.Bucket = aws.String(defaultBucket)
		in.Key = aws.String("dst")
		in.UploadId = aws.String(id)
		in.PartNumber = aws.Int32(partNumber)
		in.CopySource = source
		if rnge != "" {
			in.CopySourceRange = aws.String(rnge)
		}
		return svc.UploadPartCopy(ctx, in)
	}

	// Bad ranges and sources are refused
	for _, rnge := range []string{"0-2", "bytes=0", "bytes=hello-world", "bytes=0-bar", "bytes=0-2,3-5"} {
		_, err := copyPart(1, rnge, nil)
		if !s3HasErrorCode(err, gofakes3.ErrInvalidArgument) {
			t.Errorf("range %q: expected InvalidArgument, got %v", rnge, err)
		}
	}
	for _, rnge := range []string{"bytes=0-10", "bytes=5-4"} {
		_, err := copyPart(1, rnge, nil)
		if !s3HasErrorCode(err, gofakes3.ErrInvalidRange) {
			t.Errorf("range %q: expected InvalidRange, got %v", rnge, err)
		}
	}
	_, err := copyPart(1, "", &s3.UploadPartCopyInput{CopySourceIfMatch: aws.String(`"wrong"`)})
	if !s3HasErrorCode(err, gofakes3.ErrPreconditionFailed) {
		t.Errorf("expected PreconditionFailed, got %v", err)
	}
	_, err = svc.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("dst"),
		UploadId:   aws.String(id),
		PartNumber: aws.Int32(1),
		CopySource: aws.String(defaultBucket + "/missing"),
	})
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("expected NoSuchKey, got %v", err)
	}

	// Copy a range, the whole object, then upload a part
	var parts []types.CompletedPart
	out, err := copyPart(1, "bytes=2-4", nil)
	ts.OK(err)
	parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(1), ETag: out.CopyPartResult.ETag})
	out, err = copyPart(2, "", nil)
	ts.OK(err)
	if *out.CopyPartResult.ETag == "" || out.CopyPartResult.LastModified == nil {
		t.Fatalf("incomplete CopyPartResult: %+v", out.CopyPartResult)
	}
	parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(2), ETag: out.CopyPartResult.ETag})
	parts = append(parts, ts.uploadPart(defaultBucket, "dst", id, 3, []byte("abc")))

	_, err = svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("dst"),
		UploadId:        aws.String(id),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	ts.OK(err)
	if got := ts.backendGetString(defaultBucket, "dst", nil); got != "2340123456789abc" {
		t.Fatalf("wrong object contents %q", got)
	}
}

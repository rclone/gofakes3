package gofakes3_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
)

const autoBucket = "autobucket"

func newAutoBucketTestServer(t *testing.T) *testServer {
	t.Helper()
	return newTestServer(t,
		withoutInitialBuckets(),
		withFakerOptions(gofakes3.WithAutoBucket(true)))
}

func TestAutoBucketPutObject(t *testing.T) {
	ctx := context.Background()
	autoSrv := newAutoBucketTestServer(t)
	defer autoSrv.Close()
	svc := autoSrv.s3Client()

	_, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	})
	if err != nil {
		t.Fatal(err)
	}
	autoSrv.assertObject(autoBucket, "object", nil, "hello")
}

func TestAutoBucketGetObject(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
	})
	if !hasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObject(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(autoBucket),
		Key:    aws.String("object"),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketGetBucketLocation(t *testing.T) {
	ctx := context.Background()
	autoSrv := newAutoBucketTestServer(t)
	defer autoSrv.Close()
	svc := autoSrv.s3Client()

	_, err := svc.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObjectVersion(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(autoBucket),
		Key:       aws.String("object"),
		VersionId: aws.String("version"),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketDeleteObjectsVersion(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("object1"), VersionId: aws.String("version1")},
				{Key: aws.String("object2"), VersionId: aws.String("version2")},
			},
		},
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAutoBucketListMultipartUploads(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(autoBucket),
	})
	if !hasErrorCode(err, gofakes3.ErrNoSuchUpload) {
		t.Fatal(err)
	}
}

func TestAutoBucketGetBucketVersioning(t *testing.T) {
	ctx := context.Background()
	ts := newAutoBucketTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	_, err := svc.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(autoBucket),
	})
	if err != nil {
		t.Fatal(err)
	}
}

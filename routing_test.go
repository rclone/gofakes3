package gofakes3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
)

func TestRoutingSlashes(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	ts.backendCreateBucket("test")
	ts.backendPutString("test", "obj", nil, "yep")

	client := httpClient()

	assertStatus := func(url string, code int) {
		t.Helper()
		rs, err := client.Head(ts.url(url))
		ts.OK(err)
		if rs.StatusCode != code {
			t.Fatal("expected status", code, "found", rs.StatusCode)
		}
	}

	assertStatus("nope", 404) // sanity check missing URL
	assertStatus("test", 200)
	assertStatus("test/", 200)
	assertStatus("test/nope", 404)
	assertStatus("test/obj", 200)

	// Slashes at the start and end of keys are part of the key
	assertStatus("test/obj/", 404)
	assertStatus("test//obj", 404)
	ts.backendPutString("test", "dir/", nil, "")
	ts.backendPutString("test", "/rooted", nil, "")
	assertStatus("test/dir/", 200)
	assertStatus("test/dir", 404)
	assertStatus("test//rooted", 200)
	assertStatus("test/rooted", 404)

	// Slashes before the bucket are ignored
	rs, err := client.Head(ts.server.URL + "//test/obj")
	ts.OK(err)
	if rs.StatusCode != 200 {
		t.Fatal("expected status 200 for //test/obj, found", rs.StatusCode)
	}
}

// TestRoutingUnsupportedSubresources checks requests for sub-resources
// which aren't implemented are refused and don't touch the object or
// bucket they name.
func TestRoutingUnsupportedSubresources(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ctx := context.Background()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	for _, test := range []struct {
		name string
		call func() error
	}{
		{"PutObjectAcl", func() error {
			_, err := svc.PutObjectAcl(ctx, &s3.PutObjectAclInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
				ACL:    types.ObjectCannedACLPrivate,
			})
			return err
		}},
		{"GetObjectAcl", func() error {
			_, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
			})
			return err
		}},
		{"PutObjectTagging", func() error {
			_, err := svc.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
				Bucket:  aws.String(defaultBucket),
				Key:     aws.String("object"),
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
			})
			return err
		}},
		{"DeleteObjectTagging", func() error {
			_, err := svc.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
			})
			return err
		}},
		{"PutObjectRetention", func() error {
			_, err := svc.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
				Bucket:    aws.String(defaultBucket),
				Key:       aws.String("object"),
				Retention: &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance},
			})
			return err
		}},
		{"PutBucketPolicy", func() error {
			_, err := svc.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
				Bucket: aws.String(defaultBucket),
				Policy: aws.String("{}"),
			})
			return err
		}},
		{"DeleteBucketPolicy", func() error {
			_, err := svc.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
				Bucket: aws.String(defaultBucket),
			})
			return err
		}},
		{"DeleteBucketEncryption", func() error {
			_, err := svc.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{
				Bucket: aws.String(defaultBucket),
			})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if !s3HasErrorCode(err, gofakes3.ErrNotImplemented) {
				t.Fatal("expected NotImplemented, found", err)
			}
			ts.assertObject(defaultBucket, "object", nil, "hello")
		})
	}

	t.Run("NoSuchBucket", func(t *testing.T) {
		_, err := svc.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
			Bucket: aws.String("missing"),
		})
		if !s3HasErrorCode(err, gofakes3.ErrNoSuchBucket) {
			t.Fatal("expected NoSuchBucket, found", err)
		}
	})
}

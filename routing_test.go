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
// which aren't implemented are refused, or answered as S3 does when
// they aren't configured, and don't touch the object or bucket they
// name.
func TestRoutingUnsupportedSubresources(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ctx := context.Background()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "object", nil, "hello")

	for _, test := range []struct {
		name string
		code gofakes3.ErrorCode // "" for success
		call func() error
	}{
		{"PutObjectAcl", "", func() error {
			_, err := svc.PutObjectAcl(ctx, &s3.PutObjectAclInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
				ACL:    types.ObjectCannedACLPrivate,
			})
			return err
		}},
		{"GetObjectAcl", "", func() error {
			_, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{
				Bucket: aws.String(defaultBucket),
				Key:    aws.String("object"),
			})
			return err
		}},
		{"PutBucketTagging", gofakes3.ErrNotImplemented, func() error {
			_, err := svc.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
				Bucket:  aws.String(defaultBucket),
				Tagging: &types.Tagging{TagSet: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}},
			})
			return err
		}},
		{"PutObjectRetention", gofakes3.ErrInvalidRequest, func() error {
			_, err := svc.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
				Bucket:    aws.String(defaultBucket),
				Key:       aws.String("object"),
				Retention: &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeGovernance},
			})
			return err
		}},
		{"PutBucketPolicy", gofakes3.ErrNotImplemented, func() error {
			_, err := svc.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
				Bucket: aws.String(defaultBucket),
				Policy: aws.String("{}"),
			})
			return err
		}},
		{"DeleteBucketPolicy", "", func() error {
			_, err := svc.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
				Bucket: aws.String(defaultBucket),
			})
			return err
		}},
		{"DeleteBucketEncryption", "", func() error {
			_, err := svc.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{
				Bucket: aws.String(defaultBucket),
			})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if test.code == "" {
				ts.OK(err)
			} else if !s3HasErrorCode(err, test.code) {
				t.Fatal("expected", test.code, "found", err)
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

// TestRoutingMisusedSubresources checks requests for implemented
// sub-resources with a method they aren't implemented for are refused,
// and don't touch the object or bucket they name.
func TestRoutingMisusedSubresources(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	ts.backendPutString(defaultBucket, "object", nil, "hello")
	client := httpClient()

	for _, test := range []struct {
		method string
		url    string
	}{
		{"PUT", defaultBucket + "/object?attributes"},
		{"DELETE", defaultBucket + "/object?attributes"},
		{"PUT", defaultBucket + "/object?location"},
		{"DELETE", defaultBucket + "/object?delete"},
		{"DELETE", defaultBucket + "?location"},
		{"DELETE", defaultBucket + "?delete"},
		{"PUT", defaultBucket + "?attributes"},
	} {
		t.Run(test.method+" "+test.url, func(t *testing.T) {
			rq, err := http.NewRequest(test.method, ts.url(test.url), strings.NewReader("data"))
			ts.OK(err)
			rs, err := client.Do(rq)
			ts.OK(err)
			_ = rs.Body.Close()
			if rs.StatusCode != http.StatusBadRequest {
				t.Error("expected status 400, found", rs.StatusCode)
			}
			// Check the bucket and object are untouched, restoring
			// them for the next test if not
			ctx := context.Background()
			if exists, _ := ts.backend.BucketExists(ctx, defaultBucket); !exists {
				t.Error("bucket deleted")
				ts.OK(ts.backend.CreateBucket(ctx, defaultBucket))
			}
			obj, err := ts.backend.GetObject(ctx, defaultBucket, "object", nil)
			if err != nil {
				t.Error("object not found:", err)
			} else {
				data, _ := io.ReadAll(obj.Contents)
				_ = obj.Contents.Close()
				if string(data) != "hello" {
					t.Errorf("object changed to %q", data)
				}
			}
			ts.backendPutString(defaultBucket, "object", nil, "hello")
		})
	}

	// The methods they are implemented for still work
	for _, url := range []string{defaultBucket + "/object?attributes", defaultBucket + "?location"} {
		rq, err := http.NewRequest("HEAD", ts.url(url), nil)
		ts.OK(err)
		rs, err := client.Do(rq)
		ts.OK(err)
		_ = rs.Body.Close()
		if rs.StatusCode != http.StatusOK {
			t.Error(url, "expected status 200, found", rs.StatusCode)
		}
	}
}

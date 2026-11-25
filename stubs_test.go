package gofakes3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
)

// TestACLStub checks every bucket and object has the private ACL,
// which is the only one which can be set.
func TestACLStub(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "object", nil, "hello")
	bucket, key := aws.String(defaultBucket), aws.String("object")

	checkACL := func(owner *types.Owner, grants []types.Grant) {
		t.Helper()
		if aws.ToString(owner.ID) != "fe7272ea58be830e56fe1663b10fafef" {
			t.Errorf("owner: got %+v", owner)
		}
		if len(grants) != 1 || grants[0].Permission != types.PermissionFullControl ||
			grants[0].Grantee.Type != types.TypeCanonicalUser || aws.ToString(grants[0].Grantee.ID) != aws.ToString(owner.ID) {
			t.Errorf("grants: got %+v", grants)
		}
	}
	bucketACL, err := svc.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: bucket})
	ts.OK(err)
	checkACL(bucketACL.Owner, bucketACL.Grants)
	objectACL, err := svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: bucket, Key: key})
	ts.OK(err)
	checkACL(objectACL.Owner, objectACL.Grants)

	_, err = svc.GetObjectAcl(ctx, &s3.GetObjectAclInput{Bucket: bucket, Key: aws.String("missing")})
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchKey) {
		t.Errorf("expected NoSuchKey, got %v", err)
	}

	for _, test := range []struct {
		acl  types.ObjectCannedACL
		code gofakes3.ErrorCode
	}{
		{types.ObjectCannedACLPrivate, ""},
		{types.ObjectCannedACLPublicRead, gofakes3.ErrNotImplemented},
		{"bogus", gofakes3.ErrInvalidArgument},
	} {
		_, err = svc.PutObjectAcl(ctx, &s3.PutObjectAclInput{Bucket: bucket, Key: key, ACL: test.acl})
		if test.code == "" {
			ts.OK(err)
		} else if !s3HasErrorCode(err, test.code) {
			t.Errorf("%s: expected %s got %v", test.acl, test.code, err)
		}
	}

	// Setting the ACL just read works, but adding to it doesn't
	_, err = svc.PutBucketAcl(ctx, &s3.PutBucketAclInput{
		Bucket:              bucket,
		AccessControlPolicy: &types.AccessControlPolicy{Owner: bucketACL.Owner, Grants: bucketACL.Grants},
	})
	ts.OK(err)
	grants := append(bucketACL.Grants, types.Grant{
		Grantee:    &types.Grantee{Type: types.TypeGroup, URI: aws.String("http://acs.amazonaws.com/groups/global/AllUsers")},
		Permission: types.PermissionRead,
	})
	_, err = svc.PutBucketAcl(ctx, &s3.PutBucketAclInput{
		Bucket:              bucket,
		AccessControlPolicy: &types.AccessControlPolicy{Owner: bucketACL.Owner, Grants: grants},
	})
	if !s3HasErrorCode(err, gofakes3.ErrNotImplemented) {
		t.Errorf("expected NotImplemented, got %v", err)
	}
	ts.assertObject(defaultBucket, "object", nil, "hello")
}

// TestUnconfiguredStubs checks the bucket configuration which isn't
// supported is reported as not configured.
func TestUnconfiguredStubs(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()
	ts.backendPutString(defaultBucket, "object", nil, "hello")
	bucket, key := aws.String(defaultBucket), aws.String("object")

	for _, test := range []struct {
		name string
		code string
		call func() error
	}{
		{"GetBucketPolicy", "NoSuchBucketPolicy", func() error {
			_, err := svc.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: bucket})
			return err
		}},
		{"GetBucketLifecycleConfiguration", "NoSuchLifecycleConfiguration", func() error {
			_, err := svc.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: bucket})
			return err
		}},
		{"GetBucketCors", "NoSuchCORSConfiguration", func() error {
			_, err := svc.GetBucketCors(ctx, &s3.GetBucketCorsInput{Bucket: bucket})
			return err
		}},
		{"GetBucketEncryption", "ServerSideEncryptionConfigurationNotFoundError", func() error {
			_, err := svc.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: bucket})
			return err
		}},
		{"GetBucketTagging", "NoSuchTagSet", func() error {
			_, err := svc.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: bucket})
			return err
		}},
		{"GetBucketWebsite", "NoSuchWebsiteConfiguration", func() error {
			_, err := svc.GetBucketWebsite(ctx, &s3.GetBucketWebsiteInput{Bucket: bucket})
			return err
		}},
		{"GetBucketReplication", "ReplicationConfigurationNotFoundError", func() error {
			_, err := svc.GetBucketReplication(ctx, &s3.GetBucketReplicationInput{Bucket: bucket})
			return err
		}},
		{"GetObjectLockConfiguration", "ObjectLockConfigurationNotFoundError", func() error {
			_, err := svc.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: bucket})
			return err
		}},
		{"GetBucketOwnershipControls", "OwnershipControlsNotFoundError", func() error {
			_, err := svc.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: bucket})
			return err
		}},
		{"GetPublicAccessBlock", "NoSuchPublicAccessBlockConfiguration", func() error {
			_, err := svc.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{Bucket: bucket})
			return err
		}},
		{"PutObjectLockConfiguration", "InvalidBucketState", func() error {
			_, err := svc.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{
				Bucket:                  bucket,
				ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled},
			})
			return err
		}},
		{"GetObjectRetention", "InvalidRequest", func() error {
			_, err := svc.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: bucket, Key: key})
			return err
		}},
		{"GetObjectLegalHold", "InvalidRequest", func() error {
			_, err := svc.GetObjectLegalHold(ctx, &s3.GetObjectLegalHoldInput{Bucket: bucket, Key: key})
			return err
		}},
		{"PutBucketLifecycleConfiguration", "NotImplemented", func() error {
			_, err := svc.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
				Bucket: bucket,
				LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
					Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
				}}},
			})
			return err
		}},
		{"DeleteBucketPolicy", "", func() error {
			_, err := svc.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{Bucket: bucket})
			return err
		}},
		{"DeleteBucketLifecycle", "", func() error {
			_, err := svc.DeleteBucketLifecycle(ctx, &s3.DeleteBucketLifecycleInput{Bucket: bucket})
			return err
		}},
		{"DeletePublicAccessBlock", "", func() error {
			_, err := svc.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{Bucket: bucket})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if test.code == "" {
				ts.OK(err)
			} else if !s3HasErrorCode(err, gofakes3.ErrorCode(test.code)) {
				t.Fatal("expected", test.code, "got", err)
			}
			ts.assertObject(defaultBucket, "object", nil, "hello")
		})
	}

	// Without authentication the bucket is public
	status, err := svc.GetBucketPolicyStatus(ctx, &s3.GetBucketPolicyStatusInput{Bucket: bucket})
	ts.OK(err)
	if !aws.ToBool(status.PolicyStatus.IsPublic) {
		t.Errorf("expected bucket without authentication to be public")
	}
}

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Does GetBucketVersioning return ErrNoSuchBucket when a nonexistent bucket is used?
// Does PutBucketVersioning return ErrNoSuchBucket when a nonexistent bucket is used?
type S300007BucketVersioningNoSuchBucket struct{}

func (s S300007BucketVersioningNoSuchBucket) Run(ctx *Context) error {
	client := ctx.S3Client()

	var b [40]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return err
	}
	bucket := hex.EncodeToString(b[:])

	{ // GetBucketVersioning
		rs, err := client.GetBucketVersioning(ctx.Context, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		_ = rs
		if aerr := (awsError)(nil); errors.As(err, &aerr) {
			if aerr.Code() != "NoSuchBucket" {
				return fmt.Errorf("expected NoSuchBucket, found %s", aerr.Code())
			}
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	{ // PutBucketVersioning
		rs, err := client.PutBucketVersioning(ctx.Context, &s3.PutBucketVersioningInput{
			Bucket: aws.String("gofakes3.shabbyrobe.org"),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: "enorbled",
			},
		})
		_ = rs
		if aerr := (awsError)(nil); errors.As(err, &aerr) {
			if aerr.Code() != "NoSuchBucket" {
				return fmt.Errorf("expected NoSuchBucket, found %s", aerr.Code())
			}
		} else if err != nil {
			return err
		} else {
			return fmt.Errorf("expected NoSuchBucket, but call succeeded: %+v", rs)
		}
	}

	return nil
}

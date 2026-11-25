package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Does GetBucketLocation return ErrNoSuchBucket when a nonexistent bucket is used?
type S300006GetBucketLocationNoSuchBucket struct{}

func (s S300006GetBucketLocationNoSuchBucket) Run(ctx *Context) error {
	client := ctx.S3Client()

	var b [40]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return err
	}
	bucket := hex.EncodeToString(b[:])

	{ // Sanity check version length
		rs, err := client.GetBucketLocation(ctx.Context, &s3.GetBucketLocationInput{
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

	return nil
}

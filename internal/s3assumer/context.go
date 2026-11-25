package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Context struct {
	context.Context
	config Config
	rand   *rand.Rand
}

func (c *Context) Config() Config           { return c.config }
func (c *Context) Rand() *rand.Rand         { return c.rand }
func (c *Context) RandString(sz int) string { return hex.EncodeToString(c.RandBytes(sz)[:sz]) }

func (c *Context) RandBytes(sz int) []byte {
	out := make([]byte, sz)
	c.rand.Read(out)
	return out
}

func (c *Context) S3Client() *s3.Client {
	var config = aws.Config{
		Region: c.config.S3Region,
	}
	if c.config.Verbose {
		config.ClientLogMode = aws.ClientLogMode(math.MaxInt64)
	}
	svc := s3.NewFromConfig(config, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(c.config.S3Endpoint)
		o.UsePathStyle = c.config.S3PathStyle
		v := aws.Credentials{
			AccessKeyID:     "dummy-access",
			SecretAccessKey: "dummy-secret",
		}
		o.Credentials = &credentials.StaticCredentialsProvider{Value: v}
	})
	return svc
}

func (c *Context) EnsureVersioningEnabled(client *s3.Client, bucket string) error {
	vers, err := c.getBucketVersioning(client, bucket)
	if err != nil {
		return err
	}

	status := vers.Status
	if status != "Enabled" {
		if _, err := client.PutBucketVersioning(c.Context, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: "Enabled",
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *Context) EnsureVersioningNeverEnabled(client *s3.Client, bucket string) error {
	vers, err := c.getBucketVersioning(client, bucket)
	if err != nil {
		return err
	}

	if vers.Status != "" {
		return fmt.Errorf("unexpected status, found %q", vers.Status)
	}

	return nil
}

func (c *Context) getBucketVersioning(client *s3.Client, bucket string) (*s3.GetBucketVersioningOutput, error) {
	vers, err := client.GetBucketVersioning(c.Context, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return nil, err
	}

	status := vers.Status
	if status != "" && status != "Enabled" && status != "Suspended" {
		return nil, fmt.Errorf("unexpected status %q", status)
	}
	mfaDelete := vers.MFADelete
	if mfaDelete != "" && mfaDelete != "Disabled" {
		return nil, fmt.Errorf("unexpected MFADelete %q", vers.MFADelete)
	}

	return vers, nil
}

type Logger struct{}

func (l Logger) Log(vs ...interface{}) {
	fmt.Println(vs...)
	fmt.Println()
}

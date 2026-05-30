package gofakes3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

const (
	prefix    = "/s3"
	accessKey = "AKIAIOSFODNN7EXAMPLE"
	secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// TestPathPrefix verifies that WithPathPrefix re-adds the configured prefix to
// the URL before signature verification, mirroring the behaviour of a reverse
// proxy that strips the prefix before forwarding to gofakes3.
func TestPathPrefix(t *testing.T) {
	ctx := context.Background()
	backend := s3mem.New()

	faker := gofakes3.New(backend,
		gofakes3.WithV4Auth(map[string]string{accessKey: secretKey}),
		gofakes3.WithPathPrefixForSignatureVerification(prefix),
		gofakes3.WithTimeSkewLimit(0),
	)

	// Reverse proxy stub: strip the prefix before forwarding so gofakes3 sees
	// the same path a real proxy would deliver.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, prefix) {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		faker.Server().ServeHTTP(w, r)
	}))
	defer proxy.Close()

	svc := s3.NewFromConfig(aws.Config{Region: "region"}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(proxy.URL + prefix)
		o.UsePathStyle = true
		o.Credentials = &credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		}}
	})

	_, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
}

// TestPathPrefixRejectsBadSignature verifies that the path prefix actually
// participates in signature verification: a request signed without accounting
// for the prefix must be rejected.
func TestPathPrefixRejectsBadSignature(t *testing.T) {
	ctx := context.Background()
	backend := s3mem.New()

	faker := gofakes3.New(backend,
		gofakes3.WithV4Auth(map[string]string{accessKey: secretKey}),
		gofakes3.WithPathPrefixForSignatureVerification(prefix),
		gofakes3.WithTimeSkewLimit(0),
	)

	// No prefix stripping here: client signs without the prefix, so the
	// re-prepended path on the server side will not match.
	srv := httptest.NewServer(faker.Server())
	defer srv.Close()

	svc := s3.NewFromConfig(aws.Config{Region: "region"}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
		o.Credentials = &credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		}}
	})

	_, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err == nil {
		t.Fatal("expected signature mismatch error, got nil")
	}
}

package gofakes3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

type ownerKey struct{}

var credentialRe = regexp.MustCompile(`Credential=([^/]+)/`)

// newOwnerTestServer serves backend with multipart uploads owned by the
// access key ID each request is signed with, returning a client for
// each of owners.
func newOwnerTestServer(t *testing.T, backend gofakes3.Backend, owners ...string) map[string]*s3.Client {
	t.Helper()
	if err := backend.CreateBucket(context.Background(), defaultBucket); err != nil {
		t.Fatal(err)
	}
	faker := gofakes3.New(backend,
		gofakes3.WithTimeSkewLimit(0),
		gofakes3.WithGlobalLog(),
		gofakes3.WithUploadOwner(func(ctx context.Context) string {
			owner, _ := ctx.Value(ownerKey{}).(string)
			return owner
		}),
	)
	handler := faker.Server()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m := credentialRe.FindStringSubmatch(r.Header.Get("Authorization")); m != nil {
			r = r.WithContext(context.WithValue(r.Context(), ownerKey{}, m[1]))
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)

	clients := map[string]*s3.Client{}
	for _, owner := range owners {
		clients[owner] = s3.NewFromConfig(aws.Config{Region: "region"}, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(server.URL)
			o.UsePathStyle = true
			o.Credentials = credentials.NewStaticCredentialsProvider(owner, "secret", "")
		})
	}
	return clients
}

// TestUploadOwner checks that with WithUploadOwner one owner can't see
// or use another owner's multipart upload, whether gofakes3 or the
// backend holds its parts.
func TestUploadOwner(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backend func() gofakes3.Backend
	}{
		{"InMemory", func() gofakes3.Backend { return s3mem.New() }},
		{"Streaming", func() gofakes3.Backend { return newStreamingBackend(s3mem.New()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clients := newOwnerTestServer(t, tc.backend(), "alice", "bob")
			alice, bob := clients["alice"], clients["bob"]
			key := aws.String("object")

			create, err := alice.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(defaultBucket), Key: key})
			if err != nil {
				t.Fatal(err)
			}
			uploadID := create.UploadId

			listed := func(client *s3.Client) bool {
				out, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(defaultBucket)})
				if err != nil && !strings.Contains(err.Error(), "NoSuchUpload") {
					t.Fatal(err)
				}
				return err == nil && len(out.Uploads) > 0
			}
			if listed(bob) {
				t.Error("bob can list alice's upload")
			}
			if !listed(alice) {
				t.Error("alice can't list her own upload")
			}

			noSuchUpload := func(what string, err error) {
				t.Helper()
				if !hasErrorCode(err, gofakes3.ErrNoSuchUpload) {
					t.Errorf("%s: want NoSuchUpload, got %v", what, err)
				}
			}
			_, err = bob.ListParts(ctx, &s3.ListPartsInput{Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID})
			noSuchUpload("bob ListParts", err)
			_, err = bob.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID, PartNumber: aws.Int32(1), Body: strings.NewReader("bob's")})
			noSuchUpload("bob UploadPart", err)
			_, err = bob.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID})
			noSuchUpload("bob CompleteMultipartUpload", err)
			_, err = bob.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID})
			noSuchUpload("bob AbortMultipartUpload", err)

			part, err := alice.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID, PartNumber: aws.Int32(1), Body: strings.NewReader("alice's")})
			if err != nil {
				t.Fatal(err)
			}
			_, err = alice.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: aws.String(defaultBucket), Key: key, UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}},
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

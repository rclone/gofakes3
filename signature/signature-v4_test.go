package signature_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/rclone/gofakes3/signature"
)

//nolint:all
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	serviceS3       = "s3"
	SlashSeparator  = "/"
	stype           = serviceS3
)

func RandString(n int) string {
	src := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, (n+1)/2)

	if _, err := src.Read(b); err != nil {
		panic(err)
	}

	return hex.EncodeToString(b)[:n]
}

func TestSignatureMatch(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name           string
		useQueryString bool
	}{
		{
			name:           "Header-based Authentication",
			useQueryString: false,
		},
		{
			name:           "Query-based Authentication",
			useQueryString: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			Body := bytes.NewReader(nil)
			ak := RandString(32)
			sk := RandString(64)
			region := RandString(16)
			bodyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

			creds := aws.Credentials{
				AccessKeyID:     ak,
				SecretAccessKey: sk,
			}
			signature.ReloadKeys(map[string]string{ak: sk})
			signer := v4signer.NewSigner()

			req, err := http.NewRequest(http.MethodPost, "https://s3-endpoint.example.com/bin", Body)
			if err != nil {
				t.Error(err)
			}

			if tc.useQueryString {
				// For query-based authentication
				req.URL.RawQuery = url.Values{
					"X-Amz-Algorithm":     []string{signV4Algorithm},
					"X-Amz-Credential":    []string{fmt.Sprintf("%s/%s/%s/%s/aws4_request", ak, time.Now().Format(yyyymmdd), region, serviceS3)},
					"X-Amz-Date":          []string{time.Now().Format(iso8601Format)},
					"X-Amz-Expires":       []string{"900"},
					"X-Amz-SignedHeaders": []string{"host"},
				}.Encode()
				err = signer.SignHTTP(ctx, creds, req, bodyHash, serviceS3, region, time.Now())
			} else {
				// For header-based authentication
				err = signer.SignHTTP(ctx, creds, req, bodyHash, serviceS3, region, time.Now())
			}

			if err != nil {
				t.Error(err)
			}

			if result := signature.V4SignVerify(req); result != signature.ErrNone {
				t.Errorf("invalid result: expect none but got %+v", signature.GetAPIError(result))
			}
		})
	}
}

func TestUnsignedPayload(t *testing.T) {
	ctx := context.Background()
	Body := bytes.NewReader([]byte("test data"))

	ak := RandString(32)
	sk := RandString(64)
	region := RandString(16)

	credentials := aws.Credentials{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
	}
	signature.ReloadKeys(map[string]string{ak: sk})
	signer := v4signer.NewSigner()

	req, err := http.NewRequest(http.MethodPost, "https://s3-endpoint.example.com/bin", Body)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

	err = signer.SignHTTP(ctx, credentials, req, unsignedPayload, serviceS3, region, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if result := signature.V4SignVerify(req); result != signature.ErrNone {
		t.Errorf("invalid result for unsigned payload: expect none but got %+v", signature.GetAPIError(result))
	}
}

func TestV4SignVerifyWithSecret(t *testing.T) {
	ctx := context.Background()

	const (
		ak     = "AKIAWITHSECRETTESTKEY"
		sk     = "with-secret-test-secret-key"
		region = "us-east-1"
	)

	credentials := aws.Credentials{
		AccessKeyID:     ak,
		SecretAccessKey: sk,
	}
	// The access key is deliberately not registered with ReloadKeys
	signer := v4signer.NewSigner()

	req, err := http.NewRequest(http.MethodGet, "https://s3-endpoint.example.com/bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	err = signer.SignHTTP(ctx, credentials, req, unsignedPayload, serviceS3, region, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if result := signature.V4SignVerifyWithSecret(req, sk); result != signature.ErrNone {
		t.Errorf("expected ErrNone with correct secret but got %+v", signature.GetAPIError(result))
	}
	if result := signature.V4SignVerifyWithSecret(req, sk+"x"); result == signature.ErrNone {
		t.Errorf("expected an error with wrong secret but got ErrNone")
	} else if code := signature.GetAPIError(result).Code; code != "SignatureDoesNotMatch" {
		t.Errorf("expected SignatureDoesNotMatch with wrong secret but got %q", code)
	}
	if result := signature.V4SignVerifyWithSecret(req, ""); result == signature.ErrNone {
		t.Errorf("expected an error with empty secret but got ErrNone")
	}
	// The unregistered key must still be refused by the key store path
	if result := signature.V4SignVerify(req); result == signature.ErrNone {
		t.Errorf("expected an error from V4SignVerify for an unregistered key but got ErrNone")
	}
}

func TestCheckExpiration(t *testing.T) {
	ctx := context.Background()
	originalTimeNow := signature.TimeNow
	defer func() { signature.TimeNow = originalTimeNow }()

	testCases := []struct {
		name           string
		useQueryString bool
		expiresIn      string
		timeDelta      time.Duration
		expectedError  bool
	}{
		{
			name:           "Valid Header-based Authentication (Default 15min)",
			useQueryString: false,
			expiresIn:      "",
			timeDelta:      14 * time.Minute,
			expectedError:  false,
		},
		{
			name:           "Expired Header-based Authentication (Default 15min)",
			useQueryString: false,
			expiresIn:      "",
			timeDelta:      16 * time.Minute,
			expectedError:  true,
		},
		{
			name:           "Valid Query-based Authentication (30min)",
			useQueryString: true,
			expiresIn:      "1800", // 30 minutes
			timeDelta:      29 * time.Minute,
			expectedError:  false,
		},
		{
			name:           "Expired Query-based Authentication (30min)",
			useQueryString: true,
			expiresIn:      "1800", // 30 minutes
			timeDelta:      31 * time.Minute,
			expectedError:  true,
		},
		{
			name:           "Valid Query-based Authentication (5min)",
			useQueryString: true,
			expiresIn:      "300", // 5 minutes
			timeDelta:      4 * time.Minute,
			expectedError:  false,
		},
		{
			name:           "Expired Query-based Authentication (5min)",
			useQueryString: true,
			expiresIn:      "300", // 5 minutes
			timeDelta:      6 * time.Minute,
			expectedError:  true,
		},
		{
			name:           "Malformed Expires",
			useQueryString: true,
			expiresIn:      "not-a-number",
			timeDelta:      0,
			expectedError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			Body := bytes.NewReader(nil)
			bodyHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
			ak := RandString(32)
			sk := RandString(64)
			region := RandString(16)
			creds := aws.Credentials{
				AccessKeyID:     ak,
				SecretAccessKey: sk,
			}

			signature.ReloadKeys(map[string]string{ak: sk})
			signer := v4signer.NewSigner()

			req, err := http.NewRequest(http.MethodPost, "https://s3-endpoint.example.com/bin", Body)
			if err != nil {
				t.Fatal(err)
			}

			now := time.Now()
			signature.TimeNow = func() time.Time { return now }

			if tc.useQueryString {
				// For query-based authentication
				req.URL.RawQuery = url.Values{
					"X-Amz-Algorithm":     []string{signV4Algorithm},
					"X-Amz-Credential":    []string{fmt.Sprintf("%s/%s/%s/%s/aws4_request", ak, now.Format(yyyymmdd), region, serviceS3)},
					"X-Amz-Date":          []string{now.Format(iso8601Format)},
					"X-Amz-Expires":       []string{tc.expiresIn},
					"X-Amz-SignedHeaders": []string{"host"},
				}.Encode()
			} else {
				// For header-based authentication
				req.Header.Set("X-Amz-Date", now.Format(iso8601Format))
			}

			err = signer.SignHTTP(ctx, creds, req, bodyHash, serviceS3, region, now)
			if err != nil {
				t.Fatal(err)
			}

			// Mock time passing
			signature.TimeNow = func() time.Time { return now.Add(tc.timeDelta) }

			result := signature.V4SignVerify(req)
			if result == signature.ErrNone && tc.expectedError {
				t.Errorf("invalid result: expected error but got no error")
			}
			if result != signature.ErrNone && !tc.expectedError {
				t.Errorf("invalid result: didn't expect error but got error")
			}
		})
	}
}

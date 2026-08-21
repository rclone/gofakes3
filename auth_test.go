package gofakes3_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/rclone/gofakes3"
)

// signedGet makes a GET / request to url signed with the credentials given
// and returns the status code.
func signedGet(t *testing.T, url, accessKey, secretKey string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	err = v4.NewSigner().SignHTTP(context.Background(), creds, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// TestAuthKeysPerInstance checks that the keys added to one GoFakeS3
// instance are not accepted by another instance in the same process.
func TestAuthKeysPerInstance(t *testing.T) {
	tsA := newTestServer(t, withFakerOptions(gofakes3.WithV4Auth(map[string]string{"KEYA": "SECRETA"})))
	defer tsA.Close()
	tsB := newTestServer(t, withFakerOptions(gofakes3.WithV4Auth(map[string]string{"KEYB": "SECRETB"})))
	defer tsB.Close()

	for _, tc := range []struct {
		name      string
		url       string
		accessKey string
		secretKey string
		want      int
	}{
		{"A with A's key", tsA.server.URL, "KEYA", "SECRETA", http.StatusOK},
		{"B with B's key", tsB.server.URL, "KEYB", "SECRETB", http.StatusOK},
		{"A with B's key", tsA.server.URL, "KEYB", "SECRETB", http.StatusForbidden},
		{"B with A's key", tsB.server.URL, "KEYA", "SECRETA", http.StatusForbidden},
		{"A with wrong secret", tsA.server.URL, "KEYA", "SECRETB", http.StatusForbidden},
		{"A with unknown key", tsA.server.URL, "KEYC", "SECRETC", http.StatusForbidden},
	} {
		if got := signedGet(t, tc.url, tc.accessKey, tc.secretKey); got != tc.want {
			t.Errorf("%s: got status %d, want %d", tc.name, got, tc.want)
		}
	}

	// Keys added and removed later are also per instance
	tsA.AddAuthKeys(map[string]string{"KEYC": "SECRETC"})
	if got := signedGet(t, tsA.server.URL, "KEYC", "SECRETC"); got != http.StatusOK {
		t.Errorf("A with added key: got status %d, want 200", got)
	}
	if got := signedGet(t, tsB.server.URL, "KEYC", "SECRETC"); got != http.StatusForbidden {
		t.Errorf("B with key added to A: got status %d, want 403", got)
	}
	tsA.DelAuthKeys([]string{"KEYC"})
	if got := signedGet(t, tsA.server.URL, "KEYC", "SECRETC"); got != http.StatusForbidden {
		t.Errorf("A with removed key: got status %d, want 403", got)
	}
	// Removing a key from A must not affect B's keys
	if got := signedGet(t, tsB.server.URL, "KEYB", "SECRETB"); got != http.StatusOK {
		t.Errorf("B with B's key after A removed a key: got status %d, want 200", got)
	}
}

package signature

import (
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
)

// presignedRequest returns a PUT request authenticated by a presigned
// URL for secret which signs the host and signedHeaders, and which
// also carries the unsigned extraHeaders.
//
// If canonicalCase is set the signed headers list in the URL uses the
// canonical header case rather than lower case.
func presignedRequest(t *testing.T, secret string, signedHeaders, extraHeaders http.Header, canonicalCase bool) *http.Request {
	t.Helper()
	const region = "us-east-1"
	now := TimeNow().UTC()
	scope := now.Format(yyyymmdd) + "/" + region + "/s3/aws4_request"

	req, err := http.NewRequest("PUT", "http://example.com/dst/target.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	signed := http.Header{"Host": {req.Host}}
	var names []string
	for k, v := range signedHeaders {
		req.Header[k] = v
		signed[k] = v
	}
	for k := range signed {
		if canonicalCase && k != "Host" {
			names = append(names, k)
		} else {
			names = append(names, strings.ToLower(k))
		}
	}
	sort.Strings(names)

	q := url.Values{}
	q.Set(amzAlgorithm, signV4Algorithm)
	q.Set(amzCredential, "AKIDEXAMPLE/"+scope)
	q.Set(amzDate, now.Format(iso8601Format))
	q.Set(amzExpires, "900")
	q.Set(amzSignedHeaders, strings.Join(names, ";"))

	canonicalRequest := getCanonicalRequest(signed, "UNSIGNED-PAYLOAD", q.Encode(), req.URL.Path, req.Method)
	stringToSign := getStringToSign(canonicalRequest, now.Truncate(1e9), scope)
	q.Set(amzSignature, getSignature(getSigningKey(secret, now, region), stringToSign))
	req.URL.RawQuery = q.Encode()

	for k, v := range extraHeaders {
		req.Header[k] = v
	}
	return req
}

// TestV4SignVerifyUnsignedAmzHeader checks that an x-amz-* header
// which is not in the signed headers list is refused, so a presigned
// PUT URL can't be turned into a copy from any object by adding an
// unsigned x-amz-copy-source header.
func TestV4SignVerifyUnsignedAmzHeader(t *testing.T) {
	const secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	copySource := http.Header{"X-Amz-Copy-Source": {"/src/secret.txt"}}

	// The presigned URL verifies as intended
	req := presignedRequest(t, secret, nil, nil, false)
	if code := V4SignVerifyWithSecret(req, secret); code != ErrNone {
		t.Errorf("want ErrNone, got %v", GetAPIError(code).Code)
	}
	// but not with the wrong secret
	if code := V4SignVerifyWithSecret(req, secret+"x"); code != errSignatureDoesNotMatch {
		t.Errorf("want SignatureDoesNotMatch, got %v", GetAPIError(code).Code)
	}

	// An unsigned x-amz-* header is refused
	req = presignedRequest(t, secret, nil, copySource, false)
	code := V4SignVerifyWithSecret(req, secret)
	if code != errUnsignedHeaders {
		t.Errorf("want errUnsignedHeaders, got %v", GetAPIError(code).Code)
	}
	if apiErr := GetAPIError(code); apiErr.Code != "AccessDenied" || apiErr.HTTPStatusCode != http.StatusForbidden {
		t.Errorf("want 403 AccessDenied, got %d %s", apiErr.HTTPStatusCode, apiErr.Code)
	}

	// Whatever its case
	req = presignedRequest(t, secret, nil, http.Header{"x-AMZ-copy-source": {"/src/secret.txt"}}, false)
	if code := V4SignVerifyWithSecret(req, secret); code != errUnsignedHeaders {
		t.Errorf("want errUnsignedHeaders, got %v", GetAPIError(code).Code)
	}

	// Whereas a signed x-amz-* header is allowed
	req = presignedRequest(t, secret, copySource, nil, false)
	if code := V4SignVerifyWithSecret(req, secret); code != ErrNone {
		t.Errorf("want ErrNone, got %v", GetAPIError(code).Code)
	}

	// Even if the signed headers list is not in lower case
	req = presignedRequest(t, secret, copySource, nil, true)
	if got := req.URL.Query().Get(amzSignedHeaders); got != "X-Amz-Copy-Source;host" {
		t.Fatalf("test setup: signed headers list is %q", got)
	}
	if code := V4SignVerifyWithSecret(req, secret); code != ErrNone {
		t.Errorf("want ErrNone, got %v", GetAPIError(code).Code)
	}

	// Non x-amz-* headers need not be signed
	req = presignedRequest(t, secret, nil, http.Header{"Content-Type": {"text/plain"}}, false)
	if code := V4SignVerifyWithSecret(req, secret); code != ErrNone {
		t.Errorf("want ErrNone, got %v", GetAPIError(code).Code)
	}
}

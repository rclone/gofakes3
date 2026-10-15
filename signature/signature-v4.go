package signature

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	// TimeNow is a variable that holds the function to get the current time
	TimeNow = time.Now
)

// ref: https://github.com/minio/minio/cmd/auth-handler.go

const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
	serviceS3       = "s3"
	slashSeparator  = "/"
	stype           = serviceS3

	headerAuth       = "Authorization"
	headerDate       = "Date"
	amzContentSha256 = "X-Amz-Content-Sha256"
	amzDate          = "X-Amz-Date"
	amzAlgorithm     = "X-Amz-Algorithm"
	amzCredential    = "X-Amz-Credential"
	amzSignedHeaders = "X-Amz-SignedHeaders"
	amzSignature     = "X-Amz-Signature"
	amzExpires       = "X-Amz-Expires"

	// maxPresignExpires is the longest a presigned URL can be valid
	// for, as on AWS.
	maxPresignExpires = 7 * 24 * time.Hour

	// defaultPresignExpires is how long a presigned URL without an
	// X-Amz-Expires parameter is valid for.
	defaultPresignExpires = 15 * time.Minute

	// maxSkew is the furthest the time a request was signed may be
	// from the current time, as on AWS.
	maxSkew = 15 * time.Minute
)

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// compareSignatureV4 returns true if and only if both signatures
// are equal. The signatures are expected to be HEX encoded strings
// according to the AWS S3 signature V4 spec.
func compareSignatureV4(sig1, sig2 string) bool {
	// The CTC using []byte(str) works because the hex encoding
	// is unique for a sequence of bytes. See also compareSignatureV2.
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// Trim leading and trailing spaces and replace sequential spaces with one space, following Trimall()
// in http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to one space and return
	return strings.Join(strings.Fields(input), " ")
}

// getCanonicalRequest generate a canonical request of style
//
// canonicalRequest =
//
//	<HTTPMethod>\n
//	<CanonicalURI>\n
//	<CanonicalQueryString>\n
//	<CanonicalHeaders>\n
//	<SignedHeaders>\n
//	<HashedPayload>
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
	rawQuery := strings.ReplaceAll(queryStr, "+", "%20")
	encodedPath := encodePath(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders),
		getSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign += scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign += hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte(stype))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// requestTime returns the time the request was signed.
//
// This is in X-Amz-Date in the query of a presigned request. Otherwise
// it is in the X-Amz-Date header in ISO 8601 format, or if that is
// missing, the Date header in HTTP format.
func requestTime(r *http.Request, presigned bool) (t time.Time, Err ErrorCode) {
	var date string
	var parse func(string) (time.Time, error)
	switch {
	case presigned:
		date = r.URL.Query().Get(amzDate)
	case r.Header.Get(amzDate) != "":
		date = r.Header.Get(amzDate)
	default:
		date = r.Header.Get(headerDate)
		parse = http.ParseTime
	}
	if date == "" {
		return t, errMissingDateHeader
	}
	t, err := time.Parse(iso8601Format, date)
	if err != nil && parse != nil {
		t, err = parse(date)
	}
	if err != nil {
		return t, errMalformedDate
	}
	return t, ErrNone
}

// V4SignVerify - Verify authorization header with calculated header in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
//
// The secret key is looked up from the keys registered with StoreKeys
// using the access key ID in the request.
//
// returns ErrNone if signature matches.
func V4SignVerify(r *http.Request) ErrorCode {
	return v4SignVerify(r, func(accessKey string) (string, ErrorCode) {
		cred, _, err := checkKeyValid(r, accessKey)
		return cred.SecretKey, err
	})
}

// V4SignVerifyWithSecret verifies the request's signature against
// secretKey regardless of the access key ID in the request and
// without consulting the keys registered with StoreKeys.
//
// This is for callers which look up the secret for the access key ID
// themselves (eg per request from an external source) and want the
// secret kept out of the key store.
//
// returns ErrNone if signature matches.
func V4SignVerifyWithSecret(r *http.Request, secretKey string) ErrorCode {
	return v4SignVerify(r, func(string) (string, ErrorCode) {
		return secretKey, ErrNone
	})
}

// V4SignVerifyWithLookup verifies the request's signature against the
// secret returned by lookup for the access key ID in the request,
// without consulting the keys registered with StoreKeys.
//
// lookup returns the secret for the access key ID and true, or false
// if the access key ID is unknown, in which case the request is
// refused with InvalidAccessKeyId.
//
// returns ErrNone if signature matches.
func V4SignVerifyWithLookup(r *http.Request, lookup func(accessKey string) (secretKey string, ok bool)) ErrorCode {
	return v4SignVerify(r, func(accessKey string) (string, ErrorCode) {
		secretKey, ok := lookup(accessKey)
		if !ok {
			return "", errInvalidAccessKeyID
		}
		return secretKey, ErrNone
	})
}

// v4SignVerify verifies the request's signature using getSecret to
// find the secret key for the access key ID in the request.
func v4SignVerify(r *http.Request, getSecret func(accessKey string) (string, ErrorCode)) ErrorCode {
	// Copy request.
	req := *r
	queryf := req.URL.Query()
	isUnsignedPayload := req.Header.Get("X-Amz-Content-Sha256") == "UNSIGNED-PAYLOAD"

	// Save authorization header.
	v4Auth := req.Header.Get(headerAuth)

	// If the header is empty but the query string has the signature, then it's QueryString authentication. (https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html)
	presigned := false
	if v4Auth == "" && queryf.Get(amzSignature) != "" {
		// QueryString authentications are always "UNSIGNED-PAYLOAD".
		presigned = true
		isUnsignedPayload = true
		v4Auth = fmt.Sprintf("%s Credential=%s, SignedHeaders=%s, Signature=%s", queryf.Get(amzAlgorithm), queryf.Get(amzCredential), queryf.Get(amzSignedHeaders), queryf.Get(amzSignature))
		if queryf.Get(amzCredential) == "" {
			return errMissingCredTag
		}
	}

	// A request without any credentials is anonymous, which isn't
	// allowed.
	if v4Auth == "" {
		return errAnonymous
	}

	// Parse signature version '4' header.
	signV4Values, Err := ParseSignV4(v4Auth)
	if Err != ErrNone {
		return Err
	}

	secretKey, Err := getSecret(signV4Values.Credential.accessKey)
	if Err != ErrNone {
		return Err
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, ErrCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if ErrCode != ErrNone {
		return ErrCode
	}

	// Find the time the request was signed
	t, Err := requestTime(&req, presigned)
	if Err != ErrNone {
		return Err
	}

	// Check the request is valid now
	now := TimeNow()
	if presigned {
		// A missing X-Amz-Expires is taken to mean the default
		expires := defaultPresignExpires
		if expiresStr := queryf.Get(amzExpires); expiresStr != "" {
			expiresInt, err := strconv.ParseInt(expiresStr, 10, 64)
			if err != nil {
				return errMalformedExpires
			}
			// Check before converting, which could overflow
			if expiresInt > int64(maxPresignExpires/time.Second) {
				return errInvalidExpires
			}
			expires = time.Duration(expiresInt) * time.Second
		}
		if expires < time.Second || expires > maxPresignExpires {
			return errInvalidExpires
		}
		if t.Sub(now) > maxSkew {
			return errRequestNotYetValid
		}
		if now.After(t.Add(expires)) {
			return errExpiredRequest
		}
	} else if skew := now.Sub(t); skew > maxSkew || skew < -maxSkew {
		return errRequestTimeTooSkewed
	}

	// Query string.
	queryf.Del(amzSignature)
	rawquery := queryf.Encode()

	// Get hmac signing key.
	signingKey := getSigningKey(secretKey, signV4Values.Credential.scope.date, signV4Values.Credential.scope.region)

	var newSignature string
	if isUnsignedPayload {
		hashedPayload := "UNSIGNED-PAYLOAD"
		canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, rawquery, req.URL.Path, req.Method)
		stringToSign := getStringToSign(canonicalRequest, t, signV4Values.Credential.getScope())
		newSignature = getSignature(signingKey, stringToSign)
	} else {
		hashedPayload := getContentSha256Cksum(r)
		canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, rawquery, req.URL.Path, req.Method)
		stringToSign := getStringToSign(canonicalRequest, t, signV4Values.Credential.getScope())
		newSignature = getSignature(signingKey, stringToSign)
	}

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return errSignatureDoesNotMatch
	}

	// Return Error none.
	return ErrNone
}

package gofakes3

import (
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"strings"
)

// sseHeaders are the names of the SSE-C headers for an object, or for
// the source of a copy.
type sseHeaders struct {
	algorithm, key, keyMD5 string
}

var (
	sseCHeaders = sseHeaders{
		algorithm: "X-Amz-Server-Side-Encryption-Customer-Algorithm",
		key:       "X-Amz-Server-Side-Encryption-Customer-Key",
		keyMD5:    "X-Amz-Server-Side-Encryption-Customer-Key-Md5",
	}
	copySourceSSECHeaders = sseHeaders{
		algorithm: "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm",
		key:       "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key",
		keyMD5:    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5",
	}
)

// checkSSEC checks the SSE-C (customer provided key) headers named by
// names in h.
//
// It returns InvalidArgument if they are invalid, as S3 does, or
// NotImplemented if they are valid as SSE-C isn't supported.
func checkSSEC(h http.Header, names sseHeaders) error {
	algorithm, key, keyMD5 := h.Get(names.algorithm), h.Get(names.key), h.Get(names.keyMD5)
	if algorithm == "" && key == "" && keyMD5 == "" {
		return nil
	}
	if algorithm == "" {
		return ErrorInvalidArgument(names.algorithm, "", "Requests specifying Server Side Encryption with Customer provided keys must provide a valid encryption algorithm.")
	}
	if algorithm != "AES256" {
		return ErrorInvalidArgument(names.algorithm, algorithm, "The requested encryption algorithm is not valid, must be AES256.")
	}
	rawKey, err := base64.StdEncoding.DecodeString(key)
	if key == "" || err != nil || len(rawKey) != 32 {
		return ErrorInvalidArgument(names.key, "", "The secret key was invalid for the specified algorithm.")
	}
	sum := md5.Sum(rawKey)
	if keyMD5 == "" || keyMD5 != base64.StdEncoding.EncodeToString(sum[:]) {
		return ErrorInvalidArgument(names.keyMD5, "", "The calculated MD5 hash of the key did not match the hash that was provided.")
	}
	return ErrorMessage(ErrNotImplemented, "Server Side Encryption with Customer provided keys is not supported.")
}

// checkWriteEncryption checks the server side encryption and object
// lock headers of a request which writes an object.
//
// SSE-S3 (AES256) is accepted, but it isn't recorded or returned as the
// data isn't encrypted. SSE-KMS and SSE-C aren't supported so are
// refused with NotImplemented, after the headers have been checked as
// S3 does. Object lock isn't supported, so a request asking for it is
// refused as S3 does for a bucket without object lock.
func checkWriteEncryption(h http.Header) error {
	sse := h.Get("X-Amz-Server-Side-Encryption")
	kmsKeyID := h.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id")
	sseC := h.Get(sseCHeaders.algorithm) != "" || h.Get(sseCHeaders.key) != "" || h.Get(sseCHeaders.keyMD5) != ""
	switch {
	case sse != "" && sseC:
		return ErrorInvalidArgument("x-amz-server-side-encryption", sse, "Server Side Encryption with Customer provided key is incompatible with the encryption method specified")
	case kmsKeyID != "" && !strings.HasPrefix(sse, "aws:kms"):
		return ErrorInvalidArgument("x-amz-server-side-encryption-aws-kms-key-id", kmsKeyID, "Server Side Encryption with AWS KMS managed key requires HTTP header x-amz-server-side-encryption : aws:kms")
	}
	switch sse {
	case "", "AES256":
	case "aws:kms", "aws:kms:dsse":
		return ErrorMessage(ErrNotImplemented, "Server Side Encryption with KMS managed keys is not supported.")
	default:
		return ErrorInvalidArgument("x-amz-server-side-encryption", sse, "The encryption method specified is not supported")
	}
	if err := checkSSEC(h, sseCHeaders); err != nil {
		return err
	}
	if err := checkSSEC(h, copySourceSSECHeaders); err != nil {
		return err
	}
	for _, hdr := range []string{"X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date", "X-Amz-Object-Lock-Legal-Hold"} {
		if h.Get(hdr) != "" {
			return ErrorMessage(ErrInvalidRequest, "Bucket is missing Object Lock Configuration")
		}
	}
	return nil
}

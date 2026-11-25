package gofakes3

import (
	"context"
	"errors"
	"io"
)

// ErrMultipartUploadNotSupported is returned by a MultipartBackend implementation
// to signal that GoFakeS3 should fall back to the default in-memory multipart
// implementation for this particular upload. It is not surfaced to the S3 client.
var ErrMultipartUploadNotSupported = errors.New("gofakes3: multipart upload not supported by backend")

// MultipartBackend is an optional interface that a Backend may implement in order
// to handle multipart uploads itself, streaming each part directly into storage
// instead of having GoFakeS3 buffer every part in memory in its default uploader.
//
// When a Backend implements this interface, GoFakeS3 routes the four multipart
// operations (Initiate, UploadPart, Complete, Abort) to it. If the backend
// cannot handle a specific upload (for example, because the underlying storage
// does not support chunked writes for that bucket/object), CreateMultipartUpload
// may return ErrMultipartUploadNotSupported and GoFakeS3 will fall back to the
// default in-memory implementation for just that upload.
//
// All other Backend methods continue to be called normally; only the multipart
// path is affected.
//
// # Concurrency and lifecycle
//
// GoFakeS3 does not serialise multipart operations for a given UploadID, and it
// only removes its own bookkeeping for an upload after the backend has
// acknowledged Complete or Abort (so that a backend error leaves the upload
// available for the client to retry). Two consequences follow that
// implementations must be prepared for:
//
//   - Concurrent CompleteMultipartUpload calls for the same UploadID may both be
//     dispatched to the backend. On a versioned backend this can produce more
//     than one object version from a single logical completion. Make Complete
//     idempotent (or restartable) if this matters to you; see
//     CompleteMultipartUpload.
//
//   - An upload is only released once Complete or Abort succeeds. A client that
//     neither completes nor aborts (or whose Complete keeps failing) leaves the
//     upload tracked indefinitely. Backends that persist part data should expose
//     their own way to reap abandoned uploads rather than relying on GoFakeS3 to
//     do so, and then call GoFakeS3.ForgetMultipartUpload so GoFakeS3 releases
//     its bookkeeping too.
type MultipartBackend interface {
	// CreateMultipartUpload begins a new multipart upload and returns the
	// UploadID that subsequent UploadPart / CompleteMultipartUpload /
	// AbortMultipartUpload calls will reference.
	//
	// UploadIDs must be non-empty and unique within the lifetime of the
	// GoFakeS3 instance: GoFakeS3 tracks in-progress uploads keyed by UploadID
	// and will reject an empty or colliding ID (the backend will be asked to
	// abort the just-created upload). Implementations should use a globally
	// unique generator (for example a UUID) rather than a per-backend counter.
	//
	// Returning ErrMultipartUploadNotSupported signals GoFakeS3 to handle this
	// particular upload with its default in-memory implementation.
	CreateMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string) (UploadID, error)

	// UploadPart writes a single part. body yields at most contentLength
	// bytes. The returned etag is sent back to the S3 client and must be the
	// same value provided to CompleteMultipartUpload in the matching
	// CompletedPart entry.
	//
	// GoFakeS3 does NOT guarantee that body actually contains contentLength
	// bytes: if the client under-delivers (a truncated or aborted request)
	// body reaches EOF early. Unlike the in-memory path, GoFakeS3 does not
	// reject a short part for a streaming backend, so the implementation MUST
	// verify that it read exactly contentLength bytes and return an error
	// (for example ErrIncompleteBody) otherwise; treating a short read as a
	// complete part will silently store a truncated object.
	//
	// If UploadPart returns an error the part is considered failed and the
	// implementation must discard any partial data it has already persisted
	// for that part.
	UploadPart(ctx context.Context, bucket, object string, uploadID UploadID, partNumber int, contentLength int64, body io.Reader) (etag string, err error)

	// CompleteMultipartUpload finalises an upload that was started by
	// CreateMultipartUpload. The input lists the parts (and their etags) to
	// assemble. Implementations should validate the etags and part ordering
	// against the parts they have already received.
	//
	// The returned etag is sent back to the S3 client.
	//
	// If CompleteMultipartUpload returns an error GoFakeS3 leaves the upload
	// in place so the client may retry; implementations should therefore make
	// completion idempotent or restartable.
	CompleteMultipartUpload(ctx context.Context, bucket, object string, uploadID UploadID, input *CompleteMultipartUploadRequest) (versionID VersionID, etag string, err error)

	// AbortMultipartUpload discards an in-progress multipart upload and any
	// parts that have been received.
	//
	// If AbortMultipartUpload returns an error GoFakeS3 leaves the upload in
	// place so the client may retry; implementations should therefore make
	// abort idempotent.
	AbortMultipartUpload(ctx context.Context, bucket, object string, uploadID UploadID) error
}

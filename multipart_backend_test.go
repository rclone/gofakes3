package gofakes3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
	"github.com/rclone/gofakes3/s3mem"
)

// streamingBackend wraps an s3mem.Backend and also implements
// gofakes3.MultipartBackend. Parts are streamed into per-upload byte buffers
// and assembled in-memory on Complete; the focus of the tests is the dispatch
// path, not the storage strategy.
type streamingBackend struct {
	*s3mem.Backend

	// unsupportedKeys: any object key in this set triggers the
	// ErrMultipartUploadNotSupported fallback path in CreateMultipartUpload.
	unsupportedKeys map[string]bool

	// createIDOverride lets a test force CreateMultipartUpload to return a
	// specific UploadID instead of the auto-generated one.
	createIDOverride gofakes3.UploadID

	// emptyID forces CreateMultipartUpload to return an empty UploadID
	emptyID bool

	// versionID, if non-empty, is returned from CompleteMultipartUpload so
	// tests can verify the x-amz-version-id response header.
	versionID gofakes3.VersionID

	// failUploadPartOnce / failCompleteOnce, if set, cause the next call to
	// the corresponding method to return the stored error. The fields are
	// cleared after firing so a subsequent retry succeeds.
	failUploadPartOnce error
	failCompleteOnce   error

	mu      sync.Mutex
	uploads map[gofakes3.UploadID]*streamingUpload
	nextID  int

	createCalls   int
	uploadCalls   int
	completeCalls int
	abortCalls    int
}

type streamingUpload struct {
	bucket, key string
	meta        map[string]string
	mu          sync.Mutex
	parts       map[int][]byte
	etags       map[int]string
}

func newStreamingBackend(inner *s3mem.Backend, unsupported ...string) *streamingBackend {
	set := map[string]bool{}
	for _, k := range unsupported {
		set[k] = true
	}
	return &streamingBackend{
		Backend:         inner,
		unsupportedKeys: set,
		uploads:         map[gofakes3.UploadID]*streamingUpload{},
	}
}

func (b *streamingBackend) CreateMultipartUpload(ctx context.Context, bucket, object string, meta map[string]string) (gofakes3.UploadID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.createCalls++
	if b.unsupportedKeys[object] {
		return "", gofakes3.ErrMultipartUploadNotSupported
	}
	if b.emptyID {
		return "", nil
	}
	var id gofakes3.UploadID
	if b.createIDOverride != "" {
		id = b.createIDOverride
	} else {
		b.nextID++
		id = gofakes3.UploadID(fmt.Sprintf("streaming-%d", b.nextID))
	}
	b.uploads[id] = &streamingUpload{
		bucket: bucket,
		key:    object,
		meta:   meta,
		parts:  map[int][]byte{},
		etags:  map[int]string{},
	}
	return id, nil
}

func (b *streamingBackend) UploadPart(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID, partNumber int, contentLength int64, body io.Reader) (string, error) {
	b.mu.Lock()
	b.uploadCalls++
	if err := b.failUploadPartOnce; err != nil {
		b.failUploadPartOnce = nil
		b.mu.Unlock()
		// Drain the body so the HTTP request can still complete; a real
		// streaming backend that errors mid-write would have already
		// consumed some of the data anyway.
		_, _ = io.Copy(io.Discard, body)
		return "", err
	}
	up, ok := b.uploads[uploadID]
	b.mu.Unlock()
	if !ok {
		return "", gofakes3.ErrNoSuchUpload
	}

	buf, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if int64(len(buf)) != contentLength {
		return "", gofakes3.ErrIncompleteBody
	}
	sum := md5.Sum(buf)
	etag := fmt.Sprintf("%q", hex.EncodeToString(sum[:]))

	up.mu.Lock()
	up.parts[partNumber] = buf
	up.etags[partNumber] = etag
	up.mu.Unlock()
	return etag, nil
}

func (b *streamingBackend) CompleteMultipartUpload(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID, input *gofakes3.CompleteMultipartUploadRequest) (gofakes3.VersionID, string, error) {
	b.mu.Lock()
	b.completeCalls++
	if err := b.failCompleteOnce; err != nil {
		b.failCompleteOnce = nil
		b.mu.Unlock()
		return "", "", err
	}
	up, ok := b.uploads[uploadID]
	if !ok {
		b.mu.Unlock()
		return "", "", gofakes3.ErrNoSuchUpload
	}
	delete(b.uploads, uploadID)
	versionID := b.versionID
	b.mu.Unlock()

	// Build the final body in part-number order.
	partNumbers := make([]int, 0, len(input.Parts))
	for _, p := range input.Parts {
		partNumbers = append(partNumbers, p.PartNumber)
	}
	sort.Ints(partNumbers)
	var assembled bytes.Buffer
	for _, n := range partNumbers {
		assembled.Write(up.parts[n])
	}

	// Persist into the inner s3mem backend so subsequent GetObject works.
	_, err := b.Backend.PutObject(ctx, bucket, object, up.meta, bytes.NewReader(assembled.Bytes()), int64(assembled.Len()))
	if err != nil {
		return "", "", err
	}

	// S3-style multipart etag: hex(md5(concat(part_md5s)))-N
	var concat []byte
	for _, n := range partNumbers {
		raw, _ := hex.DecodeString(strings.Trim(up.etags[n], `"`))
		concat = append(concat, raw...)
	}
	sum := md5.Sum(concat)
	etag := fmt.Sprintf("%q", fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(partNumbers)))
	return versionID, etag, nil
}

func (b *streamingBackend) AbortMultipartUpload(ctx context.Context, bucket, object string, uploadID gofakes3.UploadID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.abortCalls++
	delete(b.uploads, uploadID)
	return nil
}

// uploadMultipart performs a 3-part multipart upload of body via the AWS SDK,
// using a small fixed part size so we exercise multiple UploadPart calls.
func uploadMultipart(t *testing.T, svc *s3.Client, bucket, key string, body []byte, partSize int) {
	t.Helper()
	ctx := context.Background()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	var completed []types.CompletedPart
	for i := 0; i*partSize < len(body); i++ {
		end := (i + 1) * partSize
		if end > len(body) {
			end = len(body)
		}
		partNum := int32(i + 1)
		out, err := svc.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   createOut.UploadId,
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(body[i*partSize : end]),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", partNum, err)
		}
		completed = append(completed, types.CompletedPart{
			ETag:       out.ETag,
			PartNumber: aws.Int32(partNum),
		})
	}

	_, err = svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        createOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
}

func TestMultipartBackend_StreamingPath(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	body := bytes.Repeat([]byte("abcdefghij"), 1024) // 10 KiB
	uploadMultipart(t, ts.s3Client(), defaultBucket, "streamed", body, 4096)

	if be.createCalls != 1 || be.completeCalls != 1 {
		t.Fatalf("expected one create+complete, got create=%d complete=%d", be.createCalls, be.completeCalls)
	}
	if be.uploadCalls != 3 {
		t.Fatalf("expected 3 UploadPart calls, got %d", be.uploadCalls)
	}

	// Object should be retrievable.
	got := ts.backendGetString(defaultBucket, "streamed", nil)
	if got != string(body) {
		t.Fatalf("uploaded body did not round-trip; len got=%d want=%d", len(got), len(body))
	}
}

func TestMultipartBackend_NotSupportedFallback(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner, "fallback-key")
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	body := bytes.Repeat([]byte("0123456789"), 1024)
	uploadMultipart(t, ts.s3Client(), defaultBucket, "fallback-key", body, 4096)

	// CreateMultipartUpload was attempted, but the streaming path was rejected:
	if be.createCalls != 1 {
		t.Fatalf("expected one CreateMultipartUpload call, got %d", be.createCalls)
	}
	if be.uploadCalls != 0 || be.completeCalls != 0 {
		t.Fatalf("streaming path should not have been used; uploadCalls=%d completeCalls=%d", be.uploadCalls, be.completeCalls)
	}

	got := ts.backendGetString(defaultBucket, "fallback-key", nil)
	if got != string(body) {
		t.Fatalf("uploaded body did not round-trip; len got=%d want=%d", len(got), len(body))
	}
}

func TestMultipartBackend_Abort(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("aborted"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	_, err = svc.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("aborted"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("hello")),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	_, err = svc.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(defaultBucket),
		Key:      aws.String("aborted"),
		UploadId: createOut.UploadId,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	if be.abortCalls != 1 {
		t.Fatalf("expected one Abort call, got %d", be.abortCalls)
	}
	if be.completeCalls != 0 {
		t.Fatalf("Complete should not have been called after abort: got %d", be.completeCalls)
	}
}

// TestMultipartBackend_UploadPartError verifies that an error from
// MultipartBackend.UploadPart reaches the S3 client and that the upload
// itself stays usable: a subsequent UploadPart for the same partNumber
// succeeds, and the upload can be completed normally.
func TestMultipartBackend_UploadPartError(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("partfail"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	be.mu.Lock()
	be.failUploadPartOnce = gofakes3.ErrInvalidPart
	be.mu.Unlock()

	part := bytes.Repeat([]byte("z"), 32)
	_, err = svc.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("partfail"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part),
	})
	if err == nil {
		t.Fatal("expected UploadPart error, got nil")
	}

	// Retry the same part — it should now succeed because the upload still
	// exists in both the backend and the uploader.
	retryOut, err := svc.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("partfail"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part),
	})
	if err != nil {
		t.Fatalf("UploadPart retry: %v", err)
	}

	if _, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(defaultBucket),
		Key:      aws.String("partfail"),
		UploadId: createOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: retryOut.ETag, PartNumber: aws.Int32(1)},
		}},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	if be.uploadCalls != 2 {
		t.Fatalf("expected 2 UploadPart calls (one fail, one retry), got %d", be.uploadCalls)
	}
	got := ts.backendGetString(defaultBucket, "partfail", nil)
	if got != string(part) {
		t.Fatalf("body did not round-trip; got len=%d want=%d", len(got), len(part))
	}
}

// TestMultipartBackend_CompleteErrorRetries verifies that an error from
// MultipartBackend.CompleteMultipartUpload leaves the upload intact so the
// client can retry. Before the ordering fix this returned ErrNoSuchUpload
// on retry because GoFakeS3 had already consumed the upload.
func TestMultipartBackend_CompleteErrorRetries(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("completefail"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	part := bytes.Repeat([]byte("y"), 32)
	partOut, err := svc.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("completefail"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(part),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	be.mu.Lock()
	be.failCompleteOnce = gofakes3.ErrInvalidPart
	be.mu.Unlock()

	completed := &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
		{ETag: partOut.ETag, PartNumber: aws.Int32(1)},
	}}
	if _, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("completefail"),
		UploadId:        createOut.UploadId,
		MultipartUpload: completed,
	}); err == nil {
		t.Fatal("expected first CompleteMultipartUpload to fail, got nil")
	}

	// Retry: the upload must still be tracked by both the uploader and the
	// backend, so a second Complete should succeed.
	if _, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("completefail"),
		UploadId:        createOut.UploadId,
		MultipartUpload: completed,
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload retry: %v", err)
	}

	if be.completeCalls != 2 {
		t.Fatalf("expected 2 Complete calls, got %d", be.completeCalls)
	}
	got := ts.backendGetString(defaultBucket, "completefail", nil)
	if got != string(part) {
		t.Fatalf("body did not round-trip; got len=%d want=%d", len(got), len(part))
	}
}

// TestMultipartBackend_ListUploadsAndParts verifies that ListMultipartUploads
// and ListParts return the in-progress streaming upload and the metadata of
// parts registered via AddStreamingPart.
func TestMultipartBackend_ListUploadsAndParts(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("listme"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	defer func() {
		_, _ = svc.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(defaultBucket),
			Key:      aws.String("listme"),
			UploadId: createOut.UploadId,
		})
	}()

	partA := bytes.Repeat([]byte("a"), 100)
	partB := bytes.Repeat([]byte("b"), 200)
	for i, body := range [][]byte{partA, partB} {
		if _, err := svc.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(defaultBucket),
			Key:        aws.String("listme"),
			UploadId:   createOut.UploadId,
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
	}

	listOut, err := svc.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(defaultBucket),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	var found bool
	for _, u := range listOut.Uploads {
		if u.UploadId != nil && *u.UploadId == *createOut.UploadId {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("streaming upload %q not in ListMultipartUploads result: %+v", *createOut.UploadId, listOut.Uploads)
	}

	partsOut, err := svc.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String(defaultBucket),
		Key:      aws.String("listme"),
		UploadId: createOut.UploadId,
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(partsOut.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(partsOut.Parts))
	}
	wantSizes := map[int32]int64{1: int64(len(partA)), 2: int64(len(partB))}
	for _, p := range partsOut.Parts {
		if p.PartNumber == nil || p.Size == nil {
			t.Fatalf("part missing fields: %+v", p)
		}
		if want, ok := wantSizes[*p.PartNumber]; !ok || want != *p.Size {
			t.Fatalf("unexpected part %d size: got %d want %d", *p.PartNumber, *p.Size, want)
		}
	}
}

// TestMultipartBackend_VersionID verifies that a VersionID returned by
// MultipartBackend.CompleteMultipartUpload is surfaced to the S3 client via
// the x-amz-version-id response header.
func TestMultipartBackend_VersionID(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	be.versionID = gofakes3.VersionID("test-version-7")
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("versioned"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	partOut, err := svc.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(defaultBucket),
		Key:        aws.String("versioned"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("hello version")),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	completeOut, err := svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(defaultBucket),
		Key:      aws.String("versioned"),
		UploadId: createOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: partOut.ETag, PartNumber: aws.Int32(1)},
		}},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if completeOut.VersionId == nil || *completeOut.VersionId != "test-version-7" {
		t.Fatalf("expected VersionId %q, got %v", "test-version-7", completeOut.VersionId)
	}
}

// TestMultipartBackend_DuplicateUploadID verifies that a backend handing
// back a colliding UploadID is rejected and the just-created upload is
// aborted so no state leaks in the backend.
func TestMultipartBackend_DuplicateUploadID(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	be.createIDOverride = gofakes3.UploadID("dup-id")
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	if _, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("first"),
	}); err != nil {
		t.Fatalf("first CreateMultipartUpload: %v", err)
	}

	_, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("second"),
	})
	if err == nil {
		t.Fatal("expected CreateMultipartUpload to fail on duplicate UploadID, got nil")
	}

	// gofakes3 should have asked the backend to abort the just-created
	// duplicate upload so its state does not leak. The AWS SDK may retry
	// the failing CreateMultipartUpload, so we expect at least one abort
	// per rejected attempt.
	be.mu.Lock()
	abortCalls := be.abortCalls
	createCalls := be.createCalls
	be.mu.Unlock()
	if abortCalls < 1 || abortCalls != createCalls-1 {
		t.Fatalf("expected one backend Abort per rejected create (create=%d), got %d aborts", createCalls, abortCalls)
	}
}

// TestMultipartBackend_EmptyUploadID verifies that a backend returning an empty
// UploadID (with a nil error) is rejected
func TestMultipartBackend_EmptyUploadID(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	be.emptyID = true
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	if _, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("empty-id"),
	}); err == nil {
		t.Fatal("expected CreateMultipartUpload to fail on empty UploadID, got nil")
	}
}

// TestMultipartBackend_ForgetMultipartUpload verifies that a streaming upload
// the backend has discarded itself can be forgotten, so it no longer shows
// in ListMultipartUploads.
func TestMultipartBackend_ForgetMultipartUpload(t *testing.T) {
	inner := s3mem.New()
	be := newStreamingBackend(inner)
	ts := newTestServer(t, withBackend(be))
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	createOut, err := svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("expired"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := gofakes3.UploadID(*createOut.UploadId)

	// The backend discards the upload itself, for example on expiry.
	if err := be.AbortMultipartUpload(ctx, defaultBucket, "expired", uploadID); err != nil {
		t.Fatalf("backend AbortMultipartUpload: %v", err)
	}
	if err := ts.ForgetMultipartUpload(ctx, defaultBucket, "expired", uploadID); err != nil {
		t.Fatalf("ForgetMultipartUpload: %v", err)
	}

	listOut, err := svc.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(defaultBucket),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(listOut.Uploads) != 0 {
		t.Fatalf("forgotten upload still listed: %+v", listOut.Uploads)
	}

	if err := ts.ForgetMultipartUpload(ctx, defaultBucket, "expired", uploadID); !gofakes3.HasErrorCode(err, gofakes3.ErrNoSuchUpload) {
		t.Fatalf("expected NoSuchUpload forgetting twice, got %v", err)
	}
}

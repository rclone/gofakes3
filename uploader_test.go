package gofakes3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rclone/gofakes3"
)

func TestMultipartUpload(t *testing.T) {
	const size = defaultUploadPartSize

	for _, tc := range []struct {
		parts int64 // number of parts
		size  int64 // size of all parts but last
		last  int64 // size of last part; if 0, size is used
	}{
		{parts: 1, size: size, last: 0},
		{parts: 10, size: size, last: 1},
		{parts: 2, size: 20 * 1024 * 1024, last: (20 * 1024 * 1024) - 1},

		// FIXME: Unfortunately, larger tests are too slow to be practical on
		// every run; should be skipped by default and enabled with a flag
		// later:
		// {parts: 100, size: 10 * 1024 * 1024, last: 1},
	} {
		t.Run("", func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			var size int64
			if tc.last == 0 {
				size = tc.parts * tc.size
			} else {
				size = (tc.parts-1)*tc.size + tc.last
			}

			body := randomFileBody(size)
			ts.assertMultipartUpload(defaultBucket, "uploadtest", body, nil)
		})
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("obj/1")})

	ts.assertAbortMultipartUpload(defaultBucket, "obj", "1")
}

func TestListMultipartUploadsWithTheSameObjectKey(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.createMultipartUpload(defaultBucket, "obj", nil)
	ts.createMultipartUpload(defaultBucket, "obj", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("obj/1", "obj/2", "obj/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 1, Uploads: strs("obj/1")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 2, Uploads: strs("obj/1", "obj/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "obj/2", Limit: 1, Uploads: strs("obj/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "obj/2", Limit: 2, Uploads: strs("obj/2", "obj/3")})
}

func TestListMultipartUploadsWithDifferentObjectKeys(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "foo", nil)
	ts.createMultipartUpload(defaultBucket, "bar", nil)
	ts.createMultipartUpload(defaultBucket, "baz", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("bar/2", "baz/3", "foo/1")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 1, Uploads: strs("bar/2")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit: 2, Uploads: strs("bar/2", "baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "baz/3", Limit: 1, Uploads: strs("baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Marker: "baz/3", Limit: 2, Uploads: strs("baz/3", "foo/1")})
}

func TestListMultipartUploadsPrefix(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ts.createMultipartUpload(defaultBucket, "foo/bar", nil)
	ts.createMultipartUpload(defaultBucket, "foo/bar", nil)
	ts.createMultipartUpload(defaultBucket, "foo/baz", nil)
	ts.createMultipartUpload(defaultBucket, "foo/nested/yep", nil)
	ts.createMultipartUpload(defaultBucket, "food/bar", nil)
	ts.createMultipartUpload(defaultBucket, "food/baz", nil)
	ts.createMultipartUpload(defaultBucket, "yep/qux", nil)

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("/"),
		Prefixes: strs("foo/", "food/", "yep/")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("fo"),
		Prefixes: strs("foo/", "food/")})

	// No prefix gives you everything:
	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Uploads: strs("foo/bar/1", "foo/bar/2", "foo/baz/3", "foo/nested/yep/4", "food/bar/5", "food/baz/6", "yep/qux/7")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{
		Limit:   3,
		Uploads: strs("foo/bar/1", "foo/bar/2", "foo/baz/3")})

	ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("foo/"),
		Prefixes: strs("foo/nested/"),
		Uploads:  strs("foo/bar/1", "foo/bar/2", "foo/baz/3")})

	// FIXME: This requires further investigation. How does AWS treat the combination
	// of commonprefixes and a limit?
	// ts.assertListMultipartUploads(defaultBucket, listUploadsOpts{Prefix: prefixFile("foo/"),
	//     Limit:    2,
	//     Prefixes: strs("foo/nested/"),
	//     Uploads:  strs("foo/bar/1", "foo/bar/2")})
}

func TestListMultipartUploadParts(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	id := ts.createMultipartUpload(defaultBucket, "foo", nil)

	parts := []types.CompletedPart{
		ts.uploadPart(defaultBucket, "foo", id, 1, []byte("abc")),
		ts.uploadPart(defaultBucket, "foo", id, 2, []byte("def")),
		ts.uploadPart(defaultBucket, "foo", id, 3, []byte("ghi")),
	}

	ts.assertListUploadParts(defaultBucket, "foo", id,
		listUploadPartsOpts{}.withCompletedParts(parts...))

	ts.assertCompleteUpload(defaultBucket, "foo", id, parts, []byte("abcdefghi"))

	// No parts should be returned after the upload is completed:
	ts.assertListUploadPartsFails(gofakes3.ErrNoSuchUpload, defaultBucket, "foo", id, listUploadPartsOpts{})
}

// TestListMultipartUploadPartsPagination checks that paginating through the
// parts of a multipart upload with a small MaxParts returns every part exactly
// once, with the correct PartNumber, and that the marker advances rather than
// looping. See https://github.com/rclone/rclone/issues/9460.
func TestListMultipartUploadPartsPagination(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	svc := ts.s3Client()

	const object = "foo"
	id := ts.createMultipartUpload(defaultBucket, object, nil)

	// Upload parts 1..numParts with distinct bodies so each ETag is unique.
	const numParts = 7
	wantETag := map[int32]string{}
	for i := int32(1); i <= numParts; i++ {
		part := ts.uploadPart(defaultBucket, object, id, i, []byte{'p', byte('0' + i)})
		wantETag[i] = *part.ETag
	}

	// Page through the parts with MaxParts strictly less than numParts so
	// more than one page is required.
	const maxParts = 3
	seen := map[int32]bool{}
	var marker *string
	for page := 1; ; page++ {
		if page > numParts+1 {
			t.Fatalf("pagination did not terminate after %d pages: marker stuck", page)
		}

		rs, err := svc.ListParts(ctx, &s3.ListPartsInput{
			Bucket:           aws.String(defaultBucket),
			Key:              aws.String(object),
			UploadId:         aws.String(id),
			MaxParts:         aws.Int32(maxParts),
			PartNumberMarker: marker,
		})
		ts.OK(err)

		for _, p := range rs.Parts {
			pn := *p.PartNumber
			if pn < 1 || pn > numParts {
				t.Fatalf("page %d: got out-of-range PartNumber %d", page, pn)
			}
			if seen[pn] {
				t.Fatalf("page %d: duplicate PartNumber %d returned (pagination loop)", page, pn)
			}
			seen[pn] = true
			if got := *p.ETag; got != wantETag[pn] {
				t.Fatalf("page %d: PartNumber %d ETag mismatch: got %s want %s", page, pn, got, wantETag[pn])
			}
		}

		if rs.IsTruncated == nil || !*rs.IsTruncated {
			break
		}
		marker = rs.NextPartNumberMarker
	}

	for i := int32(1); i <= numParts; i++ {
		if !seen[i] {
			t.Fatalf("part %d was never returned by pagination", i)
		}
	}
}

// TestCompleteMultipartUploadAgain checks completing an upload again,
// as a client which didn't get the response would, succeeds with the
// same result, but completing an aborted upload doesn't.
func TestCompleteMultipartUploadAgain(t *testing.T) {
	ctx := context.Background()
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	id := ts.createMultipartUpload(defaultBucket, "foo", nil)
	parts := []types.CompletedPart{
		ts.uploadPart(defaultBucket, "foo", id, 1, []byte("abc")),
		ts.uploadPart(defaultBucket, "foo", id, 2, []byte("def")),
	}
	in := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("foo"),
		UploadId:        aws.String(id),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
	first, err := svc.CompleteMultipartUpload(ctx, in)
	ts.OK(err)
	again, err := svc.CompleteMultipartUpload(ctx, in)
	ts.OK(err)
	if *first.ETag != *again.ETag {
		t.Errorf("ETag: first %s again %s", *first.ETag, *again.ETag)
	}
	ts.assertObject(defaultBucket, "foo", nil, "abcdef")

	// The upload is gone though
	ts.assertListUploadPartsFails(gofakes3.ErrNoSuchUpload, defaultBucket, "foo", id, listUploadPartsOpts{})

	// The result is only for the same key
	in.Key = aws.String("bar")
	_, err = svc.CompleteMultipartUpload(ctx, in)
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchUpload) {
		t.Fatal("expected NoSuchUpload for a different key, got", err)
	}

	// An aborted upload can't be completed
	id = ts.createMultipartUpload(defaultBucket, "foo", nil)
	part := ts.uploadPart(defaultBucket, "foo", id, 1, []byte("abc"))
	ts.assertAbortMultipartUpload(defaultBucket, "foo", gofakes3.UploadID(id))
	_, err = svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(defaultBucket),
		Key:             aws.String("foo"),
		UploadId:        aws.String(id),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}},
	})
	if !s3HasErrorCode(err, gofakes3.ErrNoSuchUpload) {
		t.Fatal("expected NoSuchUpload after abort, got", err)
	}
}

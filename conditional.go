package gofakes3

import (
	"context"
	"encoding/hex"
	"net/http"
	"strings"
	"time"
)

// etagMatches reports whether the If-Match or If-None-Match header
// value list, a comma separated list of entity tags or "*", matches
// etag.
func etagMatches(list, etag string) bool {
	etag = strings.Trim(etag, `"`)
	for _, tag := range strings.Split(list, ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" {
			return true
		}
		tag = strings.TrimPrefix(tag, "W/")
		if strings.Trim(tag, `"`) == etag {
			return true
		}
	}
	return false
}

// modifiedAfter reports whether lastModified is after the HTTP date in
// value. S3 times have a resolution of a second. ok is false if value
// isn't a valid date or lastModified isn't known, in which case the
// condition should be ignored.
func modifiedAfter(lastModified time.Time, value string) (after bool, ok bool) {
	t, err := http.ParseTime(value)
	if err != nil || lastModified.IsZero() {
		return false, false
	}
	return lastModified.Truncate(time.Second).After(t), true
}

// conditionalHeaders are the names of the headers giving the
// conditions on a request.
type conditionalHeaders struct {
	ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince string
}

var (
	// getConditions are the conditions on reading an object
	getConditions = conditionalHeaders{
		ifMatch:           "If-Match",
		ifNoneMatch:       "If-None-Match",
		ifModifiedSince:   "If-Modified-Since",
		ifUnmodifiedSince: "If-Unmodified-Since",
	}

	// copySourceConditions are the conditions on the source of a copy
	copySourceConditions = conditionalHeaders{
		ifMatch:           "X-Amz-Copy-Source-If-Match",
		ifNoneMatch:       "X-Amz-Copy-Source-If-None-Match",
		ifModifiedSince:   "X-Amz-Copy-Source-If-Modified-Since",
		ifUnmodifiedSince: "X-Amz-Copy-Source-If-Unmodified-Since",
	}
)

// checkConditions checks the conditions in the request headers h,
// named by names, against an object with etag and lastModified.
//
// It returns ErrPreconditionFailed if If-Match or If-Unmodified-Since
// fail, and notModified if If-None-Match or If-Modified-Since fail.
//
// As on S3, If-Unmodified-Since is ignored if If-Match is given, and
// If-Modified-Since is ignored if If-None-Match is given.
func checkConditions(h http.Header, names conditionalHeaders, etag string, lastModified time.Time, notModified error) error {
	if v := h.Get(names.ifMatch); v != "" {
		if !etagMatches(v, etag) {
			return ErrPreconditionFailed
		}
	} else if v := h.Get(names.ifUnmodifiedSince); v != "" {
		if after, ok := modifiedAfter(lastModified, v); ok && after {
			return ErrPreconditionFailed
		}
	}
	if v := h.Get(names.ifNoneMatch); v != "" {
		if etagMatches(v, etag) {
			return notModified
		}
	} else if v := h.Get(names.ifModifiedSince); v != "" {
		if after, ok := modifiedAfter(lastModified, v); ok && !after {
			return notModified
		}
	}
	return nil
}

// objectLastModified returns the modification time of obj from its
// metadata, or the zero time if it doesn't have one.
func objectLastModified(obj *Object) time.Time {
	t, _ := http.ParseTime(obj.Metadata["Last-Modified"])
	return t
}

// writeConditions are the conditions on writing an object given in a
// request with If-Match or If-None-Match.
type writeConditions struct {
	ifMatch, ifNoneMatch string
}

// writeConditionsKey is the context key for the writeConditions of a
// request.
type writeConditionsKey struct{}

// check checks the conditions against the current object, which
// exists or not and if it exists has etag.
func (c writeConditions) check(exists bool, etag string) error {
	if c.ifMatch != "" {
		if !exists {
			return ErrNoSuchKey
		}
		if !etagMatches(c.ifMatch, etag) {
			return ErrPreconditionFailed
		}
	}
	if c.ifNoneMatch != "" && exists && etagMatches(c.ifNoneMatch, etag) {
		return ErrPreconditionFailed
	}
	return nil
}

// CheckWriteConditions checks the If-Match and If-None-Match
// conditions on writing an object, given in the request with ctx,
// against the object currently stored, which exists or not and if it
// exists has etag.
//
// It returns nil if the conditions are met or there are none,
// ErrPreconditionFailed if they aren't met, or ErrNoSuchKey if
// If-Match was given but the object doesn't exist.
//
// GoFakeS3 checks the conditions before calling PutObject or
// CompleteMultipartUpload, but can't stop the object changing
// afterwards. A backend can call this as it replaces the object, while
// stopping other writes, to make the check atomic.
func CheckWriteConditions(ctx context.Context, exists bool, etag string) error {
	c, ok := ctx.Value(writeConditionsKey{}).(writeConditions)
	if !ok {
		return nil
	}
	return c.check(exists, etag)
}

// HasWriteConditions reports whether the request with ctx has If-Match
// or If-None-Match conditions on writing an object, so whether
// CheckWriteConditions needs calling.
func HasWriteConditions(ctx context.Context) bool {
	_, ok := ctx.Value(writeConditionsKey{}).(writeConditions)
	return ok
}

// checkWriteConditions checks the If-Match and If-None-Match conditions
// in r on writing the object at bucket/object.
//
// If there are conditions it returns r with them in its context for
// the backend to check with CheckWriteConditions.
func (g *GoFakeS3) checkWriteConditions(r *http.Request, bucket, object string) (*http.Request, error) {
	c := writeConditions{
		ifMatch:     r.Header.Get("If-Match"),
		ifNoneMatch: r.Header.Get("If-None-Match"),
	}
	if c == (writeConditions{}) {
		return r, nil
	}
	exists, etag := false, ""
	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err == nil {
		exists, etag = true, hex.EncodeToString(obj.Hash)
		if obj.Contents != nil {
			_ = obj.Contents.Close()
		}
	} else if !HasErrorCode(err, ErrNoSuchKey) {
		return r, err
	}
	if err := c.check(exists, etag); err != nil {
		return r, err
	}
	return r.WithContext(context.WithValue(r.Context(), writeConditionsKey{}, c)), nil
}

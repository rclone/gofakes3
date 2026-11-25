package gofakes3

import (
	"context"
	"encoding/hex"
	"net/http"
	"slices"
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

// WriteConditions contains the validated destination ETags of a request.
type WriteConditions struct{ match, none []string }

// ParseWriteConditions parses If-Match and If-None-Match. It returns nil
// when neither header is present.
func ParseWriteConditions(h http.Header) (*WriteConditions, error) {
	var c WriteConditions
	for _, field := range []struct {
		name string
		tags *[]string
	}{{"If-Match", &c.match}, {"If-None-Match", &c.none}} {
		if values := h.Values(field.name); len(values) > 0 {
			tags := splitEntityTags(strings.Join(values, ","))
			if len(tags) == 0 {
				return nil, ErrInvalidArgument
			}
			*field.tags = tags
		}
	}
	if c.match == nil && c.none == nil {
		return nil, nil
	}
	return &c, nil
}

// NeedsETag reports whether validation requires hashing an existing object.
func (c *WriteConditions) NeedsETag() bool {
	return len(c.match) > 0 && c.match[0] != "*" || len(c.none) > 0 && c.none[0] != "*"
}

// Check validates the conditions against the destination. etag is the quoted
// strong ETag when NeedsETag is true and the destination exists.
func (c *WriteConditions) Check(exists bool, etag string) error {
	if !strings.HasPrefix(etag, `"`) {
		etag = `"` + etag + `"`
	}
	if len(c.match) > 0 {
		if !exists {
			return ErrNoSuchKey
		}
		if c.match[0] != "*" && !slices.Contains(c.match, etag) {
			return ErrPreconditionFailed
		}
	}
	if exists {
		for _, tag := range c.none {
			if tag == "*" || strings.TrimPrefix(tag, "W/") == etag {
				return ErrPreconditionFailed
			}
		}
	}
	return nil
}

// writeConditionsKey is the context key for a request’s write conditions.
type writeConditionsKey struct{}

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
	c, ok := ctx.Value(writeConditionsKey{}).(*WriteConditions)
	if !ok {
		return nil
	}
	return c.Check(exists, etag)
}

// HasWriteConditions reports whether the request with ctx has If-Match
// or If-None-Match conditions on writing an object, so whether
// CheckWriteConditions needs calling.
func HasWriteConditions(ctx context.Context) bool {
	_, ok := ctx.Value(writeConditionsKey{}).(*WriteConditions)
	return ok
}

// checkWriteConditions checks the If-Match and If-None-Match conditions
// in r on writing the object at bucket/object.
//
// If there are conditions it returns r with them in its context for
// the backend to check with CheckWriteConditions.
func (g *GoFakeS3) checkWriteConditions(r *http.Request, bucket, object string) (*http.Request, error) {
	c, err := ParseWriteConditions(r.Header)
	if err != nil || c == nil {
		return r, err
	}
	exists, etag := false, ""
	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err == nil {
		exists, etag = !obj.IsDeleteMarker, hex.EncodeToString(obj.Hash)
		if obj.Contents != nil {
			_ = obj.Contents.Close()
		}
	} else if !HasErrorCode(err, ErrNoSuchKey) {
		return r, err
	}
	if err := c.Check(exists, etag); err != nil {
		return r, err
	}
	return r.WithContext(context.WithValue(r.Context(), writeConditionsKey{}, c)), nil
}

// deleteConditions are the conditions on deleting an object.
type deleteConditions struct {
	etag         string
	lastModified string // an HTTP or ISO 8601 date
	size         *int64
}

// checkDeleteConditions checks the conditions c on deleting the object
// at bucket/object. They are met if the object doesn't exist, as
// deleting it does nothing.
func (g *GoFakeS3) checkDeleteConditions(ctx context.Context, bucket, object string, version VersionID, c deleteConditions) error {
	if c == (deleteConditions{}) {
		return nil
	}
	var obj *Object
	var err error
	if version != "" && g.versioned != nil {
		obj, err = g.versioned.HeadObjectVersion(bucket, object, version)
	} else {
		obj, err = g.storage.HeadObject(ctx, bucket, object)
	}
	if HasErrorCode(err, ErrNoSuchKey) || HasErrorCode(err, ErrNoSuchVersion) {
		return nil
	} else if err != nil {
		return err
	}
	if obj.Contents != nil {
		_ = obj.Contents.Close()
	}
	if c.etag != "" && !etagMatches(c.etag, hex.EncodeToString(obj.Hash)) {
		return ErrPreconditionFailed
	}
	if c.lastModified != "" {
		t, err := http.ParseTime(c.lastModified)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, c.lastModified)
		}
		if err != nil {
			return ErrorInvalidArgument("LastModifiedTime", c.lastModified, "Invalid date")
		}
		if !objectLastModified(obj).Truncate(time.Second).Equal(t.Truncate(time.Second)) {
			return ErrPreconditionFailed
		}
	}
	if c.size != nil && *c.size != obj.Size {
		return ErrPreconditionFailed
	}
	return nil
}

// splitEntityTags accepts quoted lists and single unquoted tags used by S3 clients.
func splitEntityTags(value string) []string {
	value = strings.Trim(value, " \t")
	if value == "*" {
		return []string{"*"}
	}
	if value != "" && value[0] != '"' && !strings.HasPrefix(value, "W/") {
		for i := range len(value) {
			c := value[i]
			if c <= 0x20 || c >= 0x7f || c == '"' || c == ',' || c == '*' {
				return nil
			}
		}
		return []string{`"` + value + `"`}
	}
	var tags []string
	for value != "" {
		end := 0
		if strings.HasPrefix(value, "W/") {
			end = 2
		}
		if len(value) <= end || value[end] != '"' {
			return nil
		}
		end++
		for end < len(value) && value[end] != '"' {
			if value[end] < 0x21 || value[end] == 0x7f {
				return nil
			}
			end++
		}
		if end == len(value) {
			return nil
		}
		end++
		tags = append(tags, value[:end])
		value = strings.TrimLeft(value[end:], " \t")
		if value == "" {
			return tags
		}
		if value[0] != ',' {
			return nil
		}
		value = strings.TrimLeft(value[1:], " \t")
		if value == "" {
			return nil
		}
	}
	return tags
}

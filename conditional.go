package gofakes3

import (
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

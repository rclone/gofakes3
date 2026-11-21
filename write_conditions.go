package gofakes3

import (
	"context"
	"net/http"
	"slices"
	"strings"
)

// ConditionalWriteBackend enforces destination conditions while publishing
// PUT, COPY and multipart uploads. A backend must opt in before the server
// will pass it a conditional request.
type ConditionalWriteBackend interface{ SupportsWriteConditions() bool }

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

type parsedWriteConditionsKey struct{}

// WithContext attaches the conditions to a backend request.
func (c *WriteConditions) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, parsedWriteConditionsKey{}, c)
}

// WriteConditionsFromContext returns the destination conditions, if present.
func WriteConditionsFromContext(ctx context.Context) *WriteConditions {
	c, _ := ctx.Value(parsedWriteConditionsKey{}).(*WriteConditions)
	return c
}

// NeedsETag reports whether validation requires hashing an existing object.
func (c *WriteConditions) NeedsETag() bool {
	return len(c.match) > 0 && c.match[0] != "*" || len(c.none) > 0 && c.none[0] != "*"
}

// Check validates the conditions against the destination. etag is the quoted
// strong ETag when NeedsETag is true and the destination exists.
func (c *WriteConditions) Check(exists bool, etag string) error {
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

func conditionalWriteRequest(r *http.Request, backend ConditionalWriteBackend) (*http.Request, error) {
	c, err := ParseWriteConditions(r.Header)
	if err != nil || c == nil {
		return r, err
	}
	if !backend.SupportsWriteConditions() {
		return r, ErrNotImplemented
	}
	return r.WithContext(c.WithContext(r.Context())), nil
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

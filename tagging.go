package gofakes3

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"unicode/utf8"

	xml "github.com/minio/xxml"
)

// TaggingMetadataKey is the metadata key under which an object's tags
// are stored, URL query encoded as in the x-amz-tagging header.
const TaggingMetadataKey = "X-Amz-Tagging"

// Limits on object tags, as on S3.
const (
	maxTags           = 10
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

// ObjectTaggingBackend is an optional interface for a Backend which can
// change the tags of an existing object.
//
// An object's tags are stored in its metadata under TaggingMetadataKey
// when it is created, so any Backend stores them and they can be read.
// Without this interface they can't be changed afterwards.
type ObjectTaggingBackend interface {
	// SetObjectTagging sets the TaggingMetadataKey entry of the
	// metadata of the object to tagging, or removes it if tagging is
	// empty, leaving the rest of the object unchanged.
	//
	// It returns ErrNoSuchKey if the object doesn't exist.
	SetObjectTagging(ctx context.Context, bucketName, objectName string, tagging string) error
}

// Tag is a key and value tagging an object.
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// Tagging is the body of PutObjectTagging and response to
// GetObjectTagging.
type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  []Tag    `xml:"TagSet>Tag"`
}

// encodeTags checks tags are valid and returns them URL query encoded,
// in a canonical order.
func encodeTags(tags []Tag) (string, error) {
	if len(tags) > maxTags {
		return "", ErrorMessage(ErrInvalidTag, "Object tags cannot be greater than 10")
	}
	values := url.Values{}
	for _, tag := range tags {
		if tag.Key == "" || utf8.RuneCountInString(tag.Key) > maxTagKeyLength {
			return "", ErrorMessage(ErrInvalidTag, "The TagKey you have provided is invalid")
		}
		if utf8.RuneCountInString(tag.Value) > maxTagValueLength {
			return "", ErrorMessage(ErrInvalidTag, "The TagValue you have provided is invalid")
		}
		if values.Has(tag.Key) {
			return "", ErrorMessage(ErrInvalidTag, "Cannot provide multiple Tags with the same key")
		}
		values.Set(tag.Key, tag.Value)
	}
	return values.Encode(), nil
}

// parseTaggingHeader checks the URL query encoded tags in the
// x-amz-tagging header value v and returns them in canonical form.
func parseTaggingHeader(v string) (string, error) {
	values, err := url.ParseQuery(v)
	if err != nil {
		return "", ErrorMessage(ErrInvalidArgument, "The header 'x-amz-tagging' shall be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.")
	}
	tags := make([]Tag, 0, len(values))
	for k, vs := range values {
		if len(vs) > 1 {
			return "", ErrorMessage(ErrInvalidTag, "Cannot provide multiple Tags with the same key")
		}
		tags = append(tags, Tag{Key: k, Value: vs[0]})
	}
	return encodeTags(tags)
}

// decodeTags returns the tags URL query encoded in tagging, sorted by
// key.
func decodeTags(tagging string) []Tag {
	values, _ := url.ParseQuery(tagging)
	tags := make([]Tag, 0, len(values))
	for k, vs := range values {
		tags = append(tags, Tag{Key: k, Value: vs[0]})
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })
	return tags
}

// writeTaggingCount writes the number of tags in metadata meta to the
// x-amz-tagging-count header if there are any.
func writeTaggingCount(w http.ResponseWriter, meta map[string]string) {
	if tagging := meta[TaggingMetadataKey]; tagging != "" {
		w.Header().Set("x-amz-tagging-count", strconv.Itoa(len(decodeTags(tagging))))
	}
}

// routeObjectTagging handles requests for the ?tagging sub-resource of
// an object.
func (g *GoFakeS3) routeObjectTagging(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}
	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err != nil {
		return err
	}
	if obj.Contents != nil {
		_ = obj.Contents.Close()
	}

	switch r.Method {
	case "GET":
		return g.xmlEncoder(w).Encode(Tagging{
			Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
			TagSet: decodeTags(obj.Metadata[TaggingMetadataKey]),
		})
	case "PUT", "DELETE":
		tagger, ok := g.storage.(ObjectTaggingBackend)
		if !ok {
			return ErrNotImplemented
		}
		tagging := ""
		if r.Method == "PUT" {
			var in Tagging
			if err := g.xmlDecodeBody(r, &in, true); err != nil {
				return err
			}
			tagging, err = encodeTags(in.TagSet)
			if err != nil {
				return err
			}
		}
		if err := tagger.SetObjectTagging(r.Context(), bucket, object, tagging); err != nil {
			return err
		}
		if r.Method == "DELETE" {
			w.WriteHeader(http.StatusNoContent)
		}
		return nil
	default:
		return ErrMethodNotAllowed
	}
}

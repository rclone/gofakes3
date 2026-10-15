package gofakes3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xml "github.com/minio/xxml"
	"github.com/rclone/gofakes3/signature"
)

// GoFakeS3 implements HTTP handlers for processing S3 requests and returning
// S3 responses.
//
// Logic is delegated to other components, like Backend or uploader.
type GoFakeS3 struct {
	requestID uint64

	storage   Backend
	versioned VersionedBackend
	multipart MultipartBackend // nil if storage does not implement MultipartBackend

	timeSource              TimeSource
	timeSkew                time.Duration
	metadataSizeLimit       int
	integrityCheck          bool
	failOnUnimplementedPage bool
	hostBucket              bool
	autoBucket              bool
	uploader                *uploader
	uploadOwner             func(ctx context.Context) string // nil if uploads aren't private
	log                     Logger

	// simple v4 signature
	mu         sync.RWMutex // protects vAuthPair map only
	v4AuthPair map[string]string
}

// New creates a new GoFakeS3 using the supplied Backend. Backends are pluggable.
// Several Backend implementations ship with GoFakeS3, which can be found in the
// gofakes3/backends package.
func New(backend Backend, options ...Option) *GoFakeS3 {
	s3 := &GoFakeS3{
		storage:           backend,
		timeSkew:          DefaultSkewLimit,
		metadataSizeLimit: DefaultMetadataSizeLimit,
		integrityCheck:    true,
		uploader:          newUploader(),
		requestID:         0,
	}

	// versioned MUST be set before options as one of the options disables it:
	s3.versioned, _ = backend.(VersionedBackend)
	s3.multipart, _ = backend.(MultipartBackend)

	for _, opt := range options {
		opt(s3)
	}

	if s3.log == nil {
		s3.log = DiscardLog()
	}

	if s3.timeSource == nil {
		s3.timeSource = DefaultTimeSource()
	}

	// Copy the keys given with WithV4Auth so the caller's map isn't
	// shared with this instance
	if len(s3.v4AuthPair) != 0 {
		pairs := s3.v4AuthPair
		s3.v4AuthPair = nil
		s3.AddAuthKeys(pairs)
	}

	return s3
}

func (g *GoFakeS3) nextRequestID() uint64 {
	return atomic.AddUint64(&g.requestID, 1)
}

// Create the AWS S3 API
func (g *GoFakeS3) Server() http.Handler {
	var handler http.Handler = http.HandlerFunc(g.routeBase)

	if g.timeSkew != 0 {
		handler = g.timeSkewMiddleware(handler)
	}

	if g.hostBucket {
		handler = g.hostBucketMiddleware(handler)
	}

	return g.requestIDMiddleware(&withCORS{r: g.authMiddleware(handler), g: g})
}

// requestIDMiddleware gives each response a request ID, which is also
// returned in any error response.
func (g *GoFakeS3) requestIDMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		hdr := w.Header()
		id := fmt.Sprintf("%016X", g.nextRequestID())
		hdr.Set("x-amz-id-2", base64.StdEncoding.EncodeToString([]byte(id+id+id+id))) // x-amz-id-2 is 48 bytes of random stuff
		hdr.Set("x-amz-request-id", id)
		hdr.Set("Server", "AmazonS3")
		handler.ServeHTTP(w, rq)
	})
}

// AddAuthKeys adds access key ID to secret pairs which this instance
// accepts. The keys are private to this instance.
func (g *GoFakeS3) AddAuthKeys(p map[string]string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.v4AuthPair == nil {
		g.v4AuthPair = map[string]string{}
	}
	for k, v := range p {
		g.v4AuthPair[k] = v
	}
}

// DelAuthKeys removes the access key IDs given from those this
// instance accepts.
func (g *GoFakeS3) DelAuthKeys(p []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, v := range p {
		delete(g.v4AuthPair, v)
	}
}

// lookupAuthKey returns the secret for accessKey if this instance
// accepts it.
func (g *GoFakeS3) lookupAuthKey(accessKey string) (secretKey string, ok bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	secretKey, ok = g.v4AuthPair[accessKey]
	return secretKey, ok
}

// authMiddleware refuses requests which aren't signed with one of
// this instance's keys. If no keys have been added all requests are
// allowed.
func (g *GoFakeS3) authMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		g.mu.RLock()
		haveAuth := len(g.v4AuthPair) > 0
		g.mu.RUnlock()
		if haveAuth {
			if result := signature.V4SignVerifyWithLookup(rq, g.lookupAuthKey); result != signature.ErrNone {
				g.log.Print(LogWarn, "Access Denied:", rq.RemoteAddr, "=>", rq.URL)

				resp := signature.GetAPIError(result)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(resp.HTTPStatusCode)
				_, _ = w.Write(signature.EncodeAPIErrorToResponseWithRequestID(resp, w.Header().Get("x-amz-request-id")))
				return
			}
		}

		handler.ServeHTTP(w, rq)
	})
}

func (g *GoFakeS3) timeSkewMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		timeHdr := rq.Header.Get("x-amz-date")

		if timeHdr != "" {
			rqTime, _ := time.Parse("20060102T150405Z", timeHdr)
			at := g.timeSource.Now()
			skew := at.Sub(rqTime)

			if skew < -g.timeSkew || skew > g.timeSkew {
				g.httpError(w, rq, requestTimeTooSkewed(at, g.timeSkew))
				return
			}
		}

		handler.ServeHTTP(w, rq)
	})
}

// hostBucketMiddleware forces the server to use VirtualHost-style bucket URLs:
// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
func (g *GoFakeS3) hostBucketMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		parts := strings.SplitN(rq.Host, ".", 2)
		bucket := parts[0]

		p := rq.URL.Path
		rq.URL.Path = "/" + bucket
		if p != "/" {
			rq.URL.Path += p
		}
		g.log.Print(LogInfo, p, "=>", rq.URL)

		handler.ServeHTTP(w, rq)
	})
}

func (g *GoFakeS3) httpError(w http.ResponseWriter, r *http.Request, err error) {
	resp := ensureErrorResponse(err, w.Header().Get("x-amz-request-id"))
	if resp.ErrorCode() == ErrInternal {
		g.log.Print(LogErr, err)
	}

	status := resp.ErrorCode().Status()
	// HEAD responses and 304 Not Modified have no body
	hasBody := r.Method != http.MethodHead && status != http.StatusNotModified
	if hasBody {
		w.Header().Set("Content-Type", "application/xml")
	}
	w.WriteHeader(status)

	if hasBody {
		if err := g.xmlEncoder(w).Encode(resp); err != nil {
			g.log.Print(LogErr, err)
			return
		}
	}
}

// defaultOwner is the owner of all buckets and objects.
var defaultOwner = UserInfo{
	ID:          "fe7272ea58be830e56fe1663b10fafef",
	DisplayName: "GoFakeS3",
}

// listBuckets lists the buckets, paginated by the max-buckets and
// continuation-token parameters and limited to those starting with
// the prefix parameter.
func (g *GoFakeS3) listBuckets(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	maxBuckets, err := parseClampedInt(q.Get("max-buckets"), MaxBucketsLimit, 1, MaxBucketsLimit)
	if err != nil {
		return ErrorInvalidArgument("max-buckets", q.Get("max-buckets"), "max-buckets must be between 1 and 10000")
	}
	prefix := q.Get("prefix")
	after := ""
	if token := q.Get("continuation-token"); token != "" {
		decoded, err := base64.URLEncoding.DecodeString(token)
		if err != nil {
			return ErrorInvalidArgument("continuation-token", token, "The continuation token provided is incorrect")
		}
		after = string(decoded)
	}

	buckets, err := g.storage.ListBuckets(r.Context())
	if err != nil {
		return err
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Name < buckets[j].Name })

	s := &Storage{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner:  &defaultOwner,
		Prefix: prefix,
	}
	for _, bucket := range buckets {
		if bucket.Name <= after || !strings.HasPrefix(bucket.Name, prefix) {
			continue
		}
		if int64(len(s.Buckets)) == maxBuckets {
			last := s.Buckets[len(s.Buckets)-1].Name
			s.ContinuationToken = base64.URLEncoding.EncodeToString([]byte(last))
			break
		}
		s.Buckets = append(s.Buckets, bucket)
	}

	return g.xmlEncoder(w).Encode(s)
}

// S3 has two versions of this API, both of which are close to identical. We manage that
// jank in here so the Backend doesn't have to with the following tricks:
//
// - Hiding the NextMarker inside the ContinuationToken for V2 calls
// - Masking the Owner in the response for V2 calls
//
// The wrapping response objects are slightly different too, but the list of
// objects is pretty much the same.
//
// - https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
// - https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html
func (g *GoFakeS3) listBucket(bucketName string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "LIST BUCKET")

	if err := g.ensureBucketExists(r, bucketName); err != nil {
		return err
	}

	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketPageFromQuery(q)
	if err != nil {
		return err
	}

	isVersion2 := q.Get("list-type") == "2"
	encodingType := q.Get("encoding-type")
	useUrlEncoding := encodingType == "url"
	if !useUrlEncoding && encodingType != "" {
		return ErrInvalidArgument
	}

	g.log.Print(LogInfo, "bucketname:", bucketName, "prefix:", prefix, "page:", fmt.Sprintf("%+v", page))

	ctx := r.Context()
	var objects *ObjectList
	if page.MaxKeys == 0 {
		// Asking for no keys gets none, without asking the backend
		// which treats 0 as unlimited
		objects = NewObjectList()
	} else {
		objects, err = g.storage.ListBucket(ctx, bucketName, &prefix, page)
	}
	if err != nil {
		if err == ErrInternalPageNotImplemented && !g.failOnUnimplementedPage {
			// We have observed (though not yet confirmed) that simple clients
			// tend to work fine if you simply ignore pagination, so the
			// default if this is not implemented is to retry without it. If
			// you care about this performance impact for some weird reason,
			// you'll need to handle it yourself.
			objects, err = g.storage.ListBucket(ctx, bucketName, &prefix, ListBucketPage{})
			if err != nil {
				return err
			}

		} else if err == ErrInternalPageNotImplemented && g.failOnUnimplementedPage {
			return ErrNotImplemented
		} else {
			return err
		}
	}

	// V1 listings always have the owner, V2 only if asked for
	withOwner := !isVersion2 || q.Get("fetch-owner") == "true"
	for _, content := range objects.Contents {
		if !withOwner {
			content.Owner = nil
		} else if content.Owner == nil {
			content.Owner = &defaultOwner
		}
	}

	base := ListBucketResultBase{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           bucketName,
		CommonPrefixes: objects.CommonPrefixes,
		Contents:       objects.Contents,
		IsTruncated:    objects.IsTruncated,
		Delimiter:      prefix.Delimiter,
		Prefix:         prefix.Prefix,
		MaxKeys:        page.MaxKeys,
		EncodingType:   encodingType,
	}
	if useUrlEncoding {
		// S3 encodes the Prefix of V2 listings but not V1
		if isVersion2 {
			base.Prefix = URLEncode(base.Prefix)
		}
		base.Delimiter = URLEncode(base.Delimiter)
		for i := range base.CommonPrefixes {
			prefix := &base.CommonPrefixes[i]
			prefix.Prefix = URLEncode(prefix.Prefix)
		}
		for _, content := range base.Contents {
			content.Key = URLEncode(content.Key)
		}
	}
	encode := func(s string) string {
		if useUrlEncoding {
			return URLEncode(s)
		}
		return s
	}

	if !isVersion2 {
		var result = &ListBucketResult{
			ListBucketResultBase: base,
			Marker:               encode(page.Marker),
		}
		if base.Delimiter != "" {
			// From the S3 docs: "This element is returned only if you specify
			// a delimiter request parameter." Dunno why. This hack has been moved
			// into GoFakeS3 to spare backend implementers the trouble.
			result.NextMarker = encode(objects.NextMarker)
		}
		return g.xmlEncoder(w).Encode(result)

	} else {
		var result = &ListBucketResultV2{
			ListBucketResultBase: base,
			KeyCount:             int64(len(objects.CommonPrefixes) + len(objects.Contents)),
			StartAfter:           encode(q.Get("start-after")),
		}
		if _, ok := q["continuation-token"]; ok {
			token := q.Get("continuation-token")
			result.ContinuationToken = &token
		}
		if objects.NextMarker != "" {
			// We are just cheating with these continuation tokens; they're just the NextMarker
			// from v1 in disguise! That may change at any time and should not be relied upon
			// though.
			result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(objects.NextMarker))
		}

		return g.xmlEncoder(w).Encode(result)
	}
}

func (g *GoFakeS3) getBucketLocation(bucketName string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "GET BUCKET LOCATION")

	if err := g.ensureBucketExists(r, bucketName); err != nil { // S300006
		return err
	}

	result := GetBucketLocation{
		Xmlns:              "http://s3.amazonaws.com/doc/2006-03-01/",
		LocationConstraint: "",
	}

	return g.xmlEncoder(w).Encode(result)
}

func (g *GoFakeS3) listBucketVersions(bucketName string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucketName); err != nil {
		return err
	}

	if g.versioned == nil {
		return g.listUnversionedBucketVersions(bucketName, w, r)
	}

	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketVersionsPageFromQuery(q)
	if err != nil {
		return err
	}

	// S300004:
	if page.HasVersionIDMarker {
		if page.VersionIDMarker == "" {
			return ErrorInvalidArgument("version-id-marker", "", "A version-id marker cannot be empty.")
		} else if !page.HasKeyMarker {
			return ErrorInvalidArgument("version-id-marker", "", "A version-id marker cannot be specified without a key marker.")
		}

	} else if page.HasKeyMarker && page.KeyMarker == "" {
		// S300004: S3 ignores everything if you pass an empty key marker so
		// let's hide that bit of ugliness from Backend.
		page = ListBucketVersionsPage{}
	}

	bucket, err := g.versioned.ListBucketVersions(bucketName, &prefix, &page)
	if err != nil {
		return err
	}

	for _, ver := range bucket.Versions {
		// S300005: S3 returns the _string_ 'null' for the version ID if the
		// bucket has never had versioning enabled. GoFakeS3 backend
		// implementers should be able to simply return the empty string;
		// GoFakeS3 itself should handle this particular bit of jank once and
		// once only.
		if ver.GetVersionID() == "" {
			ver.setVersionID("null")
		}
	}

	return g.xmlEncoder(w).Encode(bucket)
}

// listUnversionedBucketVersions lists the versions of the objects in a
// bucket of a backend without versioning as S3 does for a bucket which
// has never had versioning enabled: each object has one version,
// "null".
func (g *GoFakeS3) listUnversionedBucketVersions(bucketName string, w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	prefix := prefixFromQuery(q)
	page, err := listBucketVersionsPageFromQuery(q)
	if err != nil {
		return err
	}
	result := NewListBucketVersionsResult(bucketName, &prefix, &page)
	if page.MaxKeys == 0 {
		return g.xmlEncoder(w).Encode(result)
	}

	listPage := ListBucketPage{MaxKeys: page.MaxKeys}
	if page.KeyMarker != "" {
		listPage.Marker, listPage.HasMarker = page.KeyMarker, true
	}
	ctx := r.Context()
	objects, err := g.storage.ListBucket(ctx, bucketName, &prefix, listPage)
	if err == ErrInternalPageNotImplemented {
		objects, err = g.storage.ListBucket(ctx, bucketName, &prefix, ListBucketPage{})
	}
	if err != nil {
		return err
	}

	result.CommonPrefixes = objects.CommonPrefixes
	for _, content := range objects.Contents {
		result.Versions = append(result.Versions, &Version{
			Key:          content.Key,
			VersionID:    "null",
			IsLatest:     true,
			LastModified: content.LastModified,
			Size:         content.Size,
			StorageClass: StorageStandard,
			ETag:         content.ETag,
			Owner:        &defaultOwner,
		})
	}
	result.IsTruncated = objects.IsTruncated
	if objects.IsTruncated {
		result.NextKeyMarker = objects.NextMarker
		if result.NextKeyMarker == "" && len(objects.Contents) > 0 {
			result.NextKeyMarker = objects.Contents[len(objects.Contents)-1].Key
		}
		result.NextVersionIDMarker = "null"
	}
	return g.xmlEncoder(w).Encode(result)
}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) createBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "CREATE BUCKET:", bucket)

	if err := ValidateBucketName(bucket); err != nil {
		return err
	}
	if strings.EqualFold(r.Header.Get("X-Amz-Bucket-Object-Lock-Enabled"), "true") {
		return ErrorMessage(ErrNotImplemented, "Object lock is not supported.")
	}
	if err := g.storage.CreateBucket(r.Context(), bucket); err != nil {
		return err
	}

	w.Header().Set("Location", "/"+bucket)
	_, err := w.Write([]byte{})
	if err != nil {
		return err
	}
	return nil
}

// DeleteBucket deletes the bucket in the underlying backend, if and only if it
// contains no items.
func (g *GoFakeS3) deleteBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "DELETE BUCKET:", bucket)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}
	if err := g.storage.DeleteBucket(r.Context(), bucket); err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) headBucket(bucket string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "HEAD BUCKET", bucket)
	g.log.Print(LogInfo, "bucketname:", bucket)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	_, err := w.Write([]byte{})
	if err != nil {
		return err
	}
	return nil
}

// CheckClose is a utility function used to check the return from
// Close in a defer statement.
func CheckClose(c io.Closer, err *error) {
	cerr := c.Close()
	if *err == nil {
		*err = cerr
	}
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) getObject(
	bucket, object string,
	versionID VersionID,
	w http.ResponseWriter,
	r *http.Request,
) (err error) {

	g.log.Print(LogInfo, "GET OBJECT", "Bucket:", bucket, "Object:", object)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	rnge, err := parseRangeHeader(r.Header.Get("Range"))
	if err != nil {
		return err
	}
	part, partsCount, err := g.objectPart(r, bucket, object)
	if err != nil {
		return err
	}
	if part != nil {
		rnge = part
	}

	var obj *Object

	{ // get object from backend
		if versionID == "" {
			obj, err = g.storage.GetObject(r.Context(), bucket, object, rnge)
			if err != nil {
				return err
			}
		} else {
			if g.versioned == nil {
				return ErrNotImplemented
			}
			obj, err = g.versioned.GetObjectVersion(bucket, object, versionID, rnge)
			if err != nil {
				return err
			}
		}
	}

	if obj == nil {
		g.log.Print(LogErr, "unexpected nil object for key", bucket, object)
		return ErrInternal
	}
	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		_ = obj.Contents.Close()
		return err
	}
	if partsCount > 0 {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(partsCount))
	}

	// Writes Content-Length, and Content-Range if applicable:
	obj.Range.writeHeader(obj.Size, w)

	if _, err := io.Copy(w, obj.Contents); err != nil {
		// The response has started so the error can't be sent. The
		// client sees a short body, as usually it has gone away.
		g.log.Print(LogInfo, "GET OBJECT", bucket, object, "failed:", err)
	}
	// Nor can an error closing the object be sent
	if err := obj.Contents.Close(); err != nil {
		g.log.Print(LogErr, "GET OBJECT", bucket, object, "close failed:", err)
	}
	return nil
}

var responseHeaderOverridesMap = map[string]string{
	"response-content-type":        "Content-Type",
	"response-content-language":    "Content-Language",
	"response-expires":             "Expires",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
}

// writeGetOrHeadObjectResponse contains shared logic for constructing headers for
// a HEAD and a GET request for a /bucket/object URL.
func (g *GoFakeS3) writeGetOrHeadObjectResponse(obj *Object, w http.ResponseWriter, r *http.Request) error {
	// "If the current version of the object is a delete marker, Amazon S3
	// behaves as if the object was deleted and includes x-amz-delete-marker:
	// true in the response."
	if obj.IsDeleteMarker {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
		w.Header().Set("x-amz-delete-marker", "true")
		return KeyNotFound(obj.Name)
	}

	for mk, mv := range obj.Metadata {
		mk = textproto.CanonicalMIMEHeaderKey(mk)
		switch {
		case strings.HasPrefix(mk, "X-Amz-Meta-"):
			// Clients take the case of user metadata names from
			// the response, so send them lower case as AWS does
			// rather than in canonical form.
			w.Header()[strings.ToLower(mk)] = []string{mv}
		case mk == "Last-Modified" || isMetadataHeader(mk):
			w.Header().Set(mk, mv)
		}
	}

	writeTaggingCount(w, obj.Metadata)

	if obj.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(obj.VersionID))
	}

	etag := `"` + hex.EncodeToString(obj.Hash) + `"`
	w.Header().Set("ETag", etag)

	if err := checkConditions(r.Header, getConditions, etag, objectLastModified(obj), ErrNotModified); err != nil {
		return err
	}

	// Checksums are only returned when asked for, and only for the
	// whole object. They mustn't be returned with an error, such as
	// for a failed condition, as clients check the error body
	// against them.
	if strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") && obj.Range == nil && r.Header.Get("Range") == "" && r.URL.Query().Get("partNumber") == "" {
		for mk, mv := range obj.Metadata {
			if mk = textproto.CanonicalMIMEHeaderKey(mk); strings.HasPrefix(mk, checksumHeaderPrefix) {
				w.Header().Set(mk, mv)
			}
		}
	}

	w.Header().Set("Accept-Ranges", "bytes")

	// Handle response-* query parameters to override response headers
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
	query := r.URL.Query()
	for queryParam, headerName := range responseHeaderOverridesMap {
		if v := query.Get(queryParam); v != "" {
			w.Header().Set(headerName, v)
		}
	}
	return nil
}

// headObject retrieves only meta information of an object and not the whole.
func (g *GoFakeS3) headObject(
	bucket, object string,
	versionID VersionID,
	w http.ResponseWriter,
	r *http.Request,
) (err error) {

	g.log.Print(LogInfo, "HEAD OBJECT", bucket, object)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	part, partsCount, err := g.objectPart(r, bucket, object)
	if err != nil {
		return err
	}

	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err != nil {
		return err
	}
	if obj == nil {
		g.log.Print(LogErr, "unexpected nil object for key", bucket, object)
		return ErrInternal
	}
	defer CheckClose(obj.Contents, &err)

	if err := g.writeGetOrHeadObjectResponse(obj, w, r); err != nil {
		return err
	}

	if part != nil {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(partsCount))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", part.Start, part.End, obj.Size))
		w.Header().Set("Content-Length", fmt.Sprintf("%d", part.End-part.Start+1))
		w.WriteHeader(http.StatusPartialContent)
		return nil
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))

	return nil
}

// maxPOSTBodySize is the largest browser upload body accepted: S3 allows
// objects of up to 5 GB to be uploaded this way.
var maxPOSTBodySize int64 = 5 * 1024 * 1024 * 1024

// postFileMemory is how much of a browser upload's file is held in memory
// before the rest is spooled to a temporary file.
const postFileMemory = 1024 * 1024

// createObjectBrowserUpload allows objects to be created from a multipart upload initiated
// by a browser form.
func (g *GoFakeS3) createObjectBrowserUpload(bucket string, w http.ResponseWriter, r *http.Request) (err error) {
	g.log.Print(LogInfo, "CREATE OBJECT THROUGH BROWSER UPLOAD")

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxPOSTBodySize)
	if err := r.ParseMultipartForm(postFileMemory); nil != err {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return ErrEntityTooLarge
		}
		return ErrMalformedPOSTRequest
	}

	keyValues := r.MultipartForm.Value["key"]
	if len(keyValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	key := keyValues[0]

	g.log.Print(LogInfo, "(BUC)", bucket)
	g.log.Print(LogInfo, "(KEY)", key)

	fileValues := r.MultipartForm.File["file"]
	if len(fileValues) != 1 {
		return ErrIncorrectNumberOfFilesInPostRequest
	}
	fileHeader := fileValues[0]

	infile, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer CheckClose(infile, &err)

	meta, err := metadataHeaders(r.MultipartForm.Value, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if len(key) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, key)
	}

	// FIXME: how does Content-MD5 get sent when using the browser? does it?
	rdr, err := newHashingReader(infile, "")
	if err != nil {
		return err
	}

	result, err := g.storage.PutObject(r.Context(), bucket, key, meta, rdr, fileHeader.Size)
	if err != nil {
		return err
	}
	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)
	return nil
}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) createObject(bucket, object string, w http.ResponseWriter, r *http.Request) (err error) {
	g.log.Print(LogInfo, "CREATE OBJECT:", bucket, object)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}

	if err := checkWriteEncryption(r.Header); err != nil {
		return err
	}

	if r.Header.Get("X-Amz-Copy-Source") != "" {
		return g.copyObject(bucket, object, meta, w, r)
	}

	r, err = g.checkWriteConditions(r, bucket, object)
	if err != nil {
		return err
	}

	// The size is unknown (-1) if the body is sent with chunked transfer
	// encoding rather than a Content-Length
	size := int64(-1)
	if contentLength := r.Header.Get("Content-Length"); contentLength != "" {
		size, err = strconv.ParseInt(contentLength, 10, 64)
		if err != nil || size < 0 {
			w.WriteHeader(http.StatusBadRequest) // XXX: no code for this, according to s3tests
			return nil
		}
	} else if !slices.Contains(r.TransferEncoding, "chunked") {
		return ErrMissingContentLength
	}

	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}

	var md5Base64 string
	if g.integrityCheck {
		md5Base64 = r.Header.Get("Content-MD5")

		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest // Satisfies s3tests
		}
	}

	var reader io.Reader

	if isChunkedStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		reader = newChunkedReader(r.Body)
	} else {
		reader = r.Body
	}

	// This header is set e.g. when the body contains trailing headers
	// It is used to extract real object length
	if decodedLength := r.Header.Get("X-Amz-Decoded-Content-Length"); decodedLength != "" {
		size, err = strconv.ParseInt(decodedLength, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest) // XXX: no code for this, according to s3tests
			return nil
		}
	}

	// Ensure reader only contains object (strips the trailing headers)
	if size >= 0 {
		reader = io.LimitReader(reader, size)
	}

	// hashingReader is still needed to get the ETag even if integrityCheck
	// is set to false:
	rdr, err := newHashingReader(reader, md5Base64)
	// The error from closing the request body is discarded - from go1.27
	// Close returns io.EOF if the body was not read to EOF (eg when the
	// body contains unread trailing headers), and a close error tells us
	// nothing useful anyway as real I/O problems surface from Read.
	defer func() { _ = r.Body.Close() }()
	if err != nil {
		return err
	}
	reader = rdr

	// Check the body against any checksum given, and store it to be
	// returned when asked for
	checksumAlgorithm, checksum, err := requestChecksum(r.Header)
	if err != nil {
		return err
	}
	checksumHeader, checksumValue := "", ""
	if checksumAlgorithm != "" {
		reader = newChecksumReader(reader, checksumAlgorithm, checksum)
		checksumHeader = textproto.CanonicalMIMEHeaderKey(checksumHeaderPrefix + checksumAlgorithm)
		checksumValue = base64.StdEncoding.EncodeToString(checksum)
		meta[checksumHeader] = checksumValue
	}

	result, err := g.storage.PutObject(r.Context(), bucket, object, meta, reader, size)
	if err != nil {
		return err
	}
	if checksumHeader != "" {
		w.Header().Set(checksumHeader, checksumValue)
	}

	if result.VersionID != "" {
		g.log.Print(LogInfo, "CREATED VERSION:", bucket, object, result.VersionID)
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(rdr.Sum(nil))+`"`)

	return nil
}

// CopyObject copies an existing S3 object
func (g *GoFakeS3) copyObject(bucket, object string, meta map[string]string, w http.ResponseWriter, r *http.Request) (err error) {
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	source := r.Header.Get("X-Amz-Copy-Source")
	g.log.Print(LogInfo, "COPY:", source, "TO", bucket, object)

	if len(object) > KeySizeLimit {
		return ResourceError(ErrKeyTooLong, object)
	}

	srcBucket, srcKey, err := parseCopySource(source)
	if err != nil {
		return err
	}

	directive := r.Header.Get("X-Amz-Metadata-Directive")
	if directive != "" && directive != "COPY" && directive != "REPLACE" {
		return ErrorInvalidArgument("x-amz-metadata-directive", directive, "Unknown metadata directive.")
	}
	if srcBucket == bucket && srcKey == object && directive != "REPLACE" && r.Header.Get("X-Amz-Storage-Class") == "" {
		return ErrorMessage(ErrInvalidRequest, "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
	}
	ctx := r.Context()
	srcObj, err := g.storage.HeadObject(ctx, srcBucket, srcKey)
	if err != nil {
		return err
	}
	srcETag := `"` + hex.EncodeToString(srcObj.Hash) + `"`
	if err := checkConditions(r.Header, copySourceConditions, srcETag, objectLastModified(srcObj), ErrPreconditionFailed); err != nil {
		return err
	}

	// if srcObj == nil {
	// 	g.log.Print(LogErr, "unexpected nil object for key", bucket, object)
	// 	return ErrInternal
	// }
	// defer srcObj.Contents.Close()

	// XXX No support for delete marker
	// "If the current version of the object is a delete marker, Amazon S3
	// behaves as if the object was deleted."

	// With the COPY directive, the default, the new object has the
	// source's metadata rather than any in the request
	tagging := meta[TaggingMetadataKey]
	if directive != "REPLACE" {
		lastModified := meta["Last-Modified"]
		meta = make(map[string]string, len(srcObj.Metadata))
		for k, v := range srcObj.Metadata {
			if isMetadataHeader(textproto.CanonicalMIMEHeaderKey(k)) {
				meta[k] = v
			}
		}
		meta["Last-Modified"] = lastModified
	}

	// The tags are copied too unless x-amz-tagging-directive is REPLACE
	switch tagDirective := r.Header.Get("X-Amz-Tagging-Directive"); tagDirective {
	case "", "COPY":
		tagging = srcObj.Metadata[TaggingMetadataKey]
	case "REPLACE":
	default:
		return ErrorInvalidArgument("x-amz-tagging-directive", tagDirective, "Unknown tagging directive.")
	}
	delete(meta, TaggingMetadataKey)
	if tagging != "" {
		meta[TaggingMetadataKey] = tagging
	}

	result, err := g.storage.CopyObject(ctx, srcBucket, srcKey, bucket, object, meta)
	if err != nil {
		return err
	}

	if srcObj.VersionID != "" {
		w.Header().Set("x-amz-copy-source-version-id", string(srcObj.VersionID))
	}
	// currently not supported
	// if result.VersionID != "" {
	// 	g.log.Print(LogInfo, "CREATED VERSION:", bucket, object, result.VersionID)
	// 	w.Header().Set("x-amz-version-id", string(result.VersionID))
	// }

	return g.xmlEncoder(w).Encode(result)
}

// parseCopySource returns the bucket and key of the source of a copy
// from its x-amz-copy-source header.
func parseCopySource(source string) (bucket, key string, err error) {
	// XXX No support for versionId subresource
	parts := strings.SplitN(strings.TrimPrefix(source, "/"), "/", 2)
	if len(parts) != 2 {
		return "", "", ErrorInvalidArgument("x-amz-copy-source", source, "Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	bucket = parts[0]
	key = strings.SplitN(parts[1], "?", 2)[0]
	key, err = url.QueryUnescape(key)
	if err != nil {
		return "", "", ErrorInvalidArgument("x-amz-copy-source", source, "Invalid copy source encoding")
	}
	return bucket, key, nil
}

func (g *GoFakeS3) deleteObject(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "DELETE:", bucket, object)
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	c := deleteConditions{
		etag:         r.Header.Get("If-Match"),
		lastModified: r.Header.Get("X-Amz-If-Match-Last-Modified-Time"),
	}
	if v := r.Header.Get("X-Amz-If-Match-Size"); v != "" {
		size, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return ErrorInvalidArgument("x-amz-if-match-size", v, "Invalid size")
		}
		c.size = &size
	}
	if err := g.checkDeleteConditions(r.Context(), bucket, object, c); err != nil {
		return err
	}

	result, err := g.storage.DeleteObject(r.Context(), bucket, object)
	if err != nil {
		return err
	}

	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (g *GoFakeS3) deleteObjectVersion(bucket, object string, version VersionID, w http.ResponseWriter, r *http.Request) error {
	if g.versioned == nil {
		return ErrNotImplemented
	}

	g.log.Print(LogInfo, "DELETE VERSION:", bucket, object, version)
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	result, err := g.versioned.DeleteObjectVersion(bucket, object, version)
	if err != nil {
		return err
	}
	g.log.Print(LogInfo, "DELETED VERSION:", bucket, object, version)

	if result.IsDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
	} else {
		w.Header().Set("x-amz-delete-marker", "false")
	}

	if result.VersionID != "" {
		w.Header().Set("x-amz-version-id", string(result.VersionID))
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// deleteMulti deletes multiple S3 objects from the bucket.
// https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html
func (g *GoFakeS3) deleteMulti(bucket string, w http.ResponseWriter, r *http.Request) (err error) {
	g.log.Print(LogInfo, "delete multi", bucket)

	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	var in DeleteRequest
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		return err
	}
	if len(in.Objects) > MaxDeleteObjects {
		return ErrorMessagef(ErrMalformedXML, "a maximum of %d keys can be deleted in one request", MaxDeleteObjects)
	}

	// Objects whose conditions aren't met aren't deleted
	keys := make([]string, 0, len(in.Objects))
	var refused []ErrorResult
	for _, o := range in.Objects {
		c := deleteConditions{etag: o.ETag, lastModified: o.LastModifiedTime, size: o.Size}
		if err := g.checkDeleteConditions(r.Context(), bucket, o.Key, c); err != nil {
			result := ErrorResultFromError(err)
			result.Key = o.Key
			refused = append(refused, result)
			continue
		}
		keys = append(keys, o.Key)
	}

	var out MultiDeleteResult
	if len(keys) > 0 {
		out, err = g.storage.DeleteMulti(r.Context(), bucket, keys...)
		if err != nil {
			return err
		}
	}
	out.Error = append(out.Error, refused...)

	if in.Quiet {
		out.Deleted = nil
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *GoFakeS3) initiateMultipartUpload(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "initiate multipart upload", bucket, object)

	meta, err := metadataHeaders(r.Header, g.timeSource.Now(), g.metadataSizeLimit)
	if err != nil {
		return err
	}
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}
	if err := checkWriteEncryption(r.Header); err != nil {
		return err
	}

	var uploadID UploadID
	if g.multipart != nil {
		id, err := g.multipart.CreateMultipartUpload(r.Context(), bucket, object, meta)
		switch {
		case err == nil:
			if _, err := g.uploader.BeginStreaming(g.owner(r.Context()), id, bucket, object, meta, g.timeSource.Now()); err != nil {
				// Backend returned a colliding UploadID. Tell it to drop
				// the upload it just created so we do not leak state.
				_ = g.multipart.AbortMultipartUpload(r.Context(), bucket, object, id)
				return err
			}
			uploadID = id
		case errors.Is(err, ErrMultipartUploadNotSupported):
			// fall through to in-memory uploader
		default:
			return err
		}
	}
	if uploadID == "" {
		upload := g.uploader.Begin(g.owner(r.Context()), bucket, object, meta, g.timeSource.Now())
		uploadID = upload.ID
	}

	out := InitiateMultipartUpload{
		UploadID: uploadID,
		Bucket:   bucket,
		Key:      object,
	}
	return g.xmlEncoder(w).Encode(out)
}

// From the docs:
//
//	A part number uniquely identifies a part and also defines its position
//	within the object being created. If you upload a new part using the same
//	part number that was used with a previous part, the previously uploaded part
//	is overwritten. Each part must be at least 5 MB in size, except the last
//	part. There is no size limit on the last part of your multipart upload.
func (g *GoFakeS3) putMultipartUploadPart(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) (err error) {
	g.log.Print(LogInfo, "put multipart upload", bucket, object, uploadID)

	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 0)
	if err != nil || partNumber <= 0 || partNumber > MaxUploadPartNumber {
		return ErrInvalidPart
	}

	if err := checkWriteEncryption(r.Header); err != nil {
		return err
	}

	upload, err := g.uploader.Get(g.owner(r.Context()), bucket, object, uploadID)
	if err != nil {
		// FIXME: What happens with S3 when you abort a multipart upload while
		// part uploads are still in progress? In this case, we will retain the
		// reference to the part even though another request goroutine may
		// delete it; it will be available for GC when this function finishes.
		return err
	}

	if r.Header.Get("X-Amz-Copy-Source") != "" {
		return g.uploadPartCopy(upload, int(partNumber), w, r)
	}

	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return ErrMissingContentLength
	}

	// The error from closing the request body is discarded - from go1.27
	// Close returns io.EOF if the body was not read to EOF (eg when the
	// body contains unread trailing headers), and a close error tells us
	// nothing useful anyway as real I/O problems surface from Read.
	defer func() { _ = r.Body.Close() }()

	var rdr io.Reader
	if isChunkedStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		rdr = newChunkedReader(r.Body)
		size, err = strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest) // XXX: no code for this, according to s3tests
			return nil
		}
	} else {
		rdr = r.Body
	}

	// This header is set e.g. when the body contains trailing headers
	// It is used to extract real object length
	if decodedLength := r.Header.Get("X-Amz-Decoded-Content-Length"); decodedLength != "" {
		size, err = strconv.ParseInt(decodedLength, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest) // XXX: no code for this, according to s3tests
			return nil
		}
	}

	// Ensure reader only contains object (strips the trailing headers)
	rdr = io.LimitReader(rdr, size)

	if g.integrityCheck {
		md5Base64 := r.Header.Get("Content-MD5")
		if _, ok := r.Header[textproto.CanonicalMIMEHeaderKey("Content-MD5")]; ok && md5Base64 == "" {
			return ErrInvalidDigest // Satisfies s3tests
		}

		if md5Base64 != "" {
			var err error
			rdr, err = newHashingReader(rdr, md5Base64)
			if err != nil {
				return err
			}
		}
	}

	etag, err := g.storePart(r.Context(), upload, int(partNumber), size, rdr)
	if err != nil {
		return err
	}
	w.Header().Add("ETag", etag)
	return nil
}

// storePart stores part partNumber of upload, size bytes read from rdr,
// returning its ETag.
func (g *GoFakeS3) storePart(ctx context.Context, upload *multipartUpload, partNumber int, size int64, rdr io.Reader) (etag string, err error) {
	if upload.streaming {
		etag, err := g.multipart.UploadPart(ctx, upload.Bucket, upload.Object, upload.ID, partNumber, size, rdr)
		if err != nil {
			return "", err
		}
		if err := upload.AddStreamingPart(partNumber, g.timeSource.Now(), size, etag); err != nil {
			return "", err
		}
		return etag, nil
	}

	body, err := ReadAll(rdr, size)
	if err != nil {
		return "", err
	}

	if int64(len(body)) != size {
		return "", ErrIncompleteBody
	}

	return upload.AddPart(partNumber, g.timeSource.Now(), body)
}

// copySourceRange matches a valid x-amz-copy-source-range.
var copySourceRange = regexp.MustCompile(`^bytes=([0-9]+)-([0-9]+)$`)

// uploadPartCopy stores part partNumber of upload by copying all or a
// range of an existing object.
func (g *GoFakeS3) uploadPartCopy(upload *multipartUpload, partNumber int, w http.ResponseWriter, r *http.Request) (err error) {
	ctx := r.Context()
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		return err
	}
	g.log.Print(LogInfo, "upload part copy", srcBucket, srcKey, "to", upload.Bucket, upload.Object, upload.ID, partNumber)

	srcObj, err := g.storage.HeadObject(ctx, srcBucket, srcKey)
	if err != nil {
		return err
	}
	if srcObj.Contents != nil {
		_ = srcObj.Contents.Close()
	}
	srcETag := `"` + hex.EncodeToString(srcObj.Hash) + `"`
	if err := checkConditions(r.Header, copySourceConditions, srcETag, objectLastModified(srcObj), ErrPreconditionFailed); err != nil {
		return err
	}

	var rnge *ObjectRangeRequest
	if v := r.Header.Get("X-Amz-Copy-Source-Range"); v != "" {
		m := copySourceRange.FindStringSubmatch(v)
		if m == nil {
			return ErrorInvalidArgument("x-amz-copy-source-range", v, "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")
		}
		start, err1 := strconv.ParseInt(m[1], 10, 64)
		end, err2 := strconv.ParseInt(m[2], 10, 64)
		if err1 != nil || err2 != nil {
			return ErrorInvalidArgument("x-amz-copy-source-range", v, "The x-amz-copy-source-range value is out of range")
		}
		if start > end || end >= srcObj.Size {
			return ErrorMessage(ErrInvalidRange, "The requested range is not satisfiable")
		}
		rnge = &ObjectRangeRequest{Start: start, End: end}
	}

	obj, err := g.storage.GetObject(ctx, srcBucket, srcKey, rnge)
	if err != nil {
		return err
	}
	defer CheckClose(obj.Contents, &err)
	size := obj.Size
	if obj.Range != nil {
		size = obj.Range.Length
	}

	etag, err := g.storePart(ctx, upload, partNumber, size, obj.Contents)
	if err != nil {
		return err
	}
	return g.xmlEncoder(w).Encode(&CopyPartResult{
		ETag:         etag,
		LastModified: NewContentTime(g.timeSource.Now()),
	})
}

// isChunkedStreamingPayload reports whether x-amz-content-sha256 denotes aws-chunked transfer encoding.
func isChunkedStreamingPayload(value string) bool {
	return value == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" ||
		value == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
}

func (g *GoFakeS3) abortMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "abort multipart upload", bucket, object, uploadID)
	upload, err := g.uploader.Get(g.owner(r.Context()), bucket, object, uploadID)
	if err != nil {
		return err
	}
	if upload.streaming {
		if err := g.multipart.AbortMultipartUpload(r.Context(), bucket, object, uploadID); err != nil {
			return err
		}
	}
	// Consume from the uploader only after the backend has acknowledged the
	// abort, so a backend error leaves the upload available for retry.
	if _, err := g.uploader.Complete(g.owner(r.Context()), bucket, object, uploadID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ForgetMultipartUpload removes the record of the streaming multipart upload
// uploadID which the MultipartBackend has already discarded itself - for
// example because it expired - so it no longer shows in
// ListMultipartUploads. It returns ErrNoSuchUpload if there is no such
// upload.
//
// With WithUploadOwner, ctx must give the owner of the upload.
func (g *GoFakeS3) ForgetMultipartUpload(ctx context.Context, bucket, object string, uploadID UploadID) error {
	_, err := g.uploader.Complete(g.owner(ctx), bucket, object, uploadID)
	return err
}

// owner returns the owner of the multipart uploads made by a request
// with ctx - see WithUploadOwner.
func (g *GoFakeS3) owner(ctx context.Context) string {
	if g.uploadOwner == nil {
		return ""
	}
	return g.uploadOwner(ctx)
}

func (g *GoFakeS3) completeMultipartUpload(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "complete multipart upload", bucket, object, uploadID)

	owner := g.owner(r.Context())
	upload, err := g.uploader.Get(owner, bucket, object, uploadID)
	if err == ErrNoSuchUpload {
		// Completing an upload again, as a client which didn't get the
		// response to the first attempt would, succeeds as on S3
		if completed, ok := g.uploader.Completed(owner, bucket, object, uploadID); ok {
			return g.writeCompleteMultipartUploadResult(w, bucket, object, completed.versionID, completed.etag)
		}
	}
	if err != nil {
		return err
	}

	var in CompleteMultipartUploadRequest
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		return err
	}

	r, err = g.checkWriteConditions(r, bucket, object)
	if err != nil {
		return err
	}

	if upload.streaming {
		versionID, etag, err := g.multipart.CompleteMultipartUpload(r.Context(), bucket, object, uploadID, &in)
		if err != nil {
			return err
		}
		// Consume from the uploader only after the backend has acknowledged
		// completion, so a backend error leaves the upload available for retry.
		if _, err := g.uploader.Complete(owner, bucket, object, uploadID); err != nil {
			return err
		}
		g.uploader.RecordCompleted(owner, bucket, object, uploadID, versionID, etag)
		return g.writeCompleteMultipartUploadResult(w, bucket, object, versionID, etag)
	}

	fileBody, etag, err := upload.Reassemble(&in)
	if err != nil {
		return err
	}

	result, err := g.storage.PutObject(r.Context(), bucket, object, upload.Meta, bytes.NewReader(fileBody), int64(len(fileBody)))
	if err != nil {
		return err
	}
	// Consume from the uploader only after PutObject has succeeded.
	if _, err := g.uploader.Complete(owner, bucket, object, uploadID); err != nil {
		return err
	}
	g.uploader.RecordCompleted(owner, bucket, object, uploadID, result.VersionID, etag)
	return g.writeCompleteMultipartUploadResult(w, bucket, object, result.VersionID, etag)
}

// writeCompleteMultipartUploadResult writes the response to a
// successful CompleteMultipartUpload.
func (g *GoFakeS3) writeCompleteMultipartUploadResult(w http.ResponseWriter, bucket, object string, versionID VersionID, etag string) error {
	if versionID != "" {
		w.Header().Set("x-amz-version-id", string(versionID))
	}
	return g.xmlEncoder(w).Encode(&CompleteMultipartUploadResult{
		ETag:   etag,
		Bucket: bucket,
		Key:    object,
	})
}

func (g *GoFakeS3) listMultipartUploads(bucket string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	query := r.URL.Query()
	prefix := prefixFromQuery(query)
	marker := uploadListMarkerFromQuery(query)

	maxUploads, err := parseClampedInt(query.Get("max-uploads"), DefaultMaxUploads, 0, MaxUploadsLimit)
	if err != nil {
		return ErrInvalidURI
	}
	if maxUploads == 0 {
		maxUploads = DefaultMaxUploads
	}

	out, err := g.uploader.List(g.owner(r.Context()), bucket, marker, prefix, maxUploads)
	if err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *GoFakeS3) listMultipartUploadParts(bucket, object string, uploadID UploadID, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	query := r.URL.Query()

	marker, err := parseClampedInt(query.Get("part-number-marker"), 0, 0, math.MaxInt64)
	if err != nil {
		return ErrInvalidURI
	}

	maxParts, err := parseClampedInt(query.Get("max-parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		return ErrInvalidURI
	}

	out, err := g.uploader.ListParts(g.owner(r.Context()), bucket, object, uploadID, int(marker), maxParts)
	if err != nil {
		return err
	}

	return g.xmlEncoder(w).Encode(out)
}

func (g *GoFakeS3) getBucketVersioning(bucket string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucket); err != nil { // S300007
		return err
	}

	var config VersioningConfiguration

	if g.versioned != nil {
		var err error
		config, err = g.versioned.VersioningConfiguration(bucket)
		if err != nil {
			return err
		}
	}

	return g.xmlEncoder(w).Encode(config)
}

func (g *GoFakeS3) putBucketVersioning(bucket string, w http.ResponseWriter, r *http.Request) error {
	if err := g.ensureBucketExists(r, bucket); err != nil { // S300007
		return err
	}

	var in VersioningConfiguration
	if err := g.xmlDecodeBody(r.Body, &in); err != nil {
		return err
	}

	if g.versioned == nil {
		if in.MFADelete == MFADeleteEnabled || in.Status == VersioningEnabled {
			// We only need to respond that this is not implemented if there's an
			// attempt to enable it. If we receive a request to disable it, or an
			// empty request, that matches the current state and has no effect so
			// we can accept it.
			return ErrNotImplemented
		} else {
			return nil
		}
	}

	g.log.Print(LogInfo, "PUT VERSIONING:", in.Status)
	return g.versioned.SetVersioningConfiguration(bucket, in)
}

func (g *GoFakeS3) ensureBucketExists(r *http.Request, bucket string) error {
	ctx := r.Context()
	exists, err := g.storage.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if !exists && g.autoBucket {
		if err := g.storage.CreateBucket(ctx, bucket); err != nil {
			g.log.Print(LogErr, "autobucket create failed:", err)
			return ResourceError(ErrNoSuchBucket, bucket)
		}
	} else if !exists {
		return ResourceError(ErrNoSuchBucket, bucket)
	}
	return nil
}

func (g *GoFakeS3) xmlEncoder(w http.ResponseWriter) *xml.Encoder {
	_, _ = w.Write([]byte(xml.Header))
	w.Header().Set("Content-Type", "application/xml")

	xe := xml.NewEncoder(w)
	xe.Indent("", "  ")
	return xe
}

// xmlDecodeBody decodes the XML request body in rdr into into, failing
// with ErrMaxMessageLengthExceeded if it is bigger than MaxXMLBodySize.
func (g *GoFakeS3) xmlDecodeBody(rdr io.ReadCloser, into interface{}) (err error) {
	body, err := io.ReadAll(io.LimitReader(rdr, MaxXMLBodySize+1))
	defer CheckClose(rdr, &err)
	if err != nil {
		return err
	}
	if len(body) > MaxXMLBodySize {
		return ErrMaxMessageLengthExceeded
	}

	if err := xml.Unmarshal(body, into); err != nil {
		return ErrorMessage(ErrMalformedXML, err.Error())
	}

	return nil
}

func formatHeaderTime(t time.Time) string {
	// https://github.com/aws/aws-sdk-go/issues/1937 - FIXED
	// https://github.com/aws/aws-sdk-go-v2/issues/178 - Still open
	// .Format("Mon, 2 Jan 2006 15:04:05 MST")

	tc := t.In(time.UTC)
	return tc.Format("Mon, 02 Jan 2006 15:04:05") + " GMT"
}

func metadataSize(meta map[string]string) int {
	total := 0
	for k, v := range meta {
		total += len(k) + len(v)
	}
	return total
}

// isMetadataHeader reports whether the header hk, in canonical form,
// is stored with an object and returned when it is read.
//
// Other request headers, such as X-Amz-Date, X-Amz-Checksum-* or the
// SSE-C key in X-Amz-Server-Side-Encryption-Customer-Key, describe
// the request only and must not be stored and returned.
func isMetadataHeader(hk string) bool {
	switch hk {
	case "Cache-Control",
		"Content-Disposition",
		"Content-Encoding",
		"Content-Language",
		"Content-Type",
		"Expires",
		"X-Amz-Storage-Class",
		"X-Amz-Website-Redirect-Location":
		return true
	}
	return strings.HasPrefix(hk, "X-Amz-Meta-")
}

// metadataHeaders returns the object metadata in the request headers,
// with Last-Modified set to at.
func metadataHeaders(headers map[string][]string, at time.Time, sizeLimit int) (map[string]string, error) {
	meta := make(map[string]string)
	for hk, hv := range headers {
		hk = textproto.CanonicalMIMEHeaderKey(hk)
		if len(hv) == 0 || !(isMetadataHeader(hk) || hk == "X-Amz-Tagging") {
			continue
		}
		v := hv[0]
		if hk == "X-Amz-Tagging" {
			tagging, err := parseTaggingHeader(v)
			if err != nil {
				return nil, err
			}
			if tagging != "" {
				meta[TaggingMetadataKey] = tagging
			}
			continue
		}
		if hk == "Content-Encoding" {
			// aws-chunked describes how the request body is sent,
			// not the object
			v = removeToken(v, "aws-chunked")
			if v == "" {
				continue
			}
		}
		meta[hk] = v
	}
	meta["Last-Modified"] = formatHeaderTime(at)

	if sizeLimit > 0 && metadataSize(meta) > sizeLimit {
		return meta, ErrMetadataTooLarge
	}

	return meta, nil
}

// removeToken removes token from the comma separated list of tokens in
// v, returning v unchanged if it doesn't have it.
func removeToken(v, token string) string {
	var out []string
	found := false
	for _, t := range strings.Split(v, ",") {
		t = strings.TrimSpace(t)
		if strings.EqualFold(t, token) {
			found = true
		} else if t != "" {
			out = append(out, t)
		}
	}
	if !found {
		return v
	}
	return strings.Join(out, ", ")
}

func listBucketPageFromQuery(query url.Values) (page ListBucketPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketKeys, 0, MaxBucketKeys)
	if err != nil {
		return page, err
	}

	page.MaxKeys = maxKeys

	if _, page.HasMarker = query["marker"]; page.HasMarker {
		// List Objects V1 uses marker only:
		page.Marker = query.Get("marker")

	} else if _, page.HasMarker = query["continuation-token"]; page.HasMarker {
		// List Objects V2 uses continuation-token preferentially, or
		// start-after if continuation-token is missing. continuation-token is
		// an opaque value that looks like this: 1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=.
		// This just looks like base64 junk so we just cheat and base64 encode
		// the next marker and hide it in a continuation-token.
		tok, err := base64.URLEncoding.DecodeString(query.Get("continuation-token"))
		if err != nil {
			// FIXME: log
			return page, ErrInvalidToken // FIXME: confirm for sure what AWS does here
		}
		page.Marker = string(tok)

	} else if _, page.HasMarker = query["start-after"]; page.HasMarker {
		// List Objects V2 uses start-after if continuation-token is missing:
		page.Marker = query.Get("start-after")
	}

	return page, nil
}

func listBucketVersionsPageFromQuery(query url.Values) (page ListBucketVersionsPage, rerr error) {
	maxKeys, err := parseClampedInt(query.Get("max-keys"), DefaultMaxBucketVersionKeys, 0, MaxBucketVersionKeys)
	if err != nil {
		return page, err
	}

	page.MaxKeys = maxKeys
	page.KeyMarker = query.Get("key-marker")
	page.VersionIDMarker = VersionID(query.Get("version-id-marker"))
	_, page.HasKeyMarker = query["key-marker"]
	_, page.HasVersionIDMarker = query["version-id-marker"]

	return page, nil
}

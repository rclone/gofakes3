package gofakes3

import (
	"net/http"
	"strings"

	xml "github.com/minio/xxml"
)

// Grantee is the subject of a Grant.
type Grantee struct {
	XMLNSXsi    string `xml:"xmlns:xsi,attr"`
	Type        string `xml:"xsi:type,attr"`
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
}

// Grant gives a Grantee a Permission.
type Grant struct {
	Grantee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

// AccessControlPolicy is an access control list of a bucket or object.
type AccessControlPolicy struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Owner   UserInfo `xml:"Owner"`
	Grants  []Grant  `xml:"AccessControlList>Grant"`
}

// defaultACL is the access control list of every bucket and object: the
// owner has full control.
func defaultACL() AccessControlPolicy {
	return AccessControlPolicy{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: defaultOwner,
		Grants: []Grant{{
			Grantee: Grantee{
				XMLNSXsi:    "http://www.w3.org/2001/XMLSchema-instance",
				Type:        "CanonicalUser",
				ID:          defaultOwner.ID,
				DisplayName: defaultOwner.DisplayName,
			},
			Permission: "FULL_CONTROL",
		}},
	}
}

// cannedACLs are the canned ACLs S3 supports.
var cannedACLs = map[string]bool{
	"private":                   true,
	"public-read":               true,
	"public-read-write":         true,
	"authenticated-read":        true,
	"aws-exec-read":             true,
	"bucket-owner-read":         true,
	"bucket-owner-full-control": true,
	"log-delivery-write":        true,
}

// putACL checks a PUT ?acl request, which can only set the default
// ACL, the only one supported.
func (g *GoFakeS3) putACL(r *http.Request) error {
	for _, hdr := range []string{"X-Amz-Grant-Read", "X-Amz-Grant-Write", "X-Amz-Grant-Read-Acp", "X-Amz-Grant-Write-Acp", "X-Amz-Grant-Full-Control"} {
		if r.Header.Get(hdr) != "" {
			return ErrorMessage(ErrNotImplemented, "Only the private ACL is supported.")
		}
	}
	if canned := r.Header.Get("X-Amz-Acl"); canned != "" {
		switch {
		case canned == "private":
			return nil
		case cannedACLs[canned]:
			return ErrorMessage(ErrNotImplemented, "Only the private ACL is supported.")
		default:
			return ErrorInvalidArgument("x-amz-acl", canned, "")
		}
	}
	var acl AccessControlPolicy
	if err := g.xmlDecodeBody(r.Body, &acl); err != nil {
		return err
	}
	if len(acl.Grants) != 1 ||
		acl.Grants[0].Grantee.ID != defaultOwner.ID ||
		acl.Grants[0].Permission != "FULL_CONTROL" {
		return ErrorMessage(ErrNotImplemented, "Only the private ACL is supported.")
	}
	return nil
}

// routeACL handles the ?acl sub-resource of a bucket or, if object
// isn't empty, an object. Every bucket and object has the default ACL,
// which is the only one which can be set.
func (g *GoFakeS3) routeACL(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	if object != "" {
		obj, err := g.storage.HeadObject(r.Context(), bucket, object)
		if err != nil {
			return err
		}
		if obj.Contents != nil {
			_ = obj.Contents.Close()
		}
	}
	switch r.Method {
	case "GET":
		return g.xmlEncoder(w).Encode(defaultACL())
	case "PUT":
		return g.putACL(r)
	default:
		return ErrMethodNotAllowed
	}
}

// notConfigured are the errors S3 returns for a GET of a bucket
// sub-resource which hasn't been configured. A DELETE of one of these
// succeeds as there is nothing to delete.
var notConfigured = map[string]ErrorCode{
	"cors":              ErrNoSuchCORSConfiguration,
	"encryption":        ErrServerSideEncryptionConfigurationNotFound,
	"lifecycle":         ErrNoSuchLifecycleConfiguration,
	"object-lock":       ErrObjectLockConfigurationNotFound,
	"ownershipControls": ErrOwnershipControlsNotFound,
	"policy":            ErrNoSuchBucketPolicy,
	"publicAccessBlock": ErrNoSuchPublicAccessBlockConfiguration,
	"replication":       ErrReplicationConfigurationNotFound,
	"tagging":           ErrNoSuchTagSet,
	"website":           ErrNoSuchWebsiteConfiguration,
}

// policyStatus is the response to GetBucketPolicyStatus.
type policyStatus struct {
	XMLName  xml.Name `xml:"PolicyStatus"`
	Xmlns    string   `xml:"xmlns,attr"`
	IsPublic bool     `xml:"IsPublic"`
}

// routeUnsupportedStub answers requests for some sub-resources which
// aren't supported as S3 does for a bucket or object without that
// feature configured. It returns ErrNotImplemented if it doesn't
// handle the request.
func (g *GoFakeS3) routeUnsupportedStub(bucket, object, sub string, w http.ResponseWriter, r *http.Request) error {
	if sub == "acl" {
		return g.routeACL(bucket, object, w, r)
	}
	if object != "" {
		if sub == "retention" || sub == "legal-hold" {
			return ErrorMessage(ErrInvalidRequest, "Bucket is missing Object Lock Configuration")
		}
		return ErrNotImplemented
	}
	switch {
	case sub == "policyStatus" && r.Method == "GET":
		g.mu.RLock()
		haveAuth := len(g.v4AuthPair) > 0
		g.mu.RUnlock()
		return g.xmlEncoder(w).Encode(policyStatus{
			Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
			IsPublic: !haveAuth,
		})
	case sub == "object-lock" && r.Method == "PUT":
		return ErrorMessage(ErrInvalidBucketState, "Object Lock configuration cannot be enabled on existing buckets")
	case notConfigured[sub] != "" && r.Method == "GET":
		return ResourceError(notConfigured[sub], bucket)
	case notConfigured[sub] != "" && r.Method == "DELETE" && !strings.EqualFold(sub, "object-lock"):
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	return ErrNotImplemented
}

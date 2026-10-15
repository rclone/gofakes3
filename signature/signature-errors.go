package signature

import (
	"bytes"
	"net/http"

	xml "github.com/minio/xxml"
)

// ErrorCode is code[int] of APIError
type ErrorCode int

// APIError is API Error structure
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// the format of error response
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string
	Message   string
	RequestID string `xml:"RequestId,omitempty"`
}

type errorCodeMap map[ErrorCode]APIError

const (
	errMissingFields ErrorCode = iota
	errMissingCredTag
	errCredMalformed
	errInvalidAccessKeyID
	errMalformedCredentialDate
	errInvalidRequestVersion
	errInvalidServiceS3
	errMissingSignHeadersTag
	errMissingSignTag
	errUnsignedHeaders
	errMissingDateHeader
	errMalformedDate
	errMalformedExpires
	errUnsupportAlgorithm
	errSignatureDoesNotMatch
	errExpiredRequest
	errAnonymous
	errInvalidExpires
	errRequestNotYetValid
	errRequestTimeTooSkewed

	// ErrNone is None(err=nil)
	ErrNone
)

// error code to APIerror structure, these fields carry respective
// descriptions for all the error responses.
var errorCodes = errorCodeMap{
	errMissingFields: {
		Code:           "MissingFields",
		Description:    "Missing fields in request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errMissingCredTag: {
		Code:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errCredMalformed: {
		Code:           "AuthorizationQueryParameterserror",
		Description:    "error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errInvalidAccessKeyID: {
		Code:           "InvalidAccessKeyId",
		Description:    "The Access Key Id you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},
	errMalformedCredentialDate: {
		Code:           "AuthorizationQueryParameterserror",
		Description:    "error parsing the X-Amz-Credential parameter; incorrect date format. This date in the credential must be in the format \"yyyyMMdd\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errInvalidRequestVersion: {
		Code:           "AuthorizationQueryParameterserror",
		Description:    "error parsing the X-Amz-Credential parameter; incorrect terminal. This endpoint uses \"aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errInvalidServiceS3: {
		Code:           "AuthorizationParameterserror",
		Description:    "error parsing the Credential/X-Amz-Credential parameter; incorrect service. This endpoint belongs to \"s3\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errMissingSignHeadersTag: {
		Code:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errMissingSignTag: {
		Code:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errUnsignedHeaders: {
		Code:           "AccessDenied",
		Description:    "There were headers present in the request which were not signed",
		HTTPStatusCode: http.StatusForbidden,
	},
	errMissingDateHeader: {
		Code:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header",
		HTTPStatusCode: http.StatusForbidden,
	},
	errMalformedDate: {
		Code:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errMalformedExpires: {
		Code:           "MalformedExpires",
		Description:    "Invalid X-Amz-Expires, expected to be a number",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errUnsupportAlgorithm: {
		Code:           "UnsupportedAlgorithm",
		Description:    "Encountered an unsupported algorithm.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	errSignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HTTPStatusCode: http.StatusForbidden,
	},
	errExpiredRequest: {
		Code:           "AccessDenied",
		Description:    "Request has expired",
		HTTPStatusCode: http.StatusForbidden,
	},
	errAnonymous: {
		Code:           "AccessDenied",
		Description:    "Access Denied",
		HTTPStatusCode: http.StatusForbidden,
	},
	errInvalidExpires: {
		Code:           "AccessDenied",
		Description:    "X-Amz-Expires must be between 1 and 604800 seconds",
		HTTPStatusCode: http.StatusForbidden,
	},
	errRequestNotYetValid: {
		Code:           "AccessDenied",
		Description:    "Request is not valid yet",
		HTTPStatusCode: http.StatusForbidden,
	},
	errRequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the current time is too large.",
		HTTPStatusCode: http.StatusForbidden,
	},
}

// EncodeResponse encodes the response headers into XML format.
func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	buf, err := xml.Marshal(response)
	if err != nil {
		return nil
	}
	bytesBuffer.Write(buf)
	return bytesBuffer.Bytes()
}

// GetAPIError decodes Signerror[int] to APIerror[encodable]
func GetAPIError(errCode ErrorCode) APIError {
	return errorCodes[errCode]
}

// EncodeAPIErrorToResponse encodes APIerror to http response
func EncodeAPIErrorToResponse(err APIError) []byte {
	return EncodeAPIErrorToResponseWithRequestID(err, "")
}

// EncodeAPIErrorToResponseWithRequestID encodes APIerror to http
// response including the request ID if it isn't empty.
func EncodeAPIErrorToResponseWithRequestID(err APIError, requestID string) []byte {
	return EncodeResponse(errorResponse{
		Code:      err.Code,
		Message:   err.Description,
		RequestID: requestID,
	})
}

package gofakes3

import (
	"net/http"
	"strings"
)

var (
	corsHeaders = []string{
		"Accept",
		"Accept-Encoding",
		"Authorization",
		"Content-Disposition",
		"Content-Length",
		"Content-Type",
		"X-Amz-Date",
		"X-Amz-User-Agent",
		"X-CSRF-Token",
		"x-amz-acl",
		"x-amz-content-sha256",
		"x-amz-meta-filename",
		"x-amz-meta-from",
		"x-amz-meta-private",
		"x-amz-meta-to",
		"x-amz-security-token",
	}
	corsHeadersString = strings.Join(corsHeaders, ", ")
)

// withCORS adds the CORS headers to responses and answers CORS
// preflight (OPTIONS) requests.
//
// Browsers don't authenticate preflight requests, so this must be
// outside the authentication.
type withCORS struct {
	r http.Handler
	g *GoFakeS3
}

func (s *withCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, HEAD")
	w.Header().Set("Access-Control-Allow-Headers", corsHeadersString)
	w.Header().Set("Access-Control-Expose-Headers", "ETag")

	if r.Method == "OPTIONS" {
		if r.Header.Get("Origin") == "" || r.Header.Get("Access-Control-Request-Method") == "" {
			s.g.httpError(w, r, ErrorMessage(ErrBadRequest, "Insufficient information. Origin request header needed."))
		}
		return
	}

	s.r.ServeHTTP(w, r)
}

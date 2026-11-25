// Package s3 implements a fake s3 server for rclone
package gofakes3

// From: minio/cmd/api-utils.go
// License: AGPL-3.0

func shouldEscape(c byte) bool {
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '/', '*':
		return false
	}
	return true
}

// URLEncode encodes s for a listing with encoding-type=url, as S3
// does. This is like Go's url.QueryEscape() except that '/' and '*'
// aren't encoded, '~' is, and ' ' is encoded as %20 not '+'.
func URLEncode(s string) string {
	hexCount := 0
	for i := 0; i < len(s); i++ {
		if shouldEscape(s[i]) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}

package gofakes3_test

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	xml "github.com/minio/xxml"

	"github.com/rclone/gofakes3"
)

// postXML POSTs body to rqpath with the query string query, returning the
// response.
func (ts *testServer) postXML(rqpath, query string, body []byte) *http.Response {
	ts.Helper()
	rq, err := http.NewRequest(http.MethodPost, ts.url(rqpath)+"?"+query, bytes.NewReader(body))
	ts.OK(err)
	rq.Header.Set("Content-Type", "application/xml")
	res, err := httpClient().Do(rq)
	ts.OK(err)
	return res
}

// assertErrorResponse checks res is an S3 error response with code.
func (ts *testServer) assertErrorResponse(res *http.Response, code gofakes3.ErrorCode) {
	ts.Helper()
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != code.Status() {
		ts.Fatal("bad status", res.StatusCode, "!=", code.Status())
	}
	var errResp gofakes3.ErrorResponse
	ts.OK(xml.NewDecoder(res.Body).Decode(&errResp))
	if errResp.Code != code {
		ts.Fatal("bad code", errResp.Code, "!=", code)
	}
}

// oversizedXML returns an XML body with the root element root which is
// bigger than gofakes3.MaxXMLBodySize.
func oversizedXML(root string) []byte {
	var b bytes.Buffer
	b.WriteString("<" + root + ">")
	b.WriteString("<!--")
	b.Write(bytes.Repeat([]byte("x"), gofakes3.MaxXMLBodySize))
	b.WriteString("-->")
	b.WriteString("</" + root + ">")
	return b.Bytes()
}

func TestXMLBodyTooLarge(t *testing.T) {
	t.Run("delete-objects", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		res := ts.postXML("/"+defaultBucket, "delete", oversizedXML("Delete"))
		ts.assertErrorResponse(res, gofakes3.ErrMaxMessageLengthExceeded)
	})

	t.Run("complete-multipart-upload", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		uploadID := ts.createMultipartUpload(defaultBucket, "object", nil)
		res := ts.postXML("/"+defaultBucket+"/object", "uploadId="+uploadID, oversizedXML("CompleteMultipartUpload"))
		ts.assertErrorResponse(res, gofakes3.ErrMaxMessageLengthExceeded)
	})
}

// deleteXML returns a DeleteObjects body for n keys.
func deleteXML(n int) []byte {
	var b bytes.Buffer
	b.WriteString("<Delete>")
	for i := range n {
		fmt.Fprintf(&b, "<Object><Key>key%d</Key></Object>", i)
	}
	b.WriteString("</Delete>")
	return b.Bytes()
}

func TestDeleteMultiTooManyKeys(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	res := ts.postXML("/"+defaultBucket, "delete", deleteXML(gofakes3.MaxDeleteObjects+1))
	ts.assertErrorResponse(res, gofakes3.ErrMalformedXML)

	res = ts.postXML("/"+defaultBucket, "delete", deleteXML(gofakes3.MaxDeleteObjects))
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		ts.Fatal("bad status", res.StatusCode)
	}
}

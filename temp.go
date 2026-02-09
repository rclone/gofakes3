package gofakes3

import (
	"bytes"
	"context"
	"io"
)

type MultipartBackend interface {
	New(bucket, object string, partNumber int, size int64, expectedMD5 []byte) (UploadPart, error)
}

type UploadPart interface {
	Reader(context.Context) io.ReadCloser
	Writer(context.Context) io.WriteCloser
	Cleanup(context.Context)
}

func NewMultipartBackendInMemory() MultipartBackend {
	return &multipartBackendInMemory{}
}

type multipartBackendInMemory struct{}

// New implements TempBlobFactory.
func (m *multipartBackendInMemory) New(bucket, object string, partNumber int, size int64, expectedMD5 []byte) (UploadPart, error) {
	return &memoryTempBlob{
		buf:         bytes.NewBuffer(make([]byte, 0, size)),
		expectedMD5: expectedMD5,
	}, nil
}

type memoryTempBlob struct {
	buf         *bytes.Buffer
	expectedMD5 []byte
}

// Cleanup implements TempBlob.
func (m *memoryTempBlob) Cleanup(context.Context) {}

// Reader implements TempBlob.
func (m *memoryTempBlob) Reader(context.Context) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(m.buf.Bytes()))
}

// Writer implements TempBlob.
func (m *memoryTempBlob) Writer(context.Context) io.WriteCloser { return &nopWriteCloser{m.buf} }

type nopWriteCloser struct {
	io.Writer
}

func (wc *nopWriteCloser) Close() error { return nil }

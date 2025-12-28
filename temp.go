package gofakes3

import (
	"bytes"
	"io"
)

type TempBlobFactory interface {
	New(bucket, object string, partNumber int, size int64, expectedMD5 []byte) TempBlob
}

type TempBlob interface {
	Reader() io.ReadCloser
	Writer() io.WriteCloser
	Cleanup()
}

func newMemoryTempBlobFactory() TempBlobFactory {
	return &memoryTempBlobFactory{}
}

type memoryTempBlobFactory struct{}

// New implements TempBlobFactory.
func (m *memoryTempBlobFactory) New(bucket, object string, partNumber int, size int64, epectedMD5 []byte) TempBlob {
	return &memoryTempBlob{
		buf: bytes.NewBuffer(make([]byte, 0, size)),
	}
}

type memoryTempBlob struct {
	buf *bytes.Buffer
}

// Cleanup implements TempBlob.
func (m *memoryTempBlob) Cleanup() {}

// Reader implements TempBlob.
func (m *memoryTempBlob) Reader() io.ReadCloser { return io.NopCloser(bytes.NewReader(m.buf.Bytes())) }

// Writer implements TempBlob.
func (m *memoryTempBlob) Writer() io.WriteCloser { return &nopWriteCloser{m.buf} }

type nopWriteCloser struct {
	io.Writer
}

func (wc *nopWriteCloser) Close() error { return nil }

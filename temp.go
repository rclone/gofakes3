package gofakes3

import (
	"bytes"
	"context"
	"io"
)

type TempBlobFactory interface {
	New(bucket, object string, partNumber int, size int64, expectedMD5 []byte) (TempBlob, error)
}

type TempBlob interface {
	Reader(context.Context) io.ReadCloser
	Writer(context.Context) io.WriteCloser
	Cleanup(context.Context)
}

func newMemoryTempBlobFactory() TempBlobFactory {
	return &memoryTempBlobFactory{}
}

type memoryTempBlobFactory struct{}

// New implements TempBlobFactory.
func (m *memoryTempBlobFactory) New(bucket, object string, partNumber int, size int64, epectedMD5 []byte) (TempBlob, error) {
	return &memoryTempBlob{
		buf: bytes.NewBuffer(make([]byte, 0, size)),
	}, nil
}

type memoryTempBlob struct {
	buf *bytes.Buffer
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

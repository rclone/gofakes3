package gofakes3

import (
	"io"
	"strconv"
)

func parseClampedInt(in string, defaultValue, min, max int64) (int64, error) {
	var v int64
	if in == "" {
		v = defaultValue
	} else {
		var err error
		v, err = strconv.ParseInt(in, 10, 0)
		if err != nil {
			return defaultValue, ErrInvalidArgument
		}
	}

	if v < min {
		v = min
	} else if v > max {
		v = max
	}

	return v, nil
}

// ReadAll is a fakeS3-centric replacement for io.ReadAll(), for use when
// the size of the result is known ahead of time. It is considerably faster to
// preallocate the entire slice than to allow growslice to be triggered
// repeatedly, especially with larger buffers.
//
// It also reports S3-specific errors in certain conditions, like
// ErrIncompleteBody.
func ReadAll(r io.Reader, size int64) (b []byte, err error) {
	var n int
	b = make([]byte, size)
	n, err = io.ReadFull(r, b)
	if err == io.ErrUnexpectedEOF {
		return nil, ErrIncompleteBody
	} else if err != nil {
		return nil, err
	}

	if n != int(size) {
		return nil, ErrIncompleteBody
	}

	if extra, err := io.ReadAll(r); err != nil {
		return nil, err
	} else if len(extra) > 0 {
		return nil, ErrIncompleteBody
	}

	return b, nil
}

type sizeCheckerReader struct {
	inner    io.Reader
	expected int64
	read     int64
}

func newSizeCheckerReader(inner io.Reader, expected int64) *sizeCheckerReader {
	return &sizeCheckerReader{
		inner:    inner,
		expected: expected,
	}
}

func (s *sizeCheckerReader) Read(p []byte) (n int, err error) {
	n, err = s.inner.Read(p)
	s.read += int64(n)

	if err == io.EOF && s.read != s.expected {
		return n, ErrIncompleteBody
	}
	if err == io.ErrUnexpectedEOF {
		return n, ErrIncompleteBody
	}

	return n, err
}

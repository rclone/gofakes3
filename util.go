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

// readAllPrealloc is the most ReadAll allocates before the data arrives.
const readAllPrealloc = 1024 * 1024

// ReadAll is a fakeS3-centric replacement for io.ReadAll(), for use when
// the size of the result is known ahead of time. The result has exactly
// size bytes of capacity, avoiding the waste of io.ReadAll's growth.
//
// As size usually comes from a request header it isn't trusted: at most
// readAllPrealloc is allocated up front, and the buffer only grows beyond
// that as the data arrives. A negative size gives ErrInvalidArgument.
//
// It also reports S3-specific errors in certain conditions, like
// ErrIncompleteBody.
func ReadAll(r io.Reader, size int64) (b []byte, err error) {
	if size < 0 {
		return nil, ErrInvalidArgument
	}
	b = make([]byte, 0, min(size, readAllPrealloc))
	for int64(len(b)) < size {
		if len(b) == cap(b) {
			b = append(make([]byte, 0, min(size, 2*int64(cap(b)))), b...)
		}
		var n int
		n, err = r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err == io.EOF {
			if int64(len(b)) == size {
				break
			}
			return nil, ErrIncompleteBody
		} else if err != nil {
			return nil, err
		}
	}

	if extra, err := io.ReadAll(r); err != nil {
		return nil, err
	} else if len(extra) > 0 {
		return nil, ErrIncompleteBody
	}

	return b, nil
}

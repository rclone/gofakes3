package gofakes3

import (
	"bufio"
	"fmt"
	"io"
)

type chunkedReader struct {
	inner         *bufio.Reader
	chunkRemain   int
	notFirstChunk bool
}

func newChunkedReader(inner io.Reader) *chunkedReader {
	return &chunkedReader{
		inner:         bufio.NewReader(inner),
		chunkRemain:   0,
		notFirstChunk: false,
	}
}

func (r *chunkedReader) Read(p []byte) (n int, err error) {
	sizeToRead := len(p)
	for sizeToRead > 0 {
		if r.chunkRemain > 0 {
			// read until this chunk or sizeToRead ends
			bytesToRead := sizeToRead
			if sizeToRead > r.chunkRemain {
				bytesToRead = r.chunkRemain
			}
			innerN, err := r.inner.Read(p[n : n+bytesToRead])
			r.chunkRemain -= innerN
			sizeToRead -= innerN
			n += innerN
			if err != nil {
				return n, err
			}
		} else {
			if !r.notFirstChunk {
				// Is first chunk.
				r.notFirstChunk = true
			} else {
				// skip last chunk's b"\r\n"
				_, err = io.CopyN(io.Discard, r.inner, 2)
				if err != nil {
					return n, err
				}
			}
			// read next chunk header
			chunkSize := 0
			_, err = fmt.Fscanf(r.inner, "%x", &chunkSize)
			if err != nil {
				return n, err
			}
			r.chunkRemain = chunkSize
			if err := discardLine(r.inner); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// discardLine discards bytes up to and including '\n'.
func discardLine(r *bufio.Reader) error {
	_, err := r.ReadSlice('\n')
	return err
}

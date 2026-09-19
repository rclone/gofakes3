package gofakes3

import (
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
)

func TestParseClampedIntValid(t *testing.T) {
	for _, tc := range []struct {
		in             string
		dflt, min, max int64
		out            int64
	}{
		{in: "", dflt: 1, min: 0, max: 1, out: 1},
		{in: "", dflt: 2, min: 0, max: 1, out: 1},
		{in: "1", dflt: 2, min: 0, max: 100, out: 1},
		{in: "1", dflt: 0, min: 2, max: 100, out: 2},
		{in: "1000", dflt: 0, min: 2, max: 100, out: 100},
	} {
		t.Run("", func(t *testing.T) {
			result, err := parseClampedInt(tc.in, tc.dflt, tc.min, tc.max)
			if err != nil {
				t.Fatal(err)
			}
			if result != tc.out {
				t.Fatal(result, "!=", tc.out)
			}
		})
	}
}

func TestReadAll(t *testing.T) {
	t.Run("simple-read", func(t *testing.T) {
		tt := TT{t}
		b, err := ReadAll(strings.NewReader("test"), 4)
		tt.OK(err)
		if string(b) != "test" {
			t.Fatal(string(b), "!=", "test")
		}
	})

	t.Run("data-with-eof", func(t *testing.T) {
		tt := TT{t}
		b, err := ReadAll(iotest.DataErrReader(strings.NewReader("test")), 4)
		tt.OK(err)
		if string(b) != "test" {
			t.Fatal(string(b), "!=", "test")
		}
	})

	t.Run("empty-input", func(t *testing.T) {
		tt := TT{t}
		b, err := ReadAll(strings.NewReader(""), 0)
		tt.OK(err)
		if string(b) != "" {
			t.Fatal(string(b), "!=", "")
		}
	})

	t.Run("size-too-large", func(t *testing.T) {
		_, err := ReadAll(strings.NewReader("test"), 5)
		if !HasErrorCode(err, ErrIncompleteBody) {
			t.Fatal("expected ErrIncompleteBody, found", err)
		}
	})

	t.Run("size-too-small", func(t *testing.T) {
		_, err := ReadAll(strings.NewReader("test"), 3)
		if !HasErrorCode(err, ErrIncompleteBody) {
			t.Fatal("expected ErrIncompleteBody, found", err)
		}
	})

	t.Run("bigger-than-first-allocation", func(t *testing.T) {
		tt := TT{t}
		in := strings.Repeat("0123456789", readAllPrealloc/4)
		b, err := ReadAll(strings.NewReader(in), int64(len(in)))
		tt.OK(err)
		if string(b) != in {
			t.Fatal("read back different data")
		}
		if cap(b) != len(in) {
			t.Fatal("capacity", cap(b), "!=", len(in))
		}
	})

	// The size usually comes from a request header, so must not be
	// allocated before the data arrives.
	t.Run("huge-declared-size", func(t *testing.T) {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err := ReadAll(strings.NewReader("test"), 1<<62)
		runtime.ReadMemStats(&after)
		if !HasErrorCode(err, ErrIncompleteBody) {
			t.Fatal("expected ErrIncompleteBody, found", err)
		}
		if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 2*readAllPrealloc {
			t.Fatal("allocated", allocated, "bytes for a 4 byte body")
		}
	})

	t.Run("negative-size", func(t *testing.T) {
		_, err := ReadAll(strings.NewReader("test"), -1)
		if !HasErrorCode(err, ErrInvalidArgument) {
			t.Fatal("expected ErrInvalidArgument, found", err)
		}
	})
}

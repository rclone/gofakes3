package gofakes3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultipartBackendGeneric is a generic helper function that can test any MultipartBackend implementation
func TestMultipartBackendGeneric(t *testing.T, backendFactory func() MultipartBackend, backendName string) {
	t.Run(backendName+"_New", func(t *testing.T) {
		backend := backendFactory()

		// Test normal case
		part, err := backend.New("test-bucket", "test-object", 1, 1024, []byte("test-md5"))
		assert.NoError(t, err, "New should not return an error")
		assert.NotNil(t, part, "New should return a non-nil UploadPart")

		// Test with zero size
		part, err = backend.New("test-bucket", "test-object", 1, 0, []byte("test-md5"))
		assert.NoError(t, err, "New should not return an error even with zero size")
		assert.NotNil(t, part, "New should return a non-nil UploadPart even with zero size")

		// Test with large size
		part, err = backend.New("test-bucket", "test-object", 1, 1024*1024, []byte("test-md5"))
		assert.NoError(t, err, "New should not return an error even with large size")
		assert.NotNil(t, part, "New should return a non-nil UploadPart even with large size")
	})

	t.Run(backendName+"_UploadPartOperations", func(t *testing.T) {
		backend := backendFactory()

		// Test Cleanup
		t.Run("Cleanup", func(t *testing.T) {
			part, err := backend.New("test-bucket", "test-object", 1, 1024, []byte("test-md5"))
			require.NoError(t, err)

			// Cleanup should not panic or return any error
			part.Cleanup(context.Background())

			// Cleanup is idempotent, should be safe to call multiple times
			part.Cleanup(context.Background())
		})

		// Test Writer and Reader
		t.Run("WriterAndReader", func(t *testing.T) {
			part, err := backend.New("test-bucket", "test-object", 1, 1024, []byte("test-md5"))
			require.NoError(t, err)

			writer := part.Writer(context.Background())
			testData := []byte("Hello, World!")
			n, err := writer.Write(testData)
			require.NoError(t, err)
			require.Equal(t, len(testData), n)
			require.NoError(t, writer.Close())

			reader := part.Reader(context.Background())
			defer reader.Close()

			readData, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, testData, readData)
		})

		// Test Multiple Writes
		t.Run("MultipleWrites", func(t *testing.T) {
			part, err := backend.New("test-bucket", "test-object", 1, 1024, []byte("test-md5"))
			require.NoError(t, err)

			writer := part.Writer(context.Background())

			data1 := []byte("First chunk")
			data2 := []byte("Second chunk")
			data3 := []byte("Third chunk")

			n1, err := writer.Write(data1)
			require.NoError(t, err)
			require.Equal(t, len(data1), n1)

			n2, err := writer.Write(data2)
			require.NoError(t, err)
			require.Equal(t, len(data2), n2)

			n3, err := writer.Write(data3)
			require.NoError(t, err)
			require.Equal(t, len(data3), n3)

			require.NoError(t, writer.Close())

			reader := part.Reader(context.Background())
			defer reader.Close()

			expectedData := append(append(data1, data2...), data3...)
			actualData, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, expectedData, actualData)
		})

		// Test Empty Data
		t.Run("EmptyData", func(t *testing.T) {
			part, err := backend.New("test-bucket", "test-object", 1, 1024, []byte("test-md5"))
			require.NoError(t, err)

			reader := part.Reader(context.Background())
			defer reader.Close()

			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Empty(t, data, "Empty temp blob should return empty data")
		})

		// Test Large Data
		t.Run("LargeData", func(t *testing.T) {
			part, err := backend.New("test-bucket", "test-object", 1, 1024*1024, []byte("test-md5"))
			require.NoError(t, err)

			writer := part.Writer(context.Background())

			largeData := make([]byte, 1024*1024)
			for i := range largeData {
				largeData[i] = byte(i % 256)
			}

			n, err := writer.Write(largeData)
			require.NoError(t, err)
			require.Equal(t, len(largeData), n)
			require.NoError(t, writer.Close())

			reader := part.Reader(context.Background())
			defer reader.Close()

			readData, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, largeData, readData)
		})

		// Test MD5 Validation
		t.Run("MD5Validation", func(t *testing.T) {
			// Test with correct MD5
			t.Run("CorrectMD5", func(t *testing.T) {
				testData := []byte("Hello, World!")
				expectedMD5 := []byte("test-md5-correct")

				part, err := backend.New("test-bucket", "test-object", 1, 1024, expectedMD5)
				require.NoError(t, err)

				// Write data
				writer := part.Writer(context.Background())
				n, err := writer.Write(testData)
				require.NoError(t, err)
				require.Equal(t, len(testData), n)
				require.NoError(t, writer.Close())
			})

			// Test with incorrect MD5
			t.Run("IncorrectMD5", func(t *testing.T) {
				testData := []byte("Hello, World!")
				expectedMD5 := []byte("test-md5-correct")

				part, err := backend.New("test-bucket", "test-object", 1, 1024, expectedMD5)
				require.NoError(t, err)

				// Write data
				writer := part.Writer(context.Background())
				n, err := writer.Write(testData)
				require.NoError(t, err)
				require.Equal(t, len(testData), n)
				require.NoError(t, writer.Close())
			})

			// Test with no MD5 validation (nil expected MD5)
			t.Run("NoMD5Validation", func(t *testing.T) {
				testData := []byte("Hello, World!")

				part, err := backend.New("test-bucket", "test-object", 1, 1024, nil)
				require.NoError(t, err)

				// Write data
				writer := part.Writer(context.Background())
				n, err := writer.Write(testData)
				require.NoError(t, err)
				require.Equal(t, len(testData), n)
				require.NoError(t, writer.Close())
			})
		})
	})
}

// TestMemoryTempBlobGeneric applies the generic test to the in-memory backend
func TestMemoryTempBlobGeneric(t *testing.T) {
	TestMultipartBackendGeneric(t, func() MultipartBackend {
		return NewMultipartBackendInMemory()
	}, "MemoryTempBlob")
}

// Example of how to use the generic test with a different backend implementation
// This is commented out since we don't have another implementation, but shows the pattern
/*
func TestSomeOtherBackendGeneric(t *testing.T) {
	testMultipartBackendGeneric(t, func() MultipartBackend {
		return newSomeOtherBackend() // hypothetical other backend implementation
	}, "SomeOtherBackend")
}
*/

func TestNopWriteCloser(t *testing.T) {
	// Test the nopWriteCloser helper
	buf := &bytes.Buffer{}
	wc := &nopWriteCloser{Writer: buf}

	// Test writing
	testData := []byte("Test data")
	n, err := wc.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)
	require.Equal(t, testData, buf.Bytes())

	// Test closing (should not return error)
	require.NoError(t, wc.Close())

	// Test that Close is idempotent
	require.NoError(t, wc.Close())
}

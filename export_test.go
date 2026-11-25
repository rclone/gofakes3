package gofakes3

// SetMaxPOSTBodySize sets the largest browser upload body accepted,
// returning a function to restore it.
func SetMaxPOSTBodySize(size int64) (restore func()) {
	old := maxPOSTBodySize
	maxPOSTBodySize = size
	return func() { maxPOSTBodySize = old }
}

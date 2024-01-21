package streamcache

// internal_test.go contains functions that
// expose internal state for testing.

// ReaderOffset returns the current offset of the reader.
func ReaderOffset(r *Reader) int {
	return r.offset
}

// CacheInternal returns c's internal cache byte slice.
func CacheInternal(c *Cache) []byte {
	return c.cache
}

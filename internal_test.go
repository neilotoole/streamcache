package streamcache

// internal_test.go contains functions that
// expose internal state for testing.

// ReaderOffset returns the current offset of the reader.
func ReaderOffset(r *Reader) int {
	return r.offset
}

// CacheData returns c's internal cache byte slice.
func CacheData(c *Cache) []byte {
	return c.cache
}

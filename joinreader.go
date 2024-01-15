package streamcache

import "io"

var _ io.Reader = (*joinReader)(nil)

// joinReader is an io.Reader whose Read method reads from the
// concatenation of the unreadCache, and the origin reader. Effectively,
// joinReader is a lightweight io.MultiReader.
type joinReader struct {
	src         *Source
	unreadCache []byte
	origin      io.Reader
}

// Read implements io.Reader.
func (jr *joinReader) Read(p []byte) (n int, err error) {
	cacheLen := len(jr.unreadCache)
	if cacheLen == 0 {
		// The cache is empty, so we read from the origin.
		n, err = jr.origin.Read(p)
		jr.src.mu.Lock()
		jr.src.size += n
		jr.src.mu.Unlock()
		return n, err
	}

	if len(p) <= cacheLen {
		// The read can be satisfied entirely from the cache.
		n = copy(p, jr.unreadCache)
		jr.unreadCache = jr.unreadCache[n:]
		return n, nil
	}

	// The read will be partially from the cache, with the remainder
	// coming from the origin.
	copy(p, jr.unreadCache)
	n, err = jr.origin.Read(p[cacheLen:])
	jr.src.mu.Lock()
	jr.src.size += n
	jr.src.mu.Unlock()
	n += cacheLen
	jr.unreadCache = nil
	return n, err
}

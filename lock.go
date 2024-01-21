package streamcache

func (c *Cache) readLock(r *Reader) {
	logf(r, "read lock: before")
	c.cMu.RLock()
	logf(r, "read lock: after")
}

func (c *Cache) readUnlock(r *Reader) {
	logf(r, "read unlock: before")
	c.cMu.RUnlock()
	logf(r, "read unlock: after")
}

func (c *Cache) readTryLock(r *Reader) bool {
	logf(r, "try read lock: before")
	ok := c.cMu.TryRLock()
	logf(r, "try read lock: after: %v", ok)
	return ok
}

func (c *Cache) writeLock(r *Reader) {
	logf(r, "write lock: before")
	c.cMu.Lock()
	logf(r, "write lock: after")
}

func (c *Cache) writeTryLock(r *Reader) bool { //nolint:unused
	logf(r, "try write lock: before")
	ok := c.cMu.TryLock()
	logf(r, "try write lock: after: %v", ok)
	return ok
}

func (c *Cache) writeUnlock(r *Reader) {
	logf(r, "write unlock: before")
	c.cMu.Unlock()
	logf(r, "write unlock: after")
}

func (c *Cache) srcLock(r *Reader) {
	logf(r, "src lock: before")
	c.srcMu.Lock()
	logf(r, "src lock: after")
}

func (c *Cache) srcTryLock(r *Reader) bool {
	logf(r, "try src lock: before")
	ok := c.srcMu.TryLock()
	// ok := c.srcMu.TryLock()
	logf(r, "try src lock: after: %v", ok)
	return ok
}

func (c *Cache) srcUnlock(r *Reader) {
	logf(r, "src unlock: before")
	c.srcMu.Unlock()
	logf(r, "src unlock: after")
}

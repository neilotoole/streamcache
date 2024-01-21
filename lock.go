package streamcache

func (c *Cache) readLock(r *Reader) {
	c.Infof(r, "read lock: before")
	c.cMu.RLock()
	c.Infof(r, "read lock: after")
}

func (c *Cache) readUnlock(r *Reader) {
	c.Infof(r, "read unlock: before")
	c.cMu.RUnlock()
	c.Infof(r, "read unlock: after")
}

func (c *Cache) readTryLock(r *Reader) bool {
	c.Infof(r, "try read lock: before")
	ok := c.cMu.TryRLock()
	c.Infof(r, "try read lock: after: %v", ok)
	return ok
}

func (c *Cache) writeLock(r *Reader) {
	c.Infof(r, "write lock: before")
	c.cMu.Lock()
	c.Infof(r, "write lock: after")
}

func (c *Cache) writeTryLock(r *Reader) bool { //nolint:unused
	c.Infof(r, "try write lock: before")
	ok := c.cMu.TryLock()
	c.Infof(r, "try write lock: after: %v", ok)
	return ok
}

func (c *Cache) writeUnlock(r *Reader) {
	c.Infof(r, "write unlock: before")
	c.cMu.Unlock()
	c.Infof(r, "write unlock: after")
}

func (c *Cache) srcLock(r *Reader) {
	c.Infof(r, "src lock: before")
	c.srcMu.Lock()
	c.Infof(r, "src lock: after")
}

func (c *Cache) srcTryLock(r *Reader) bool {
	c.Infof(r, "try src lock: before")
	ok := c.srcMu.TryLock()
	// ok := c.srcMu.TryLock()
	c.Infof(r, "try src lock: after: %v", ok)
	return ok
}

func (c *Cache) srcUnlock(r *Reader) {
	c.Infof(r, "src unlock: before")
	c.srcMu.Unlock()
	c.Infof(r, "src unlock: after")
}

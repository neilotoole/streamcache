package streamcache

// FIXME: delete this lock functions before release.

func (s *Stream) readLock(r *Reader) {
	logf(r, "read lock: before")
	s.cMu.RLock()
	logf(r, "read lock: after")
}

func (s *Stream) readUnlock(r *Reader) {
	logf(r, "read unlock: before")
	s.cMu.RUnlock()
	logf(r, "read unlock: after")
}

func (s *Stream) readTryLock(r *Reader) bool {
	logf(r, "try read lock: before")
	ok := s.cMu.TryRLock()
	logf(r, "try read lock: after: %v", ok)
	return ok
}

func (s *Stream) writeLock(r *Reader) {
	logf(r, "write lock: before")
	s.cMu.Lock()
	logf(r, "write lock: after")
}

func (s *Stream) writeTryLock(r *Reader) bool { //nolint:unused
	logf(r, "try write lock: before")
	ok := s.cMu.TryLock()
	logf(r, "try write lock: after: %v", ok)
	return ok
}

func (s *Stream) writeUnlock(r *Reader) {
	logf(r, "write unlock: before")
	s.cMu.Unlock()
	logf(r, "write unlock: after")
}

func (s *Stream) srcLock(r *Reader) {
	logf(r, "src lock: before")
	s.srcMu.Lock()
	logf(r, "src lock: after")
}

func (s *Stream) srcTryLock(r *Reader) bool {
	logf(r, "try src lock: before")
	ok := s.srcMu.TryLock()
	// ok := c.srcMu.TryLock()
	logf(r, "try src lock: after: %v", ok)
	return ok
}

func (s *Stream) srcUnlock(r *Reader) {
	logf(r, "src unlock: before")
	s.srcMu.Unlock()
	logf(r, "src unlock: after")
}

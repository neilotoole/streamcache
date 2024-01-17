// Package streamcache addresses an arcane scenario: multiple readers
// want to sample the start of an input stream (from an io.Reader),
// which involves caching, but after the samplers are satisfied,
// there's no need to maintain that cache and its memory overhead
// for the remainder of the read.
//
// Package streamcache implements a cache  mechanism that allows
// multiple callers to read some or all of the contents of a
// source reader, while only reading from the source reader once.
//
// Let's say we're reading from stdin. For example:
//
//	$ cat myfile.ext | myprogram
//
// In this scenario, myprogram wants to detect the type of data
// in the file/pipe, and then print it out. That sampling could be done
// in a separate goroutine per sampler type. The input file could be,
// let's say, a CSV file, or a JSON file.
//
// The obvious approach is to inspect the first few lines of the
// input, and check if the input is either valid CSV, or valid JSON.
// After that process, let's say we want to dump out the entire contents
// of the input.
//
// Package streamcache provides a facility to create a Cache from an
// underlying io.Reader (os.Stdin in this scenario), and spawn multiple
// readers, each of which can operate independently, in their own
// goroutines if desired. The underlying source (again, os.Stdin in this
// scenario) will only be read from once, but its data is available to
// multiple readers, because that data is cached in memory.
//
// That is, until after Cache.Seal is invoked: when there's only one final
// reader left, the cache is discarded, and the final reader reads directly
// from the underlying source.
//
// The entrypoint to this package is streamcache.New, which returns a
// new Cache instance, from which readers can be created via Cache.NewReader.
package streamcache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"sync"
)

// ErrAlreadySealed is returned by Cache.NewReader and Cache.Seal if
// the source is already sealed.
var ErrAlreadySealed = errors.New("cache is already sealed")

// ErrAlreadyClosed is returned by Reader.Read if the reader is
// already closed.
var ErrAlreadyClosed = errors.New("reader is already closed")

// Cache mediates access to the bytes of an underlying source io.Reader.
// Multiple callers can invoke Cache.NewReader to obtain a Reader, each of
// which can read the full contents of the source reader. Note that the
// source is only read from once, and the returned bytes are cached
// in memory. After Cache.Seal is invoked and readers are closed, the
// final reader discards the cache and reads directly from source for
// the remaining bytes.
type Cache struct {
	// cMu guards concurrent access to Cache's fields and methods.
	// REVISIT: Why a pointer and not a value?
	cMu sync.RWMutex

	// srcMu guards concurrent access to reading from src.
	srcMu sync.Mutex

	logMu sync.Mutex // FIXME: delete

	// Because cMu is a read-write mutex and srcMu is a regular mutex,
	// there are effectively three locks that can be acquired. They
	// are referred to in the comments as read, write, and src locks.

	// src is the underlying reader from which bytes are read.
	src io.Reader

	// rdrs is the set of unclosed Reader instances created
	// by Cache.NewReader. When a Reader is closed, it is removed
	// from this slice.
	rdrs []*Reader

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// done is closed after the Cache is sealed and the last
	// reader is closed. See Cache.Done.
	done chan struct{}

	// readErr is the first (and only) error returned by src's
	// Read method. Once readErr has been set to a non-nil value,
	// src is never read from again.
	readErr error

	// readErrAt is the offset at which readErr occurred.
	readErrAt int

	// size is the count of bytes read from src.
	size int

	// cache holds the accumulated bytes read from src.
	// It is nilled when the final reader switches to readSrcDirect.
	cache []byte

	// trim is the number of bytes trimmed from the start of the cache.
	// This mechanism eliminates the need to cache bytes that
	// will never be read again.
	//
	// FIXME: implement cache trimming. Or don't. Re-slicing the cache
	// doesn't help save memory? Would we do a copy to a new slice?
	trim int //nolint:unused

	log *slog.Logger
}

// New returns a new Cache that reads from src. Use Cache.NewReader
// to create read from src.
func New(log *slog.Logger, src io.Reader) *Cache {
	c := &Cache{
		log:   log,
		src:   src,
		cache: make([]byte, 0),
		done:  make(chan struct{}),
	}

	return c
}

// NewReader returns a new Reader for Cache. If Cache is already
// sealed, ErrAlreadySealed is returned. If ctx is non-nil, it is
// checked for cancellation at the start of Reader.Read.
// It is the caller's responsibility to close the returned Reader.
func (c *Cache) NewReader(ctx context.Context) (*Reader, error) {
	c.cMu.Lock()
	defer c.cMu.Unlock()

	if c.sealed {
		return nil, ErrAlreadySealed
	}

	r := &Reader{
		ctx:    ctx,
		c:      c,
		readFn: c.readMain,
	}
	c.rdrs = append(c.rdrs, r)
	return r, nil
}

// readFunc is the type of the Reader.readFn field.
type readFunc func(r *Reader, p []byte, offset int) (n int, err error)

var (
	_ readFunc = (*Cache)(nil).readMain
	_ readFunc = (*Cache)(nil).readSrcDirect
)

// readSrcDirect reads directly from Cache.src. The src's size is
// incremented as bytes are read from src, and Cache.readErr is set if
// src returns an error.
func (c *Cache) readSrcDirect(_ *Reader, p []byte, _ int) (n int, err error) {
	n, err = c.src.Read(p)

	c.cMu.Lock()
	defer c.cMu.Unlock()
	c.size += n
	c.readErr = err
	return n, c.readErr
}

func (c *Cache) readLock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "read lock: before")
	c.cMu.RLock()
	c.Infof(r, log, slog.LevelDebug, "read lock: after")
}

func (c *Cache) readUnlock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "read unlock: before")
	c.cMu.RUnlock()
	c.Infof(r, log, slog.LevelDebug, "read unlock: after")
}

func (c *Cache) tryReadLock(r *Reader, log *slog.Logger) bool {
	c.Infof(r, log, slog.LevelDebug, "try read lock: before")
	ok := c.cMu.TryRLock()
	c.Infof(r, log, slog.LevelDebug, "try read lock: after: %v", ok)
	return ok
}

func (c *Cache) writeLock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "write lock: before")
	c.cMu.Lock()
	c.Infof(r, log, slog.LevelDebug, "write lock: after")
}

func (c *Cache) tryWriteLock(r *Reader, log *slog.Logger) bool { //nolint:unused
	c.Infof(r, log, slog.LevelDebug, "try write lock: before")
	ok := c.cMu.TryLock()
	c.Infof(r, log, slog.LevelDebug, "try write lock: after: %v", ok)
	return ok
}

func (c *Cache) writeUnlock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "write unlock: before")
	c.cMu.Unlock()
	c.Infof(r, log, slog.LevelDebug, "write unlock: after")
}

// https://www.reddit.com/r/golang/comments/m9b0yp/fastest_way_to_pick_uniformly_from_a_slice_from/

func (c *Cache) srcLock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "src lock: before")
	c.srcMu.Lock()
	c.Infof(r, log, slog.LevelDebug, "src lock: after")
}

func (c *Cache) trySrcLock(r *Reader, log *slog.Logger) bool {
	c.Infof(r, log, slog.LevelDebug, "try src lock: before")
	ok := c.srcMu.TryLock()
	c.Infof(r, log, slog.LevelDebug, "try src lock: after: %v", ok)
	return ok
}

func (c *Cache) srcUnlock(r *Reader, log *slog.Logger) {
	c.Infof(r, log, slog.LevelDebug, "src unlock: before")
	c.srcMu.Unlock()
	c.Infof(r, log, slog.LevelDebug, "src unlock: after")
}

// readMain reads from Cache.cache and/or Cache.src. If Cache is sealed
// and r is the final Reader, this method may switch r.readFn
// to Cache.readSrcDirect, such that the remaining reads occur
// directly against src, bypassing Cache.cache entirely.
func (c *Cache) readMain(r *Reader, p []byte, offset int) (n int, err error) {
	log := c.getLog(r)
TOP: // FIXME: do we still need TOP
	c.readLock(r, log)

	if c.sealed {
		switch len(c.rdrs) {
		case 0:
			// Should be impossible.
			panic("Cache is sealed but has zero readers")
		case 1:
			c.readUnlock(r, log)
			return c.readFinal(r, p, offset)
		default:
			//  There are multiple readers, so we continue with
			//  normal reading using the cache.
		}
	}

	//if end <= cacheLen {
	//	// The read can be satisfied entirely from the cache.
	//	n, err = c.fillFromCache(p, offset)
	//	c.readUnlock(log)
	//	return n, err
	//}
	if len(c.cache) > offset {
		n, err = c.fillFromCache(p, offset)
		c.readUnlock(r, log)
		return n, err
	}

	// We don't have enough in the cache to satisfy the read.
	// We're going to need to read from src.

	// First we give up the read lock.
	c.readUnlock(r, log)

	// And try to get the src lock.
	if !c.trySrcLock(r, log) {
		// We couldn't get the src lock. Another reader has src lock,
		// and could be blocked on io. But, if there's data in the
		// cache that could be returned to r, we do that now, so that r
		// can catch up to the current cache state while the other
		// reader holds src lock.

		if !c.tryReadLock(r, log) {
			// We couldn't get the read lock, probably because another
			// reader is updating the cache. Our strategy here is to kick r
			// back to the start of readMain, so that it can try again.
			// Maybe there's a better strategy?
			c.Infof(r, log, slog.LevelDebug, "try read lock failed; going back to TOP.")
			println("goto top")
			goto TOP
		}

		// We've got the read lock, let's see if there's any bytes
		// in the cache that can be returned.
		if len(c.cache) > offset {
			// Yup, there are bytes available in the cache. Return them.
			n, err = c.fillFromCache(p, offset)
			c.readUnlock(r, log)
			return n, err
		}

		// There's nothing in the cache for r to consume.
		// So, we need to get more data from src.

		// First we give up the read lock.
		c.readUnlock(r, log)

		// And now we acquire src lock, so that we can read from src.
		c.srcLock(r, log)
		//goto TOP

	}

	// FIXME: Should check for context cancellation after each lock?

	// If we've gotten this far, we have the src lock, but not the
	// read or write lock. We're here because there was nothing in
	// the cache for r the last time we checked. However, it's possible
	// that another reader has since updated the cache, so we check again.
	//c.readLock(log)
	if len(c.cache) > offset { // FIXME: use c.size or len(c.cache)?
		// There's some new stuff in the cache. Return it.
		n, err = c.fillFromCache(p, offset)

		//c.readUnlock(log)
		c.srcUnlock(r, log)
		return n, err
	}
	//c.readUnlock(log)

	// We're almost ready to read from src. Because that's a potentially
	// blocking operation, we first check context cancellation. After all,
	// it's possible that r has been waiting around for a while trying to
	// acquire the src lock, and the context has been canceled in the
	// meantime.
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			c.srcUnlock(r, log)
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	// OK, this time, we're REALLY going to read from src.

	c.Infof(r, log, slog.LevelInfo, "Entering src.Read")
	n, err = c.src.Read(p)
	c.Infof(r, log, slog.LevelInfo, "Returned from src.Read: n=%d, err=%v", n, err)

	if n == 0 && err == nil {
		// For this special case, there's no need to update the cache,
		// so we can just return now.
		c.srcUnlock(r, log)
		return 0, nil
	}

	// Now we need to update c, so we need to get the write lock.
	//fmt.Printf("waiting for write lock with [%d] bytes to return: %s\n", n, r.Name)
	c.writeLock(r, log)
	if err != nil {
		c.readErr = err
		c.readErrAt = offset + n
	}
	if n > 0 {
		c.size += n
		c.cache = append(c.cache, p[:n]...)
	}

	// We're done updating the cache, so we can release the write and src
	// locks, and return.
	c.writeUnlock(r, log)
	c.srcUnlock(r, log)
	runtime.Gosched()
	return n, err
}

// fillFromCache copies bytes from the cache to p, starting at offset.
// returning the number of bytes copied. If readErr is non-nil and we've
// reached the value of readErrAt, readErr is returned.
func (c *Cache) fillFromCache(p []byte, offset int) (n int, err error) {
	n = copy(p, c.cache[offset:])
	if c.readErr != nil && n+offset >= c.readErrAt {
		err = c.readErr
	}
	return n, err
}

// readFinal is invoked by Cache.readMain when the Cache is sealed
// and r is the final Reader. There are four possibilities for this read:
//
//  1. This read is entirely satisfied by the cache, with some
//     unread bytes still left in the cache. The next read
//     will still need to use the cache.
//  2. This read exactly matches the end of the cache, with no
//     unread bytes left in the cache. The subsequent read will be
//     directly against source and cache can be nilled.
//  3. The read offset aligns exactly with src's offset, thus this
//     read can be satisfied directly from src, as will all future
//     reads. We no longer need the cache.
//  4. The read is an overlap of the cache and src, so we need to
//     combine bytes from both. The subsequent read will be direct
//     from src and thus cache can be nilled.
func (c *Cache) readFinal(r *Reader, p []byte, offset int) (n int, err error) {
	log := c.getLog(r)

	end := offset + len(p)
	switch {
	case end < c.size:
		// The read can be satisfied entirely from the cache.
		// Subsequent reads could also be satisfied by the
		// cache, so we can't nil out c.cache yet.
		return c.fillFromCache(p, offset)
	case end == c.size:
		// The read is satisfied completely by the cache with
		// no unread cache bytes. Thus, we can nil out c.cache,
		// because the next read will be direct from src, and
		// the cache will never be used again.
		c.cache = nil
		r.readFn = c.readSrcDirect
		return c.fillFromCache(p, offset)
	case offset == c.size:
		// The read is entirely beyond what's cached, so we switch
		// to reading directly from src. We can nil out c.cache,
		// as it'll never be used again.
		c.cache = nil
		r.readFn = c.readSrcDirect

		// We don't need to get the src lock, because this is the final reader.
		n, err = c.src.Read(p)

		// Because we're updating c's fields, we need to get write lock.
		c.writeLock(r, log)
		c.size += n
		if err != nil {
			c.readErr = err
			c.readErrAt = offset + n
		}
		c.writeUnlock(r, log)
		return n, err
	case offset > c.size:
		// Should be impossible.
		panic("Offset is beyond end of cache")
	default:
		// This read is an overlap of cache and src.
	}

	// This read requires combining bytes from cache with new
	// bytes from src. First, copy the cache bytes.
	n, err = c.fillFromCache(p, offset)
	// Now that we've got what we need from the cache,
	// we can nil it out. It'll never be used again.
	c.cache = nil
	if err != nil {
		return n, err
	}

	// Next, read some more from src. We don't need to get src lock,
	// because this is the final reader.
	var n2 int
	n2, err = c.src.Read(p[n:])

	// Because we're updating c's fields, we need to get write lock.
	c.writeLock(r, c.getLog(r))
	c.size += n2
	n += n2
	if err != nil {
		c.readErr = err
		c.readErrAt = offset + n // REVISIT: is this correct?
	}
	c.writeUnlock(r, c.getLog(r))
	// Any subsequent reads will be direct from src.
	r.readFn = c.readSrcDirect
	return n, err
}

// Done returns a channel that is closed when the Cache is finished. This
// channel can be used to wait for work to complete. A Cache is considered
// finished after Seal has been invoked on it, and there are zero unclosed
// Reader instances remaining.
//
// Note that it's possible that a finished Cache has not closed its underlying
// source reader. For example, if a Cache is created and immediately sealed,
// the Cache is finished, but the underlying source reader was never closed.
// The source reader is closed only by closing the last Reader instance that
// was active after Seal was invoked.
func (c *Cache) Done() <-chan struct{} {
	return c.done
}

// Size returns the number of bytes read from the underlying reader.
// This value increases as readers read from the Cache.
func (c *Cache) Size() int {
	c.cMu.RLock()
	defer c.cMu.RUnlock()
	return c.size
}

// Err returns the first error (if any) that was returned by the underlying
// source reader, which may be io.EOF. After the source reader returns an
// error, it is never read from again. But typically the source reader
// should still be explicitly closed, by closing all of this Cache's readers.
func (c *Cache) Err() error {
	c.cMu.RLock()
	defer c.cMu.RUnlock()
	return c.readErr
}

// close is invoked by Reader.Close to close itself. If the Cache
// is sealed and r is the final unclosed reader, this method closes
// the src reader, if it implements io.Closer.
func (c *Cache) close(r *Reader) error {
	c.cMu.Lock()
	defer c.cMu.Unlock()

	c.rdrs = remove(c.rdrs, r)

	if !c.sealed {
		return nil
	}

	if len(c.rdrs) == 0 {
		defer close(c.done)
		// r is last Reader, so we can close the source
		// reader, if it implements io.Closer.
		if rc, ok := c.src.(io.Closer); ok {
			return rc.Close()
		}
	}

	return nil
}

// Seal is called to indicate that no more calls to NewReader are permitted.
// If there are no unclosed readers when Seal is invoked, the Cache.Done
// channel is closed, and the Cache is considered finished. Subsequent
// invocations will return ErrAlreadySealed.
func (c *Cache) Seal() error {
	c.cMu.Lock()
	defer c.cMu.Unlock()

	if c.sealed {
		return ErrAlreadySealed
	}

	c.sealed = true
	if len(c.rdrs) == 0 {
		close(c.done)
	}

	return nil
}

// Sealed returns true if Seal has been invoked.
func (c *Cache) Sealed() bool {
	c.cMu.RLock()
	defer c.cMu.RUnlock()
	return c.sealed
}

// Reader is returned by Cache.NewReader. It is the responsibility of the
// caller to close Reader.
type Reader struct {
	Name string // FIXME: delete when done with development

	// mu guards Reader's methods.
	mu sync.Mutex // FIXME: Do we need mu?

	// ctx is the context provided to Cache.NewReader. If non-nil,
	// every invocation of Reader.Read checks ctx for cancellation
	// before proceeding. Note that Reader.Close ignores ctx.
	ctx context.Context

	// c is the Reader's parent Cache.
	c *Cache

	// readFn is the func that Reader.Read invokes to read bytes.
	// Initially it is set to Cache.readMain, but if this reader
	// becomes the last man standing, this field may be set
	// to Cache.readSrcDirect.
	readFn readFunc

	// offset is the offset into the stream from which the next
	// Read will read. It is incremented by each Read.
	offset int

	// readErr is set by Reader.Read when an error is returned
	// from the source, and its non-nil value is returned by
	// subsequent calls to Read. That is to say: the same non-nil
	// read error is returned every time.
	readErr error

	// pCloseErr is set by Reader.Close, and the set value is
	// returned by subsequent calls to Close.
	pCloseErr *error
}

// Read implements io.Reader. If the non-nil context provided to Cache.NewReader
// is canceled, Read will return the context's error via context.Cause. If this
// reader has already been closed via Reader.Close, Read will return
// ErrAlreadyClosed. If a previous invocation of Read returned an error from the
// source, that error is returned. Otherwise Read reads from Cache, which may
// return bytes from Cache's cache or new bytes from the source, or a
// combination of both.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readErr != nil {
		return 0, r.readErr
	}

	if r.pCloseErr != nil {
		return 0, ErrAlreadyClosed
	}

	n, err = r.readFn(r, p, r.offset)
	r.readErr = err
	r.offset += n
	return n, err
}

// Close closes this Reader. If the parent Cache is not sealed, this method is
// effectively a no-op. If the parent Cache is sealed and this is the last
// remaining reader, the Cache's origin io.Reader is closed, if it implements
// io.Closer. At that point, the Cache instance is considered finished, and
// the channel returned by Cache.Done is closed.
//
// If you don't want the source reader to be closed, wrap it via io.NopCloser
// before passing it to streamcache.New.
//
// The Close operation proceeds even if the non-nil context provided to
// Cache.NewReader is cancelled. That is to say, Reader.Close ignores context.
//
// Note that subsequent calls to Close are no-op and return the same result
// as the first call.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pCloseErr != nil {
		// Already closed. Return the same error as the first call
		// to Close (which may be nil).
		return *r.pCloseErr
	}

	closeErr := r.c.close(r)
	r.pCloseErr = &closeErr
	return closeErr
}

// remove returns the slice without the first occurrence of v.
// Element order is not preserved.
func remove[T any](a []*T, v *T) []*T {
	// https://stackoverflow.com/a/37335777/6004734
	for i := range a {
		if a[i] == v {
			return append(a[:i], a[i+1:]...)
		}
	}
	return a
}
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
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/neilotoole/sq/libsq/core/lg/devlog"
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
	cMu *sync.RWMutex

	// srcMu guards concurrent access to reading from src.
	srcMu *sync.Mutex

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
}

// New returns a new Cache that reads from src. Use Cache.NewReader
// to create read from src.
func New(src io.Reader) *Cache {
	c := &Cache{
		cMu:   &sync.RWMutex{},
		srcMu: &sync.Mutex{},
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
		// cacheFillNotifyCh: make(chan struct{}, 1),
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

func Infof(l *slog.Logger, format string, args ...any) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelInfo, fmt.Sprintf(format, args...), pcs[0])
	_ = r
	_ = l.Handler().Handle(context.Background(), r)
}

func (c *Cache) readLock(log *slog.Logger) {
	Infof(log, "read lock: before")
	c.cMu.RLock()
	Infof(log, "read lock: after")
}

func (c *Cache) readUnlock(log *slog.Logger) {
	Infof(log, "read unlock: before")
	c.cMu.RUnlock()
	Infof(log, "read unlock: after")
}

func (c *Cache) tryReadLock(log *slog.Logger) bool {
	Infof(log, "try read lock: before")
	ok := c.cMu.TryRLock()
	Infof(log, "try read lock: after: %v", ok)
	return ok
}

func (c *Cache) writeLock(log *slog.Logger) {
	Infof(log, "write lock: before")
	c.cMu.Lock()
	Infof(log, "write lock: after")
}

func (c *Cache) tryWriteLock(log *slog.Logger) bool { //nolint:unused
	Infof(log, "try write lock: before")
	ok := c.cMu.TryLock()
	Infof(log, "try write lock: after: %v", ok)
	return ok
}

func (c *Cache) writeUnlock(log *slog.Logger) {
	Infof(log, "write unlock: before")
	c.cMu.Unlock()
	Infof(log, "write unlock: after")
}

func (c *Cache) srcLock(log *slog.Logger) {
	Infof(log, "src lock: before")
	c.srcMu.Lock()
	Infof(log, "src lock: after")
}

func (c *Cache) trySrcLock(log *slog.Logger) bool {
	Infof(log, "try src lock: before")
	ok := c.srcMu.TryLock()
	Infof(log, "try src lock: after: %v", ok)
	return ok
}

func (c *Cache) srcUnlock(log *slog.Logger) {
	Infof(log, "src unlock: before")
	c.srcMu.Unlock()
	Infof(log, "src unlock: after")
}

func (c *Cache) getLog(r *Reader) *slog.Logger {
	h := devlog.NewHandler(os.Stdout, slog.LevelDebug)
	return slog.New(h).With("rdr", r.Name)
}

// readMain reads from Cache.cache and/or Cache.src. If Cache is sealed
// and r is the final Reader, this method may switch r.readFn
// to Cache.readSrcDirect, such that the remaining reads occur
// directly against src, bypassing Cache.cache entirely.
func (c *Cache) readMain(r *Reader, p []byte, offset int) (n int, err error) {
	log := c.getLog(r)
TOP: // FIXME: do we still need TOP
	c.readLock(log)

	if c.sealed {
		switch len(c.rdrs) {
		case 0:
			// Should be impossible.
			panic("Cache is sealed but has zero readers")
		case 1:
			c.readUnlock(log)
			return c.handleFinalReader(r, p, offset)
		default:
			//  There are multiple readers, so we continue with
			//  normal reading using the cache.
		}
	}

	cacheLen := len(c.cache)
	end := offset + len(p)
	if end <= cacheLen {
		// The read can be satisfied entirely from the cache.
		n, err = c.fillFromCache(p, offset)
		c.readUnlock(log)
		return n, err
	}

	// We don't have enough in the cache to satisfy the read.
	// We're going to need to read from src.

	// c.mu.RUnlock()
	// First we give up the read lock.
	c.readUnlock(log)

	// And try to get the src lock.
	if !c.trySrcLock(log) {
		// We couldn't get the src lock. Another reader has src lock,
		// and could be blocked on io. But, if there's data in the
		// cache that could be returned to r, we do that now, so that r
		// can catch up to the current cache state while the other
		// reader holds src lock.

		if !c.tryReadLock(log) {
			// We couldn't get the read lock, probably because another
			// reader is updating the cache. Our strategy here is to kick r
			// back to the start of readMain, so that it can try again.
			// Maybe there's a better strategy?
			Infof(log, "try read lock failed; going back to TOP.")
			goto TOP
		}

		// We've got the read lock, let's see if there's any bytes
		// in the cache that can be returned.
		if c.size-offset > 0 {
			// Yup, there are bytes available in the cache. Return them.
			n, err = c.fillFromCache(p, offset)
			c.readUnlock(log)
			return n, err
		}

		// There's nothing in the cache for r to consume.
		// So, we need to get more data from src.

		// First we give up the read lock.
		c.readUnlock(log)

		// And now we acquire src lock, so that we can read from src.
		c.srcLock(log)
	}

	// FIXME: Should check for context cancellation after each lock?

	// If we've gotten this far, we have the src lock, but not the
	// read or write lock. We're here because there was nothing in
	// the cache for r the last time we checked. However, it's possible
	// that another reader has since updated the cache, so we check again.

	if c.size > offset {
		// There's some new stuff in the cache. Return it.
		n, err = c.fillFromCache(p, offset)
		c.srcUnlock(log)
		return n, err
	}

	// OK, this time, we're REALLY going to read from src.

	Infof(log, "Entering src.Read")
	n, err = c.src.Read(p)
	Infof(log, "Returned from src.Read: n=%d, err=%v", n, err)

	if n == 0 && err == nil {
		// For this special case, there's no need to update the cache,
		// so we can just return now.
		c.srcUnlock(log)
		return 0, nil
	}

	// Now we need to update the cache, so we need to get the write lock.
	// Note that this is the only place in readMain where we get the
	// write lock.
	c.writeLock(log)
	if err != nil {
		c.readErr = err
		c.readErrAt = offset + n
	}
	if n > 0 {
		c.size += n
		c.cache = append(c.cache, p[:n]...)
	}

	// We're done updating c, so we can release the write and src
	// locks and return.
	c.writeUnlock(log)
	c.srcUnlock(log)
	return n, err
}

// fillFromCache copies bytes from the cache to p, starting at offset,
// returning the number of bytes copied, and readErr, if it's set
// and we've reached the value of readErrAt.
func (c *Cache) fillFromCache(p []byte, offset int) (n int, err error) {
	n = copy(p, c.cache[offset:])
	if c.readErr != nil && n+offset >= c.readErrAt {
		err = c.readErr
	}
	return n, err
}

// handleFinalReader is invoked by Cache.readMain when the Cache is sealed
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
func (c *Cache) handleFinalReader(r *Reader, p []byte, offset int) (n int, err error) {
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
		// because the next read will be direct from src.
		c.cache = nil
		r.readFn = c.readSrcDirect
		return c.fillFromCache(p, offset)
	case offset == c.size:
		// The read is beyond the end of the cache.
		c.cache = nil
		r.readFn = c.readSrcDirect

		n, err = c.src.Read(p)
		c.size += n
		if err != nil {
			c.readErr = err
			c.readErrAt = offset + n
		}
		return n, err
	case offset > c.size:
		// FIXME: What's the logic with this?
		// Should be impossible.
		panic("Offset is beyond end of cache")
	default:
		// This read is an overlap of cache and src.
	}

	// This read requires combining bytes from cache with new
	// bytes from src. First, copy the cache bytes.
	n = copy(p, c.cache[offset:])

	// Now that we've got what we need from the cache,
	// we can nil it out.
	c.cache = nil

	// Next, read some more from src.
	var n2 int
	n2, err = c.src.Read(p[n:])
	c.size += n2
	n += n2
	if err != nil {
		c.readErr = err
		c.readErrAt = offset + n // REVISIT: is this correct?
	}

	// Any subsequent reads will be direct from src.
	r.readFn = c.readSrcDirect
	return n, c.readErr
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
	c.cMu.Lock()
	defer c.cMu.Unlock()
	return c.sealed
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

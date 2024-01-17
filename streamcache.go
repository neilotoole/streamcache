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
	"github.com/neilotoole/sq/libsq/core/lg/devlog"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"
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
	// mu guards concurrent access to Cache's fields and methods.
	//mu *upmutex.UpgradableRWMutex
	//mu *sync.Mutex
	mu    *sync.RWMutex
	srcMu *sync.Mutex

	//fillTo     *atomic.Int64
	//fillNotify chan *Reader

	// done is closed after the Cache is sealed and the last
	// reader is closed. See Cache.Done.
	done chan struct{}

	// src is the underlying reader from which bytes are read.
	src io.Reader

	// rdrs is the set of unclosed Reader instances created
	// by Cache.NewReader. When a Reader is closed, it is removed
	// from this slice.
	rdrs []*Reader

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// readErr is the first (and only) error returned by src's
	// Read method. Once readErr has been set to a non-nil value,
	// src is never read from again.
	readErr error

	// readErrAt is the offset at which readErr occurred.
	readErrAt int

	// size is the count of bytes read from src.
	size int

	// cache holds the accumulated bytes read from src.
	// It is nilled when the final reader switches to readOriginDirect.
	cache []byte

	// buf is used as a buffer when reading from src.
	// It is nilled when the final reader switches to readOriginDirect.
	buf []byte

	// trim is the number of bytes trimmed from the start of the cache.
	// This mechanism eliminates the need to cache bytes that
	// will never be read again.
	//
	// FIXME: implement cache trimming.
	trim int
}

// New returns a new Cache that reads from src. Use Cache.NewReader
// to create read from src.
func New(src io.Reader) *Cache {
	c := &Cache{
		src:   src,
		cache: make([]byte, 0),
		done:  make(chan struct{}),
		//fillNotify: make(chan *Reader, 100),
		//fillTo:     &atomic.Int64{},
		//mu:         &sync.Mutex{},
		mu:    &sync.RWMutex{},
		srcMu: &sync.Mutex{},
		//mu: &upmutex.UpgradableRWMutex{},
		//cacheMu: &sync.Mutex{},
		//srcMu:   &sync.Mutex{},
	}

	//c.startFiller()
	return c
}

//
//func (c *Cache) startFiller() {
//	go func() {
//		//LOOP:
//		for {
//			select {
//			case <-c.done:
//				return
//			case r := <-c.fillNotify:
//				want := int(c.fillTo.Load())
//				if want <= c.size {
//					r.cacheFillNotifyCh <- struct{}{}
//					continue
//				}
//				c.ensureBufLen(want - c.size)
//				n, err := c.src.Read(c.buf)
//				if n == 0 && err == nil {
//					r.cacheFillNotifyCh <- struct{}{}
//					continue
//				}
//				// Need to get the lock here.
//				c.mu.Lock()
//				c.readErr = err
//				if n > 0 {
//					c.size += n
//					c.cache = append(c.cache, c.buf[:n]...)
//				}
//				c.mu.Unlock()
//				r.cacheFillNotifyCh <- struct{}{}
//			}
//		}
//	}()
//}

// NewReader returns a new Reader for Cache. If Cache is already
// sealed, ErrAlreadySealed is returned. If ctx is non-nil, it is
// checked for cancellation at the start of Reader.Read.
// It is the caller's responsibility to close the returned Reader.
func (c *Cache) NewReader(ctx context.Context) (*Reader, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sealed {
		return nil, ErrAlreadySealed
	}

	r := &Reader{
		ctx:    ctx,
		c:      c,
		readFn: c.read,
		//cacheFillNotifyCh: make(chan struct{}, 1),
	}
	c.rdrs = append(c.rdrs, r)
	return r, nil
}

// readFunc is the type of the Reader.readFn field.
type readFunc func(r *Reader, p []byte, offset int) (n int, err error)

var (
	_ readFunc = (*Cache)(nil).read
	_ readFunc = (*Cache)(nil).readOriginDirect
)

// readOriginDirect reads directly from Cache.src. The src's size is
// incremented as bytes are read from src, and Cache.readErr is set if
// src returns an error.
func (c *Cache) readOriginDirect(_ *Reader, p []byte, _ int) (n int, err error) {
	n, err = c.src.Read(p)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.size += n
	c.readErr = err
	return n, c.readErr
}

//func (c *Cache) requestFillTo(r *Reader, n int) {
//	if n <= c.size {
//		c.mu.Unlock()
//		r.cacheFillNotifyCh <- struct{}{}
//		return
//	}
//
//	v := c.fillTo.Load()
//	if v < int64(n) {
//		c.fillTo.Store(int64(n))
//		c.mu.Unlock()
//		c.fillNotify <- r
//		return
//	}
//	c.mu.Unlock()
//	r.cacheFillNotifyCh <- struct{}{}
//}

func Infof(l *slog.Logger, format string, args ...any) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelInfo, fmt.Sprintf(format, args...), pcs[0])
	_ = r
	_ = l.Handler().Handle(context.Background(), r)
}

func (c *Cache) muRLock(log *slog.Logger) {
	Infof(log, "mu.RLock: before")
	c.mu.RLock()
	Infof(log, "mu.RLock: after")
}
func (c *Cache) muLock(log *slog.Logger) {
	Infof(log, "mu.Lock: before")
	c.mu.Lock()
	Infof(log, "mu.Lock: after")
}
func (c *Cache) muRUnlock(log *slog.Logger) {
	Infof(log, "mu.RUnlock: before")
	c.mu.RUnlock()
	Infof(log, "mu.RUnlock: after")
}
func (c *Cache) muUnlock(log *slog.Logger) {
	Infof(log, "mu.Unlock: before")
	c.mu.Unlock()
	Infof(log, "mu.Unlock: after")
}
func (c *Cache) srcMuLock(log *slog.Logger) {
	Infof(log, "srcMu.Lock: before")
	c.srcMu.Lock()
	Infof(log, "srcMu.Lock: after")
}
func (c *Cache) srcMuTryLock(log *slog.Logger) bool {
	Infof(log, "srcMu.TryLock: before")
	ok := c.srcMu.TryLock()
	Infof(log, "srcMu.TryLock: after: %v", ok)
	return ok
}
func (c *Cache) muTryLock(log *slog.Logger) bool {
	Infof(log, "mu.TryLock: before")
	ok := c.mu.TryLock()
	Infof(log, "mu.TryLock: after: %v", ok)
	return ok
}
func (c *Cache) muTryRLock(log *slog.Logger) bool {
	Infof(log, "mu.TryRLock: before")
	ok := c.mu.TryRLock()
	Infof(log, "mu.TryRLock: after: %v", ok)
	return ok
}
func (c *Cache) srcMuUnlock(log *slog.Logger) {
	Infof(log, "srcMu.Unlock: before")
	c.srcMu.Unlock()
	Infof(log, "srcMu.Unlock: after")
}

func (c *Cache) getLog(r *Reader) *slog.Logger {
	h := devlog.NewHandler(os.Stdout, slog.LevelDebug)
	return slog.New(h).With("rdr", r.Name)
}

//func (c *Cache) getLog(r *Reader) *slog.Logger {
//	opts := &slog.HandlerOptions{
//		AddSource: true,
//		Level:     slog.LevelDebug,
//		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
//			if a.Key == "source" {
//				s := a.Value.Any().(*slog.Source)
//				t := filepath.Base(s.File) + ":" + strconv.Itoa(s.Line)
//				return slog.String("s", t)
//			}
//			if a.Key == "time" {
//				t := a.Value.Any().(time.Time)
//				return slog.String("t", t.Format("15:04:05.000000"))
//			}
//
//			return a
//		},
//	}
//	h := slog.NewTextHandler(os.Stdout, opts)
//	return slog.New(h).With("rdr", r.Name)
//}

// read reads from Cache.cache and/or Cache.src. If Cache is sealed
// and r is the final Reader, this method may switch r.readFn
// to Cache.readOriginDirect, such that the remaining reads occur
// directly against src, bypassing Cache.cache entirely.
func (c *Cache) read(r *Reader, p []byte, offset int) (n int, err error) {
	log := c.getLog(r)
TOP: // FIXME: do we still need TOP
	c.muRLock(log)

	if c.sealed {
		switch len(c.rdrs) {
		case 0:
			// Should be impossible.
			panic("Cache is sealed but has zero readers")
		case 1:
			c.muRUnlock(log)
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
		n = copy(p, c.cache[offset:offset+len(p)])
		c.muRUnlock(log)
		return n, nil
	}

	if c.readErr != nil {
		n = copy(p, c.cache[offset:])
		c.muRUnlock(log)
		//c.mu.RUnlock()
		return n, c.readErr
	}

	// We don't have enough in the cache to satisfy the read.
	// We're going to need to read from src.

	//c.mu.RUnlock()
	c.muRUnlock(log)

	//if !c.srcMu.TryLock() {
	if !c.srcMuTryLock(log) {
		// We couldn't get the src lock.

		// Maybe we can get some bytes from the cache?
		if !c.muTryRLock(log) {
			// We couldn't get the reader lock, go back to the start.
			goto TOP
		}
		// We've got the read lock.
		if c.size-offset > 0 {
			// If we can get some bytes from the cache, do so.
			// This will allow this reader to "catch up" to the
			// other readers.
			n = copy(p, c.cache[offset:])
			c.muRUnlock(log)
			//c.mu.RUnlock()
			//fmt.Printf("%s: mu.RUnlock\n", r.Name)
			return n, nil
		}

		// We really need to read from source. So, we must get
		// the src lock.
		//fmt.Printf("%s: mu.RUnlock\n", r.Name)
		//c.mu.RUnlock()

		//fmt.Printf("%s: srcMu.Lock\n", r.Name)
		c.muRUnlock(log)
		c.srcMuLock(log)
		//c.srcMu.Lock()
	}

	// If we've gotten this far, we have the src lock, but don't have
	// the other locks.

	// FIXME: Should check for context cancellation after each lock?

	// FIXME: create a method cacheCopy that copies from cache to p,
	// but also returns an error if we've reached it. For that, we'll
	// probably need a new field in c, like c.errAtN, to record when
	// the error occurred.

	// Things may have changed; let's see if we can satisfy the read.
	if c.size > offset {
		// There's some new stuff in the cache.
		n = copy(p, c.cache[offset:])
		c.srcMuUnlock(log)
		//c.srcMu.Unlock()
		return n, nil
	}

	// We still need to read from src.
	// REVISIT: Do we really need c.buf? Can't we just copy directly into p?
	//c.ensureBufLen(end - c.size)
	Infof(log, "Entering src.Read")
	n, err = c.src.Read(p)
	//n, err = c.src.Read(c.buf)
	Infof(log, "Returned from src.Read: n=%d, err=%v", n, err)

	// Now we need to update the cache.

	// We need to get the write lock here.
	c.muLock(log)
	//c.mu.Lock()
	c.readErr = err
	if n > 0 {
		c.size += n
		c.cache = append(c.cache, p[:n]...)
	}

	//n = copy(p, c.cache[offset:])
	c.muUnlock(log)
	//c.mu.Unlock()
	c.srcMuUnlock(log)
	//c.srcMu.Unlock()

	return n, err
}

func (c *Cache) fillFromCache(p []byte, offset int) (n int, err error) {
	n = copy(p, c.cache[offset:])
	if c.readErr != nil && n+offset >= c.readErrAt {
		err = c.readErr
	}
	return n, err
}

func (c *Cache) handleFinalReader(r *Reader, p []byte, offset int) (n int, err error) {
	// It's the final reader, and the cache is sealed.
	// We're in the endgame now. There are four possibilities
	// for this read:
	//
	// 1. This read is entirely satisfied by the cache, with some
	//    unread bytes still left in the cache. The next read
	//    will still need to use the cache.
	// 2. This read exactly matches the end of the cache, with no
	//    unread bytes left in the cache. The subsequent read will be
	//    directly against source and cache can be nilled.
	// 3. The read offset aligns exactly with src's offset, thus this
	//    read can be satisfied directly from src, as will all future
	//    reads. We no longer need the cache.
	// 4. The read is an overlap of the cache and src, so we need to
	//    combine bytes from both. The subsequent read will be direct
	//    from src and thus cache can be nilled.

	//c.mu.UpgradableRUnlock() // There's only one reader left, so we can unlock now.
	//c.mu.RUnlock() // There's only one reader left, so we can unlock now.

	c.buf = nil // We no longer need c.buf at all.
	end := offset + len(p)
	switch {
	case end < c.size:
		// The read can be satisfied entirely from the cache.
		// Subsequent reads could also be satisfied by the
		// cache, so we can't nil out c.cache yet.
		n = copy(p, c.cache[offset:])
		//c.mu.Unlock()
		return n, nil
	case end == c.size:
		// The read is satisfied completely by the cache with
		// no unread cache bytes. Thus, we can nil out c.cache,
		// because the next read will be direct from source.
		n, err = copy(p, c.cache[offset:]), c.readErr
		c.cache = nil
		//c.mu.Unlock()
		r.readFn = c.readOriginDirect
		return n, err
	case offset == c.size:
		// The read is beyond the end of the cache, so we go direct.
		c.cache = nil
		r.readFn = c.readOriginDirect

		//c.mu.UpgradeWLock()
		n, err = c.src.Read(p)
		c.size += n
		c.readErr = err
		//c.mu.Unlock()
		return n, err

		//return r.readFn(r, p, offset
	case offset > c.size:
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

	// Next, fill the rest of p from src.
	var n2 int
	//c.mu.UpgradeWLock()
	n2, c.readErr = c.src.Read(p[n:])
	n += n2
	c.size += n2
	// Any subsequent reads will be direct from src.
	r.readFn = c.readOriginDirect
	return n, c.readErr
}

//
//// Copy the bytes from the cache to p.
//
//// We've read from src, but now we need to get the write lock.
//
////if n == 0 && err == nil {
////	r.cacheFillNotifyCh <- struct{}{}
////	continue
////}
//// Need to get the lock here.
////c.mu.Lock()
//c.readErr = err
//if n > 0 {
//	c.size += n
//	c.cache = append(c.cache, c.buf[:n]...)
//	n = copy(p, c.buf[:n])
//}
//c.srcMu.Unlock()
//c.mu.Unlock()
//return n, err
//c.mu.Unlock()

//c.size += n
//
//if n > 0 {
//	c.cache = append(c.cache, c.buf[:n]...)
//}
//cacheLen = len(c.cache)
//if offset >= cacheLen {
//	return 0, c.readErr
//}
//
//if end > cacheLen {
//	end = cacheLen
//}
//
//return copy(p, c.cache[offset:end]), c.readErr

//// Well, we don't have enough in the cache to satisfy the
//// read. Let's request a cache fill.
//if end <= c.size {
//	n = copy(p, c.cache[offset:])
//
//	// Uhhh... this can't happen, right?
//	c.mu.Unlock()
//	r.cacheFillNotifyCh <- struct{}{}
//	return n, nil
//}
//
//v := c.fillTo.Load()
//if v <= int64(end) {
//	// Ask the filler goroutine to fill her up.
//	c.fillTo.Store(int64(end))
//	c.mu.Unlock()
//	c.fillNotify <- r
//	return 0, nil
//}
//
//// Else, we've probably already requested a fill.
//n = copy(p, c.cache[offset:])
//c.mu.Unlock()
////r.cacheFillNotifyCh <- struct{}{}
//
////c.mu.UpgradeWLock()
////c.requestFillTo(r, end)
//return n, c.readErr
//
//c.ensureBufLen(end - cacheLen)
//println("before src read 259")
//n, err = c.src.Read(c.buf)
//println("after src read 261")
//c.readErr = err
//c.size += n
//if n > 0 {
//	c.cache = append(c.cache, c.buf[:n]...)
//}
//cacheLen = len(c.cache)
//if offset >= cacheLen {
//	return 0, c.readErr
//}
//
//if end > cacheLen {
//	end = cacheLen
//}
//
//return copy(p, c.cache[offset:end]), c.readErr

// Done returns a channel that is closed when the Cache is done. This channel
// can be used to wait for work to complete. A Cache is considered "done" after
// Seal has been invoked on it, and there are no unclosed Reader instances
// remaining.
//
// Note that it's possible that a "done" Cache has not closed its underlying
// source reader. For example, if a Cache is created and immediately sealed,
// the Cache is "done", but the underlying source reader was never closed.
// The source reader is closed only by closing the last Reader instance that
// was active after Seal was invoked.
func (c *Cache) Done() <-chan struct{} {
	return c.done
}

// Size returns the number of bytes read from the underlying reader.
// This value increases as readers read from the Cache.
func (c *Cache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// Err returns the first error (if any) that was returned by the underlying
// source reader, which may be io.EOF. After the source reader returns an
// error, it is never read from again. But typically the source reader
// should still be explicitly closed, by closing all of this Cache's readers.
func (c *Cache) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

//// ensureBufLen ensures that Cache.buf has length n, re-slicing or
//// growing buf as necessary.
//func (c *Cache) ensureBufLen(n int) {
//	switch {
//	case c.buf == nil:
//		c.buf = make([]byte, n)
//		return
//	case len(c.buf) == n:
//		return
//	case n <= cap(c.buf):
//		c.buf = c.buf[:n]
//		return
//	default:
//	}
//
//	// We need to grow the buffer.
//	c.buf = make([]byte, n)
//}

// close is invoked by Reader.Close to close itself. If the Cache
// is sealed and r is the final unclosed reader, this method closes
// the src reader, if it implements io.Closer.
func (c *Cache) close(r *Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
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

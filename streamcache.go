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
	"github.com/neilotoole/streamcache/upmutex"
	"io"
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
	mu upmutex.UpgradableRWMutex
	//srcMu   *sync.Mutex

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
	return &Cache{
		src:   src,
		cache: make([]byte, 0),
		done:  make(chan struct{}),
		//cacheMu: &sync.Mutex{},
		//srcMu:   &sync.Mutex{},
	}
}

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

	r := &Reader{ctx: ctx, c: c, readFn: c.read}
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

// read reads from Cache.cache and/or Cache.src. If Cache is sealed
// and r is the final Reader, this method may switch r.readFn
// to Cache.readOriginDirect, such that the remaining reads occur
// directly against src, bypassing Cache.cache entirely.
func (c *Cache) read(r *Reader, p []byte, offset int) (n int, err error) {

	c.mu.UpgradableRLock()
	defer c.mu.UpgradableRUnlock()

	end := offset + len(p)
	if c.sealed {
		switch len(c.rdrs) {
		case 0:
			// Should be impossible.
			panic("Cache is sealed but has zero readers")
		case 1:
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

			c.buf = nil // We no longer need c.buf at all.
			switch {
			case end < c.size:
				// The read can be satisfied entirely from the cache.
				// Subsequent reads could also be satisfied by the
				// cache, so we can't nil out c.cache yet.
				return copy(p, c.cache[offset:]), nil
			case end == c.size:
				// The read is satisfied completely by the cache with
				// no unread cache bytes. Thus, we can nil out c.cache,
				// because the next read will be direct from source.
				n, err = copy(p, c.cache[offset:]), c.readErr
				c.cache = nil
				r.readFn = c.readOriginDirect
				return n, err
			case offset == c.size:
				// The read is beyond the end of the cache, so we go direct.
				c.cache = nil
				r.readFn = c.readOriginDirect

				c.mu.UpgradeWLock()
				n, err = c.src.Read(p)
				c.size += n
				c.readErr = err
				return n, c.readErr

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
			c.mu.UpgradeWLock()
			n2, c.readErr = c.src.Read(p[n:])
			n += n2
			c.size += n2
			// Any subsequent reads will be direct from src.
			r.readFn = c.readOriginDirect
			return n, c.readErr
		default:
			//  There are multiple readers, so we continue with
			//  normal reading using the cache.
		}
	}

	cacheLen := len(c.cache)
	if end < cacheLen {
		// The read can be satisfied entirely from the cache.
		return copy(p, c.cache[offset:offset+len(p)]), nil
	}

	if c.readErr != nil {
		return copy(p, c.cache[offset:]), c.readErr
	}

	c.mu.UpgradeWLock()
	c.ensureBufLen(end - cacheLen)
	n, c.readErr = c.src.Read(c.buf)
	c.size += n

	if n > 0 {
		c.cache = append(c.cache, c.buf[:n]...)
	}
	cacheLen = len(c.cache)
	if offset >= cacheLen {
		return 0, c.readErr
	}

	if end > cacheLen {
		end = cacheLen
	}

	return copy(p, c.cache[offset:end]), c.readErr
}

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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.size
}

// Err returns the first error (if any) that was returned by the underlying
// source reader, which may be io.EOF. After the source reader returns an
// error, it is never read from again. But typically the source reader
// should still be explicitly closed, by closing all of this Cache's readers.
func (c *Cache) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.readErr
}

// ensureBufLen ensures that Cache.buf has length n, re-slicing or
// growing buf as necessary.
func (c *Cache) ensureBufLen(n int) {
	switch {
	case c.buf == nil:
		c.buf = make([]byte, n)
		return
	case len(c.buf) == n:
		return
	case n <= cap(c.buf):
		c.buf = c.buf[:n]
		return
	default:
	}

	// We need to grow the buffer.
	c.buf = make([]byte, n)
}

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
	c.mu.RLock()
	defer c.mu.RUnlock()
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

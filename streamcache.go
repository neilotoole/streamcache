// Package streamcache addresses an arcane scenario: multiple readers
// want to sample the start of an input stream (from an io.Reader),
// which involves caching, but after the samplers are satisfied,
// there's no need to maintain that cache and its memory overhead
// for the remainder of the read.
//
// Package streamcache implements a reader mechanism that allows
// multiple callers to sample some or all of the contents of a
// source reader, while only reading from the source reader once.
//
// This is, admittedly, a rather arcane situation.
//
// Let's say we're reading from stdin. For example:
//
//	$ cat myfile.ext | myprogram
//
// In this scenario, myprogram wants to detect the type of data
// in the file/pipe, and then print it out. That sampling could be done
// in a separate goroutine per sampler type. The input file could be,
// let's say, a CSV file, or a TSV file.
//
// The obvious approach is to inspect the first few lines of the
// input, and check if the input is either valid CSV, or valid TSV.
// After that process, let's say we want to dump out the entire contents
// of the input.
//
// Package streamcache provides a facility to create a Source from an
// underlying io.Reader (os.Stdin in this scenario), and spawn multiple
// readers, each of which can operate independently, in their own
// goroutines if desired. The underlying source (again, os.Stdin in this
// scenario) will only once be read from, but its data is available to
// multiple readers, because that data is cached in memory.
//
// That is, until there's only one final reader left, (after invoking
// Source.Seal) at which point the cache is discarded, and
// the final reader reads straight from the underlying source.
package streamcache

import (
	"context"
	"errors"
	"io"
	"sync"
)

// ErrSealed is returned by Source.NewReader if the source is
// already sealed.
var ErrSealed = errors.New("already sealed")

// Source encapsulates an underlying io.Reader that many callers
// can read from.
type Source struct {
	// mu protects concurrent access to Source's fields.
	mu sync.Mutex

	// origin is the underlying reader.
	origin io.Reader

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// err is the first (and only) error returned by the underlying reader.
	// Once err has been set, the underlying reader is never read from again.
	err error

	// rdrCount is the number of active Reader instances.
	rdrCount int

	// cache holds the accumulated bytes read from origin.
	cache []byte

	// buf is used as a temporary buffer for reading from origin.
	buf []byte
}

// NewSource returns a new source with r as the underlying reader.
func NewSource(r io.Reader) *Source {
	return &Source{origin: r, cache: make([]byte, 0)}
}

// NewReader returns a new Reader for Source. If Source is already
// sealed, ErrSealed is returned. If ctx is non-nil, it is used by the
// returned Reader to check for cancellation before each read.
// It is the caller's responsibility to close the returned Reader.
func (s *Source) NewReader(ctx context.Context) (*Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil, ErrSealed
	}

	s.rdrCount++
	return &Reader{ctx: ctx, s: s}, nil
}

func (s *Source) readAt(p []byte, offset int) (n int, err error) {
	end := offset + len(p)

	s.mu.Lock()
	defer s.mu.Unlock()

	cacheLen := len(s.cache)
	if end < cacheLen {
		// We already have the data in the buf cache
		return copy(p, s.cache[offset:offset+len(p)]), nil
	}

	if s.err != nil {
		return copy(p, s.cache[offset:]), s.err
	}

	need := end - cacheLen
	s.ensureBufSize(need)
	n, s.err = s.origin.Read(s.buf)

	if n > 0 {
		s.cache = append(s.cache, s.buf[:n]...)
	}

	cacheLen = len(s.cache)
	if offset >= cacheLen {
		return 0, s.err
	}

	if end > cacheLen {
		end = cacheLen
	}

	return copy(p, s.cache[offset:end]), s.err
}

func (s *Source) ensureBufSize(n int) {
	// https://www.calhoun.io/how-to-use-slice-capacity-and-length-in-go/
	// https://github.com/golang/go/issues/51462
	// https://stackoverflow.com/questions/48618201/how-to-release-memory-allocated-by-a-slice
	// https://medium.com/@cerebrovinny/mastering-golang-memory-management-tips-and-tricks-99868f1f4971
	// https://github.com/golang/go/issues/51462

	switch {
	case s.buf == nil:
		s.buf = make([]byte, n)
		return
	case len(s.buf) == n:
		return
	case n <= cap(s.buf):
		s.buf = s.buf[:n]
		return
	default:
	}

	// We need to grow the buffer.
	s.buf = make([]byte, n)
}

// close is invoked by Reader.Close to close the Reader. If the Source
// is sealed, this method will close the underlying reader if it
// implements io.Closer, and switch to "direct mode" for the final Reader
// to complete its work.
func (s *Source) close(r *Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rdrCount--
	if !s.sealed {
		return nil
	}

	switch s.rdrCount {
	default:
		// There's still multiple active Reader instances, so we
		// don't close the underlying reader.
		return nil
	case 0:
		// r was the last reader standing, so we close the underlying
		// reader if it implements io.Closer.
		if c, ok := s.origin.(io.Closer); ok {
			return c.Close()
		}
		return nil
	case 1:
		// Continues below
	}

	s.buf = nil
	if len(s.cache)-r.offset == 0 {
		// r has read already read everything in the cache, so
		// it can switch to reading directly from the origin
		// for the remaining bytes.
		r.finalReader = s.origin
		s.cache = nil
		return nil
	}

	// There's still some bytes in the cache that r hasn't read.
	// We construct a joinReader that is the concatenation of the
	// remaining cache bytes plus the origin reader. Effectively
	// it's a lightweight io.MultiReader.
	r.finalReader = &joinReader{
		unreadCache: s.cache[r.offset:],
		origin:      s.origin,
	}
	s.cache = nil
	return nil
}

// Seal is called to indicate that there will be no more calls
// to NewReader.
func (s *Source) Seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

// Reader is returned by Source.NewReader. It is the
// responsibility of the receiver to close the returned Reader.
type Reader struct {
	s         *Source
	ctx       context.Context
	offset    int
	closeOnce sync.Once
	closeErr  error

	// finalReader is only set if this Reader becomes the
	// last-man-standing of the parent Source's spawned Reader
	// children. If finalReader is set, then it will be
	// used by Read to read out the rest of data from the parent
	// Source's underlying Reader.
	finalReader io.Reader
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	if r.finalReader != nil {
		// This Reader is in "last-man-standing" mode, so we switch
		// to "direct mode". That is to say, this final Reader will
		// now read directly from finalReader instead of going
		// through the cache.
		return r.finalReader.Read(p)
	}

	// We're not in "last-man-standing" mode, so just continue
	// as normal.
	n, err = r.s.readAt(p, r.offset)
	r.offset += n
	return n, err
}

// Close closes this Reader. If the parent Source is not sealed,
// this method is effectively a no-op. If the parent Source is sealed
// and this is the last remaining reader, the parent Source's underlying
// io.Reader is closed (if it implements io.Closer).
//
// Note that subsequent calls to this method are no-op and return the
// same result as the first call.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.s.close(r)
	})

	return r.closeErr
}

var _ io.Reader = (*joinReader)(nil)

// joinReader is an io.Reader whose Read method reads from the
// concatenation of the unreadCache, and the origin reader. Effectively,
// joinReader is a lightweight io.MultiReader.
type joinReader struct {
	unreadCache []byte
	origin      io.Reader
}

// Read implements io.Reader.
func (jr *joinReader) Read(p []byte) (n int, err error) {
	cacheLen := len(jr.unreadCache)
	if cacheLen == 0 {
		// The cache is empty, so we read from the origin.
		return jr.origin.Read(p)
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
	n += cacheLen
	jr.unreadCache = nil
	return n, err
}

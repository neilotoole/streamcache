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
	"bytes"
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

	// src is the underlying reader.
	src io.Reader

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// err is the first (and only) error returned by the underlying reader.
	// Once err has been set, the underlying reader is never read from again.
	err error

	// rdrCount is the number of active Reader instances.
	rdrCount int

	// cache holds the accumulated bytes read from src.
	cache []byte

	// buf is used as a temporary buffer for reading from src.
	buf []byte
}

// NewSource returns a new source with r as the underlying reader.
func NewSource(r io.Reader) *Source {
	return &Source{src: r, cache: make([]byte, 0)}
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

func (s *Source) readAt(p []byte, offset int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := int(offset) + len(p)
	cacheLen := len(s.cache)
	if end < cacheLen {
		// We already have the data in the buf cache
		return copy(p, s.cache[offset:int(offset)+len(p)]), nil
	}

	if s.err != nil {
		return copy(p, s.cache[offset:]), s.err
	}

	need := end - cacheLen

	s.ensureBufSize(need)
	n, s.err = s.src.Read(s.buf)

	if n > 0 {
		s.cache = append(s.cache, s.buf[:n]...)
	}

	cacheLen = len(s.cache)
	if int(offset) >= cacheLen {
		return 0, s.err
	}

	if end > cacheLen {
		end = cacheLen
	}

	return copy(p, s.cache[offset:end]), s.err
}

func (s *Source) ensureBufSize(n int) {
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
	// Reset first to avoid a memory leak?
	// Not sure if this is the right thing to do.
	s.buf = s.buf[:0]
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
		if c, ok := s.src.(io.Closer); ok {
			return c.Close()
		}
		return nil
	case 1:
		// Continues below
	}

	// Now there's only one final reader left, so we switch
	// to "direct mode" for that final reader.
	// The bytes from cache are the "penultimate" bytes, because the
	// "ultimate" bytes come from s.src itself.
	penultimateBytes := s.cache[r.offset:]

	// It is safe to modify rc.finalReadCloser because rc calls this
	// close() method already being inside its own mu.Lock.
	r.finalReadCloser = &finalReadCloser{
		Reader: io.MultiReader(bytes.NewReader(penultimateBytes), s.src),
		src:    s.src,
	}

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
	mu        sync.Mutex
	s         *Source
	ctx       context.Context
	offset    int
	closeOnce sync.Once
	closeErr  error

	// finalReadCloser is only set if this Reader becomes the
	// last-man-standing of the parent Source's spawned Reader
	// children. If finalReadCloser is set, then it will be
	// used by Read to read out the rest of data from the parent
	// Source's underlying Reader.
	finalReadCloser io.Reader
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	if r.finalReadCloser == nil {
		// We're not in "last-man-standing" mode, so just continue
		// as normal.
		n, err = r.s.readAt(p, int64(r.offset))
		r.offset += n
		return n, err
	}

	// This Reader is in "last-man-standing" mode, so we switch
	// to "direct mode". That is to say, this final Reader will
	// now read directly from the finalReadCloser instead of going
	// through the cache.
	return r.finalReadCloser.Read(p)
}

// Close closes this Reader. If the parent Source is not sealed,
// this method is effectively a no-op. If the parent Source is sealed
// and this is the last remaining reader, the parent Source's underlying
// io.Reader is closed (if it implements io.Closer), and this Reader
// switches to "direct mode" reading for the remaining data.
//
// Note that subsequent calls to this method are no-op and return the
// same result as the first call.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.s.close(r)
	})

	return r.closeErr
}

// finalReadCloser is used by the final child of Source.NewReader
// to wrap up the reading/closing without the use of the cache.
type finalReadCloser struct {
	io.Reader
	src io.Reader
}

// Close implements io.Closer.
func (fc *finalReadCloser) Close() error {
	if c, ok := fc.src.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

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
var ErrSealed = errors.New("source is already sealed")

// ErrAlreadyClosed is returned by Reader.Read if the reader is
// already closed and the reader hasn't already received an error
// from the origin reader. If Reader has received such an origin error
// (including io.EOF), that error is returned preferentially.
// Invoking Reader.Close multiple times is a no-op, and always returns the
// same origin error (if any).
var ErrAlreadyClosed = errors.New("reader is already closed")

// Source encapsulates an underlying io.Reader that many callers
// can read from.
type Source struct {
	// mu protects concurrent access to Source's fields.
	mu sync.Mutex

	done chan struct{}

	// origin is the underlying reader.
	origin io.Reader

	rdrs []*Reader

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// err is the first (and only) error returned by the underlying reader.
	// Once err has been set, the underlying reader is never read from again.
	err error

	// size is the number of bytes read from origin.
	size int

	// cache holds the accumulated bytes read from origin.
	cache []byte

	// buf is used as a temporary buffer for reading from origin.
	buf []byte
}

// NewSource returns a new source with r as the underlying reader.
func NewSource(r io.Reader) *Source {
	return &Source{origin: r, cache: make([]byte, 0), done: make(chan struct{})}
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

	r := &Reader{ctx: ctx, s: s, readFn: s.readAt}
	s.rdrs = append(s.rdrs, r)
	return r, nil
}

// readDirect reads directly from the origin reader. The source's
// size is incremented as bytes are read form origin, and s.err
// may be set if origin returns an error.
func (s *Source) readDirect(_ *Reader, p []byte, _ int) (n int, err error) {
	n, err = s.origin.Read(p)
	s.size += n
	s.err = err
	return n, s.err
}

func (s *Source) readAt(r *Reader, p []byte, offset int) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := offset + len(p)
	if s.sealed {
		switch len(s.rdrs) {
		case 0:
			// Should be impossible.
			panic("Source is sealed but has zero readers")
		case 1:
			// It's the final reader, and the source is sealed.
			// We're in the endgame now. There are four possibilities
			// for this read:
			//
			// 1. This read is entirely satisfied by the cache, with some
			//    unread bytes still left in the cache. The next read
			//    will still need to use the cache.
			// 2. This read exactly matches the end of the cache, with no
			//    unread bytes left in the cache. The subsequent read will be
			//    directly against origin and cache can be nilled.
			// 3. The read offset aligns exactly with origin's offset, thus this
			//    read can be satisfied directly from origin, as will all future
			//    reads. We no longer need the cache.
			// 4. The read is an overlap of the cache and origin, so we need to
			//    combine bytes from both. The subsequent read will be direct
			//    from origin and thus cache can be nilled.

			s.buf = nil // We no longer need s.buf at all.
			switch {
			case end < s.size:
				// The read can be satisfied entirely from the cache.
				// Subsequent reads could also be satisfied by the
				// cache, so we can't nil out s.cache yet.
				return copy(p, s.cache[offset:]), nil
			case end == s.size:
				// The read is satisfied completely by the cache with
				// no unread cache bytes. Thus, we can nil out s.cache,
				// because the next read will be direct from origin.
				n, err = copy(p, s.cache[offset:]), s.err
				s.cache = nil
				r.readFn = s.readDirect
				return n, err
			case offset == s.size:
				// The read is beyond the end of the cache, so we go direct.
				// I'm not actually sure if we can reach this case?
				s.cache = nil
				r.readFn = s.readDirect
				return r.readFn(r, p, offset)
			case offset > s.size:
				// Should be impossible.
				panic("Offset is beyond end of cache")
			default:
				// The read is an overlap of the cache and origin.
			}

			// This read requires combining bytes from the cache with new
			// bytes from origin. First copy the cache bytes.
			n = copy(p, s.cache[offset:])

			// Now that we've got what we need from the cache,
			// we can nil it out.
			s.cache = nil

			// Next, fill the rest of p from origin.
			var n2 int
			n2, s.err = s.origin.Read(p[n:])
			n += n2
			s.size += n2
			// Any subsequent reads will be direct from origin.
			r.readFn = s.readDirect
			return n, s.err
		default:
			//  There are multiple readers, so continue with normal reading.
		}
	}

	cacheLen := len(s.cache)
	if end < cacheLen {
		// The read can be satisfied entirely from the cache.
		return copy(p, s.cache[offset:offset+len(p)]), nil
	}

	if s.err != nil {
		return copy(p, s.cache[offset:]), s.err
	}

	s.ensureBufSize(end - cacheLen)
	n, s.err = s.origin.Read(s.buf)
	s.size += n

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

// Done returns a channel that is closed when the Source is done. This channel
// can be used to wait for work to complete. A source is considered "done" after
// Seal has been invoked on it and there are no unclosed Reader instances
// remaining.
//
// Note that it's possible that a "done" Source has not closed its underlying
// origin reader. For example, if a Source is created and immediately sealed,
// the Source is "done", but the underlying origin reader was never closed.
// The origin reader is closed only by closing the last Reader instance that
// was active after Seal was invoked.
func (s *Source) Done() <-chan struct{} {
	return s.done
}

// Size returns the number of bytes read from the underlying reader.
// This value increases as readers read from the Source.
func (s *Source) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Err returns the first error (if any) that was returned by the underlying
// origin reader, which may be io.EOF. After the origin reader returns an
// error, it is never read from again. But, it should still be explicitly
// closed, by closing all readers returned by Source.NewReader.
func (s *Source) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
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

	s.rdrs = remove(s.rdrs, r)

	if !s.sealed {
		return nil
	}

	if len(s.rdrs) == 0 {
		defer close(s.done)
		// This was the last reader, so we can close the underlying
		// reader if it implements io.Closer.
		if c, ok := s.origin.(io.Closer); ok {
			return c.Close()
		}
	}

	return nil
}

// Seal is called to indicate that no more calls to NewReader are permitted.
// If there are no existing readers, the Source.Done channel is closed,
// and the Source is considered completed.
func (s *Source) Seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return ErrSealed
	}

	s.sealed = true
	if len(s.rdrs) == 0 {
		close(s.done)
	}

	return nil
}

// Sealed returns true if Seal has been invoked.
func (s *Source) Sealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// remove returns the slice without v. Element order is not preserved.
func remove[T any](a []*T, v *T) []*T {
	// https://stackoverflow.com/a/37335777/6004734
	for i := range a {
		if a[i] == v {
			return append(a[:i], a[i+1:]...)
		}
	}
	return a
}

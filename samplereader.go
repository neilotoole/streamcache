// Package samplereader implements a reader mechanism that allows
// multiple callers to sample some or the entire contents of a
// source reader, while only reading from the source reader once.
//
// This is admittedly a rather arcane need.
// Let's say we're reading from stdin. For example:
//
//  # cat myfile.ext | myprogram
//
// In this scenario, myprogram wants to detect the type of data
// in the file/pipe, and then print it out. The input file could be,
// let's say, a CSV file, or a TSV file.
//
// The obvious approach is to inspect the first few lines of the
// input, and check if the input is either valid CSV, or valid TSV.
// After that process, let's say we want to dump out the entire contents
// of the input.
//
// Package samplereader provides a facility to create a Source from an
// underlying io.Reader (os.Stdin in this scenario), and spawn multiple
// readers, each of which can operate independently, in their own
// goroutines if desired. The underlying source (again, os.Stdin in this
// scenario) will only be read from once, but its data is available to
// multiple readers, because that data is cached in memory.
// That is, until there's only one final reader left, (after invoking
// Source.Seal) at which point the cache is discarded, and
// the final reader reads straight from the underlying source.
package samplereader

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// ErrSealed is returned by Source.NewReadCloser if the source is
// already sealed.
var ErrSealed = errors.New("already sealed")

// Source encapsulates an underlying io.Reader that many callers
// can read from.
type Source struct {
	src    io.Reader
	mu     sync.Mutex
	sealed bool
	closed bool
	count  int
	buf    *bytes.Buffer
	eof    bool
}

// NewSource returns a new source with r as the underlying reader.
func NewSource(r io.Reader) *Source {
	return &Source{src: r, buf: &bytes.Buffer{}}
}

// NewReadCloser returns a new ReadCloser for Source. It is the caller's
// responsibility to close the returned ReadCloser.
func (s *Source) NewReadCloser() (*ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil, ErrSealed
	}

	s.count++
	return &ReadCloser{s: s}, nil
}

func (s *Source) readAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := int(off) + len(p)

	bLen := s.buf.Len()

	if end < bLen {
		// We already have the data
		b := s.buf.Bytes()
		copy(p, b[off:int(off)+len(p)])
		return len(p), nil
	}

	need := end - bLen
	tmp := make([]byte, need)
	n, err = s.src.Read(tmp)

	if n > 0 {
		_, _ = s.buf.Write(tmp[0:n])
	}

	if int(off) >= s.buf.Len() {
		return 0, err
	}

	if end > s.buf.Len() {
		end = s.buf.Len()
	}

	x := s.buf.Bytes()[off:end]
	n = copy(p, x)

	return n, err
}

func (s *Source) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count--
	if s.sealed && s.count == 0 {
		if c, ok := s.src.(io.Closer); ok {
			return c.Close()
		}

		return nil
	}

	// There's still open reader instances, so we don't
	// close the underlying reader.
	return nil
}

// Seal is called to indicate that there will be no more calls
// to NewReadCloser.
func (s *Source) Seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

// ReadCloser is returned by Source.NewReadCloser.
type ReadCloser struct {
	s   *Source
	off int // needs to be atomic
}

// Read implements io.Reader.
func (r *ReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.s.readAt(p, int64(r.off))
	r.off += n

	return n, err
}

// Close closes this reader. If the parent Source is not sealed, this
// method is effectively a no-op. If the parent Source is sealed and
// is the last remaining reader, the parent Source's underlying
// io.Reader is closed, if it implements io.Closer.
func (r *ReadCloser) Close() error {
	return r.s.close()
}

// buffer is a basic implementation of io.Writer.
type buffer struct {
	b []byte
}

// Write implements io.Writer.
func (b *buffer) Write(p []byte) (n int, err error) {
	b.b = append(b.b, p...)
	return len(p), nil
}

// Package samplereader implements a reader mechanism that allows
// multiple callers to sample some or the entire contents of a
// source reader, while only reading from the source reader once.
package samplereader

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// Source provides a somewhat specialized reader/sampling
// mechanism. It takes a source io.Reader, and allows many callers
// to invoke NewReader, providing each with a reader that returns
// the same data as the source reader. The data read from the source
// is cached. Once Seal is invoked, no new readers can be created.
// When there's only one reader remaining, the cache is dispensed
// with, and that reader reads remaining data directly from source.
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

// ErrSealed is returned by Source.NewReader if it is
// already sealed.
var ErrSealed = errors.New("already sealed")

func (s *Source) NewReader() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil, ErrSealed
	}

	s.count++
	return &reader{s: s}, nil
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
	//s.buf.Grow(need)

	tmp := make([]byte, need)

	//tmp := bufb[bLen:end]
	n, err = s.src.Read(tmp)
	//if err != nil {
	//	// TODO: this is a simple return for now, but if n > 0,
	//	//  we should copy some bytes into p
	//	return 0, err
	//}

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
	//
	//
	//read := copy(p, s.buf.Bytes()[off:])
	//return read, nil
	//
	//
	//
	//
	//n, err = s.src.Read(p)
	//if err != nil {
	//	return n, err
	//}
	//
	//s.b = append(s.b, p[0:n]...)
	//
	//return n, err
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
	// close the underlying.
	return nil
}

// Seal results
func (s *Source) Seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

//func (s *Source) Close() error {
//	s.mu.Lock()
//	s.closed = true
//	s.mu.Unlock()
//	return nil
//}

type reader struct {
	s   *Source
	off int // needs to be atomic
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.s.readAt(p, int64(r.off))
	r.off += n

	return n, err

	//return r.s.read(p)
}

func (r *reader) Close() error {
	r.s.close()
	return nil
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

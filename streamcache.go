// Package streamcache implements a cache mechanism that allows
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
// Package streamcache provides a facility to create a caching Stream from
// an underlying io.Reader (os.Stdin in this scenario), and spawn multiple
// readers, each of which can operate independently, in their own
// goroutines if desired. The underlying source (again, os.Stdin in this
// scenario) will only be read from once, but its data is available to
// multiple readers, because that data is cached in memory.
//
// That is, until after Stream.Seal is invoked: when there's only one final
// reader left, the cache is discarded, and the final reader reads directly
// from the underlying source.
//
// The entrypoint to this package is streamcache.New, which returns a
// new Stream instance, from which readers can be created via Stream.NewReader.
package streamcache

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/neilotoole/fifomu"
)

// ErrAlreadyClosed is returned by Reader.Read if the reader is
// already closed.
var ErrAlreadyClosed = errors.New("reader is already closed")

// Stream mediates access to the bytes of an underlying source io.Reader.
// Multiple callers can invoke Stream.NewReader to obtain a Reader, each of
// which can read the full or partial contents of the source reader. Note
// that the source is only read from once, and the returned bytes are cached
// in memory. After Stream.Seal is invoked and readers are closed, the final
// reader discards the cache and reads directly from the source for the
// remaining bytes.
type Stream struct {
	// src is the underlying reader from which bytes are read.
	src io.Reader

	// readErr is the first (and only) error returned by src's
	// Read method. Once readErr has been set to a non-nil value,
	// src is never read from again.
	readErr error

	// rdrsDoneCh is closed after the Stream is sealed and the last
	// reader is closed. See Stream.ReadersDone.
	rdrsDoneCh chan struct{}

	// srcDoneCh is closed when the underlying source reader
	// returns an error, including io.EOF. See Stream.EOF.
	srcDoneCh chan struct{}

	// rdrs is the set of unclosed Reader instances created
	// by Stream.NewReader. When a Reader is closed, it is removed
	// from this slice.
	rdrs []*Reader

	// cache holds the accumulated bytes read from src.
	// It is nilled when the final reader switches to readSrcDirect.
	cache []byte

	// srcMu guards concurrent access to reading from src. Note that it
	// is not an instance of sync.Mutex, but instead fifomu.Mutex, which
	// is a mutex whose Lock method returns the lock to callers in FIFO
	// call order. This is important in Stream.readMain because, as implemented,
	// a reader could get the src lock on repeated calls, starving the other
	// readers, which is a big problem if that greedy reader blocks
	// on reading from src. Most likely our use of locks could be
	// improved to avoid this scenario, but that's where we're at today.
	srcMu fifomu.Mutex

	// size is the count of bytes read from src.
	size int

	// cMu guards concurrent access to Stream's fields and methods.
	cMu sync.RWMutex

	// sealed is set to true when Seal is called. When sealed is true,
	// no more calls to NewReader are allowed.
	sealed bool

	// Consider our two mutexes cMu and srcMu above. There are effectively
	// three locks that can be acquired.
	//
	//  - cMu's read lock
	//  - cMu's write lock
	//  - srcMu's lock
	//
	// These three locks are referred to in the comments as the read, write,
	// and src locks.
}

// New returns a new Stream that wraps src. Use Stream.NewReader
// to read from src.
func New(src io.Reader) *Stream {
	c := &Stream{
		src:        src,
		cache:      make([]byte, 0),
		rdrsDoneCh: make(chan struct{}),
		srcDoneCh:  make(chan struct{}),
	}

	return c
}

// NewReader returns a new Reader from Stream. If ctx is non-nil, it is
// checked for cancellation at the start of Reader.Read (and possibly
// at some other checkpoints).
//
// It is the caller's responsibility to close the returned Reader.
//
// NewReader panics if s is already sealed via Stream.Seal.
func (s *Stream) NewReader(ctx context.Context) *Reader {
	s.cMu.Lock()
	defer s.cMu.Unlock()

	if s.sealed {
		panic("streamcache: Stream.NewReader invoked on sealed Stream")
	}

	r := &Reader{
		ctx:    ctx,
		s:      s,
		readFn: s.readMain,
	}
	s.rdrs = append(s.rdrs, r)
	return r
}

// readFunc is the type of the Reader.readFn field.
type readFunc func(r *Reader, p []byte, offset int) (n int, err error)

var (
	_ readFunc = (*Stream)(nil).readMain
	_ readFunc = (*Stream)(nil).readSrcDirect
)

// readSrcDirect reads directly from Stream.src. The src's size is
// incremented as bytes are read from src, and Stream.readErr is set if
// src returns an error.
func (s *Stream) readSrcDirect(r *Reader, p []byte, _ int) (n int, err error) {
	n, err = s.src.Read(p)

	// Get the write lock before updating c's fields.
	s.writeLock(r)
	defer s.writeUnlock(r)
	s.size += n
	s.readErr = err
	if err != nil {
		close(s.srcDoneCh)
	}

	return n, err
}

// readMain reads from Stream.cache and/or Stream.src. If Stream is sealed
// and r is the final Reader, this method may switch r's Reader.readFn
// to Stream.readSrcDirect, such that the remaining reads occur
// directly against src, bypassing Stream.cache entirely.
func (s *Stream) readMain(r *Reader, p []byte, offset int) (n int, err error) {
TOP:
	s.readLock(r)

	if s.sealed && len(s.rdrs) == 1 {
		// The stream is sealed, and this is the final reader.
		// We can release the read lock (because this is the only possible
		// reader), and delegate to readFinal.
		s.readUnlock(r)
		return s.readFinal(r, p, offset)
	}

	if s.size > offset {
		// There's some data in the cache that can be returned.
		// Even if the amount of data is not enough to fill p,
		// we return what we can, and let the caller decide
		// whether to read more.
		n, err = s.fillFromCache(p, offset)
		s.readUnlock(r)
		return n, err
	}

	// We don't have enough in the cache to satisfy the read.
	// We're going to need to read from src.

	// First we give up the read lock.
	s.readUnlock(r)

	// And try to get the src lock.
	if !s.srcTryLock(r) {
		// We couldn't get the src lock. Another reader has the src lock,
		// and could be blocked on io. But, in the time since we released
		// the read lock above, it's possible that more data was added to
		// the cache. If that's the case we return that fresh cache data
		// so that r can catch up to the current cache state while some other
		// reader holds src lock.

		if !s.readTryLock(r) {
			// We couldn't get the read lock, because another reader
			// is updating the cache after having read from src, and thus
			// has the write lock. There's only a tiny window where the
			// write lock is held, so our naive strategy here is to just
			// go back to the top.
			logf(r, "try read lock failed; going back to TOP.")
			goto TOP
		}

		// We've got the read lock, let's see if there's any fresh bytes
		// in the cache that can be returned.
		if s.size > offset {
			// Yup, there are bytes available in the cache. Return them.
			n, err = s.fillFromCache(p, offset)
			s.readUnlock(r)
			return n, err
		}

		// There's nothing in the cache for r to consume.
		// So, we really need to get more data from src.

		// First we give up the read lock.
		s.readUnlock(r)

		// And now we acquire the src lock so that we
		// can read from src.
		s.srcLock(r)
	}

	// FIXME: Should check for context cancellation after each lock?

	// If we've gotten this far, we have the src lock, but not the
	// read or write lock. We're here because there was nothing new in
	// the cache for r the last time we checked. However, it's possible
	// that another reader has since updated the cache, so we check again.

	// We don't need to acquire the read lock, because we already have the
	// src lock, and only the src lock holder ever acquires the write lock,
	// so it's safe to proceed.
	if s.size > offset {
		// There's some new stuff in the cache. Return it.
		n, err = s.fillFromCache(p, offset)
		s.srcUnlock(r)
		return n, err
	}

	// We're almost ready to read from src. Because src read is potentially
	// a blocking operation, we first check context cancellation. After all,
	// it's possible that r has been waiting around for a while trying to
	// acquire the src lock, and the context has been canceled in the
	// meantime.
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			s.srcUnlock(r)
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	// OK, this time, we're REALLY going to read from src.
	logf(r, "Entering src.Read")
	n, err = s.src.Read(p)
	logf(r, "Returned from src.Read: n=%d, err=%v", n, err)

	if n == 0 && err == nil {
		// For this special case, there's no need to update the cache,
		// so we can just return now.
		s.srcUnlock(r)
		return 0, nil
	}

	// Now we need to update the cache, so we need to get the write lock.
	s.writeLock(r)
	s.readErr = err
	if n > 0 {
		s.size += n
		s.cache = append(s.cache, p[:n]...)
	}

	// We're done updating the cache, so we can release the write and src
	// locks, and return.
	s.writeUnlock(r)
	s.srcUnlock(r)
	runtime.Gosched() // FIXME: delete runtime.Gosched call.
	return n, err
}

// fillFromCache copies bytes from Stream.cache to p, starting at offset,
// returning the number of bytes copied. If readErr is non-nil and we've
// reached the value of ErrAt, readErr is returned.
func (s *Stream) fillFromCache(p []byte, offset int) (n int, err error) {
	n = copy(p, s.cache[offset:])
	if s.readErr != nil && n+offset >= s.size {
		err = s.readErr
	}
	return n, err
}

// readFinal is invoked by Stream.readMain when the Stream is sealed
// and r is the final Reader. There are four possibilities for this read:
//
//  1. This read is entirely satisfied by the cache, with some
//     unread bytes still left in the cache. The next read
//     will still need to use the cache.
//  2. This read exactly matches the end of the cache, with no
//     unread bytes left in the cache. The subsequent read will be
//     directly against src and cache can be nilled.
//  3. The read offset aligns exactly with src's offset, thus this
//     read can be satisfied directly from src, as will all future
//     reads. We no longer need the cache.
//  4. The read is an overlap of the cache and src, so we need to
//     combine bytes from both. The subsequent read will be direct
//     from src and thus cache can be nilled.
func (s *Stream) readFinal(r *Reader, p []byte, offset int) (n int, err error) {
	end := offset + len(p)
	switch {
	case end < s.size:
		// The read can be satisfied entirely from the cache.
		// Subsequent reads could also be satisfied by the
		// cache, so we can't nil out s.cache yet.
		return s.fillFromCache(p, offset)
	case end == s.size:
		n, err = s.fillFromCache(p, offset)
		// The read is satisfied completely by the cache with
		// no unread cache bytes. Thus, we can nil out s.cache,
		// because the next read will be directly from src, and
		// the cache will never be used again.
		s.cache = nil
		r.readFn = s.readSrcDirect
		return n, err
	case offset == s.size:
		// The read is entirely beyond what's cached, so we switch
		// to reading directly from src. We can nil out s.cache,
		// as it'll never be used again.
		s.cache = nil
		r.readFn = s.readSrcDirect

		// We don't need to get the src lock, because this is the final reader.
		n, err = s.src.Read(p)

		// Because we're updating s's fields, we need to get the write lock.
		s.writeLock(r)
		s.size += n
		s.readErr = err
		s.writeUnlock(r)
		return n, err
	case offset > s.size:
		// Should be impossible.
		panic("Offset is beyond end of cache")
	default:
		// This read is an overlap of cache and src.
	}

	// This read requires combining bytes from cache with new
	// bytes from src. First, copy the cache bytes.
	n, err = s.fillFromCache(p, offset)
	// Now that we've got what we need from the cache,
	// we can nil it out. It'll never be used again.
	s.cache = nil
	if err != nil {
		return n, err
	}

	// Next, read some more from src. We don't need to get src lock,
	// because this is the final reader.
	var n2 int
	n2, err = s.src.Read(p[n:])

	// Because we're updating s's fields, we need to get write lock.
	s.writeLock(r)
	s.size += n2
	s.readErr = err
	n += n2
	s.writeUnlock(r)
	// Any subsequent reads will be direct from src.
	r.readFn = s.readSrcDirect
	return n, err
}

// ReadersDone returns a channel that is closed when the Stream is
// sealed and all remaining readers are closed.
//
//	select {
//	case <-s.ReadersDone():
//	  fmt.Println("All readers are done")
//	  if err := s.Err(); err != nil {
//	    fmt.Println("But an error occurred:", err)
//	  }
//	default:
//	  fmt.Println("The stream still is being read from")
//	}
//
// The returned channel is closed after Seal has been invoked on it, and there
// are zero unclosed Reader instances remaining. Note that Stream.Err returning a
// non-nil value does not of itself indicate that the readers are done. There
// could be other readers still consuming earlier parts of the cache.
//
// Note also that it's possible that even after the returned channel is closed,
// Stream may not have closed its underlying source reader. For example, if a
// Stream is created and immediately sealed, the channel returned by ReadersDone
// is closed, although the underlying source reader was never closed.
// The source reader is closed only by closing the final Reader instance
// that was active after Seal is invoked.
//
// See also: Stream.SourceDone.
func (s *Stream) ReadersDone() <-chan struct{} {
	return s.rdrsDoneCh
}

// SourceDone returns a channel that is closed when the underlying source
// reader returns an error, including io.EOF. If the source reader returns
// an error, it is never read from again. If the source reader does not
// return an error, this channel is never closed.
//
// See also: Stream.ReadersDone.
func (s *Stream) SourceDone() <-chan struct{} {
	return s.srcDoneCh
}

// Size returns the current count of bytes read from the source reader.
// This value increases as readers read from the Stream.
//
// See also: Stream.Total.
func (s *Stream) Size() int {
	s.cMu.RLock()
	defer s.cMu.RUnlock()
	return s.size
}

// Total blocks until the source reader is fully read, and returns the total
// number of bytes read from the source, and any read error other than io.EOF
// returned by the source. If ctx is cancelled, zero and the context's cause
// error are returned. If source returned a non-EOF error, that error and
// the total number of bytes read are returned.
//
// Note that Total only returns if the channel returned by Stream.SourceDone
// is closed, but Total can return even if Stream.ReadersDone is not closed.
// That is to say, Total returning does not necessarily mean that all readers
// are closed.
//
// See also: Stream.Size, Stream.Err, Stream.SourceDone, Stream.ReadersDone.
func (s *Stream) Total(ctx context.Context) (size int, err error) {
	select {
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	case <-s.srcDoneCh:
		s.cMu.RLock()
		defer s.cMu.RUnlock()

		size = s.size
		err = s.readErr
		if err != nil && errors.Is(err, io.EOF) {
			err = nil
		}
		return size, err
	}
}

// Err returns the first error (if any) that was returned by the underlying
// source reader, which may be io.EOF. After the source reader returns an
// error, it is never read from again. But typically the source reader
// should still be explicitly closed, by closing all of this Stream's readers.
func (s *Stream) Err() error {
	s.cMu.RLock()
	defer s.cMu.RUnlock()
	return s.readErr
}

// ErrAt returns the byte offset at which the first error (if any) was
// returned by the underlying source reader, or -1. Thus, if Stream.Err
// is non-nil, ErrAt will be >= 0 and equal to Stream.Size.
func (s *Stream) ErrAt() int {
	s.cMu.RLock()
	defer s.cMu.RUnlock()
	if s.readErr == nil {
		return -1
	}
	return s.size
}

// Seal is called to indicate that no more calls to NewReader are permitted.
// If there are no unclosed readers when Seal is invoked, the Stream.ReadersDone
// channel is closed, and the Stream is considered finished. Subsequent
// invocations are no-op.
func (s *Stream) Seal() {
	s.cMu.Lock()
	defer s.cMu.Unlock()

	if s.sealed {
		return
	}

	s.sealed = true
	if len(s.rdrs) == 0 {
		close(s.rdrsDoneCh)
	}
}

// Sealed returns true if Seal has been invoked.
func (s *Stream) Sealed() bool {
	s.cMu.RLock()
	defer s.cMu.RUnlock()
	return s.sealed
}

// close is invoked by Reader.Close to close itself. If the Stream
// is sealed and r is the final unclosed reader, this method closes
// the src reader, if it implements io.Closer.
func (s *Stream) close(r *Reader) error {
	s.cMu.Lock()
	defer s.cMu.Unlock()

	s.rdrs = remove(s.rdrs, r)

	if !s.sealed {
		return nil
	}

	if len(s.rdrs) == 0 {
		defer close(s.rdrsDoneCh)
		// r is last Reader, so we can close the source
		// reader, if it implements io.Closer.
		if rc, ok := s.src.(io.Closer); ok {
			return rc.Close()
		}
	}

	return nil
}

var _ io.ReadCloser = (*Reader)(nil)

// Reader is returned by Stream.NewReader. It implements io.ReadCloser; the
// caller must close the Reader when finished with it.
type Reader struct {
	// ctx is the context provided to Stream.NewReader. If non-nil,
	// every invocation of Reader.Read checks ctx for cancellation
	// before proceeding. Note that Reader.Close ignores ctx.
	ctx context.Context

	// readErr is set by Reader.Read when an error is returned
	// from the source, and its non-nil value is returned by
	// subsequent calls to Read. That is to say: the same non-nil
	// read error is returned every time.
	readErr error

	// s is the Reader's parent Stream.
	s *Stream

	// readFn is the func that Reader.Read invokes to read bytes.
	// Initially it is set to Stream.readMain, but if this reader
	// becomes the last man standing, this field may be set
	// to Stream.readSrcDirect.
	readFn readFunc

	// pCloseErr is set by Reader.Close, and the set value is
	// returned by any subsequent calls to Close.
	pCloseErr *error
	Name      string // FIXME: delete when done with development

	// offset is the offset into the stream from which the next
	// call to Read will read. It is incremented by each Read.
	offset int

	// mu guards Reader's methods.
	mu sync.Mutex
}

// Read implements io.Reader. If a non-nil context was provided to Stream.NewReader
// to create this Reader, that context is checked at the start of each call
// to Read (and possibly at some other checkpoints): if the context has been
// canceled, Read will return the context's error via context.Cause. Note
// however that Read can still block on reading from the Stream source. If this
// reader has already been closed via Reader.Close, Read will return ErrAlreadyClosed.
// If a previous invocation of Read returned an error from the source, that
// error is returned. Otherwise Read reads from Stream, which may return bytes
// from Stream's cache or new bytes from the source, or a combination of both.
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

// Close closes this Reader. If the parent Stream is not sealed, this method
// ultimately returns nil. If the parent Stream is sealed and this is the last
// remaining reader, the Stream's source reader is closed, if it implements
// io.Closer. At that point, and the channel returned by Stream.ReadersDone
// is closed.
//
// If you don't want the source to be closed, wrap it via io.NopCloser before
// passing it to streamcache.New.
//
// The Close operation proceeds even if the non-nil context provided to
// Stream.NewReader is cancelled. That is to say, Reader.Close ignores context.
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

	closeErr := r.s.close(r)
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

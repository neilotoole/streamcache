package streamcache

import (
	"context"
	"io"
	"sync"
)

// Reader is returned by Source.NewReader. It is the
// responsibility of the receiver to close the returned Reader.
type Reader struct {
	mu        sync.Mutex
	s         *Source
	ctx       context.Context
	offset    int
	closeOnce sync.Once
	closeErr  error

	err error

	// finalReader is only set if this Reader becomes the
	// last-man-standing of the parent Source's spawned Reader
	// children. If finalReader is set, then it will be
	// used by Read to read out the rest of data from the parent
	// Source's underlying Reader.
	finalReader io.Reader

	rdrAtFunc readerAtFunc
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

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return 0, r.err
	}

	//if r.finalReader != nil {
	//	// This Reader is in "last-man-standing" mode, so we switch
	//	// to "direct mode". That is to say, this final Reader will
	//	// now read directly from finalReader instead of going
	//	// through the cache.
	//	// TODO: we can update s.size here
	//	n, err = r.finalReader.Read(p)
	//	r.err = err
	//	return n, r.err
	//}

	// We're not in "last-man-standing" mode, so just continue
	// as normal.
	n, err = r.rdrAtFunc(r, p, r.offset)
	r.err = err
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
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closeErr = r.s.close(r)
	})

	return r.closeErr
}

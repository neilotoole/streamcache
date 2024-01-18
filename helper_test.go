package streamcache

// File helper_test.go contains test helper functionality.

import (
	crand "crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"time"
)

var _ io.Reader = (*delayReader)(nil)

// newDelayReader returns an io.Reader that delays on each read from r.
// If jitter is true, a randomized jitter factor is added to the delay.
// If r implements io.Closer, the returned reader will also
// implement io.Closer; if r doesn't implement io.Closer,
// the returned reader will not implement io.Closer.
// If r is nil, nil is returned.
func newDelayReader(r io.Reader, delay time.Duration, jitter bool) io.Reader {
	if r == nil {
		return nil
	}

	dr := delayReader{r: r, delay: delay, jitter: jitter}
	if _, ok := r.(io.Closer); ok {
		return delayReadCloser{dr}
	}
	return dr
}

var _ io.Reader = (*delayReader)(nil)

type delayReader struct {
	r      io.Reader
	delay  time.Duration
	jitter bool
}

// Read implements io.Reader.
func (d delayReader) Read(p []byte) (n int, err error) {
	delay := d.delay
	if d.jitter {
		delay += time.Duration(mrand.Int63n(int64(d.delay))) / 3
	}

	time.Sleep(delay)
	return d.r.Read(p)
}

type delayReadCloser struct {
	delayReader
}

// Close implements io.Closer.
func (d delayReadCloser) Close() error {
	if c, ok := d.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// newErrorAfterNReader returns an io.Reader that returns err after
// reading n random bytes from crypto/rand.Reader.
func newErrorAfterNReader(n int, err error) io.Reader {
	return &errorAfterNReader{afterN: n, err: err}
}

type errorAfterNReader struct {
	err    error
	mu     sync.Mutex
	afterN int
	count  int
}

func (r *errorAfterNReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count >= r.afterN {
		return 0, r.err
	}

	// There's some bytes to read
	allowed := r.afterN - r.count
	if allowed > len(p) {
		n, _ = crand.Read(p)
		r.count += n
		return n, nil
	}
	n, _ = crand.Read(p[:allowed])
	if n != allowed {
		panic(fmt.Sprintf("expected to readMain %d bytes, got %d", allowed, n))
	}
	r.count += n
	return n, r.err
}

// newLimitRandReader returns an io.Reader that reads up to limit bytes
// from crypto/rand.Reader.
func newLimitRandReader(limit int64) io.Reader {
	return io.LimitReader(crand.Reader, limit)
}

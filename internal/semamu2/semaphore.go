package semamu2

import (
	"container/list"
	"context"
	"sync"
)

func NewWeighted() *Weighted {
	return &Weighted{
		reqPool: sync.Pool{New: func() any {
			return make(chan struct{})
		}},
	}
}

// Weighted provides a way to bound concurrent access to a resource.
// The callers can request access with a given weight.
type Weighted struct {
	// reqPool caches request instances for reuse.
	// For some applications, it may be normal for the Mutex to
	// be locked and unlocked millions of times, so we want to
	// avoid allocating millions of request instances.
	reqPool sync.Pool
	waiters list.List

	cur int64
	mu  sync.Mutex
}

// Acquire acquires the semaphore with a weight of n, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
func (s *Weighted) Acquire(ctx context.Context) error {
	s.mu.Lock()
	if s.cur <= 0 && s.waiters.Len() == 0 {
		s.cur++
		s.mu.Unlock()
		return nil
	}

	w := s.reqPool.Get().(chan struct{})
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-w:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancellation.
			err = nil
			s.reqPool.Put(w)
		default:
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			// If we're at the front and there're extra tokens left, notify other waiters.
			if isFront && 1 > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return err

	case <-w:
		s.reqPool.Put(w)
		return nil
	}
}

// TryAcquire acquires the semaphore with a weight of n without blocking.
// On success, returns true. On failure, returns false and leaves the semaphore unchanged.
func (s *Weighted) TryAcquire() bool {
	s.mu.Lock()
	success := s.cur <= 0 && s.waiters.Len() == 0
	if success {
		s.cur++
	}
	s.mu.Unlock()
	return success
}

// Release releases the semaphore with a weight of n.
func (s *Weighted) Release() {
	s.mu.Lock()
	s.cur--
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
	s.mu.Unlock()
}

func (s *Weighted) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(chan struct{})
		if s.cur < 0 {
			// Not enough tokens for the next waiter.  We could keep going (to try to
			// find a waiter with a smaller request), but under load that could cause
			// starvation for large requests; instead, we leave all remaining waiters
			// blocked.
			//
			// Consider a semaphore used as a read-write lock, with N tokens, N
			// readers, and one writer.  Each reader can LockContext(1) to obtain a read
			// lock.  The writer can LockContext(N) to obtain a write lock, excluding all
			// of the readers.  If we allow the readers to jump ahead in the queue,
			// the writer will starve — there is always one token available for every
			// reader.
			break
		}

		s.cur++
		s.waiters.Remove(next)
		w <- struct{}{}
	}
}

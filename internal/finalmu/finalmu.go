// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package finalmu provides a weighted semaphore implementation.
package finalmu

import (
	"container/list"
	"context"
	"sync"
)

var _ sync.Locker = (*Mutex)(nil)

const n = 1

type waiter chan struct{}

func New() *Mutex {
	w := &Mutex{}
	return w
}

// Mutex provides a way to bound concurrent access to a resource.
// The callers can request access with a given weight.
type Mutex struct {
	cur     int64
	mu      sync.Mutex
	waiters list.List
}

// LockContext acquires the semaphore with a weight of n, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, LockContext may still succeed without blocking.
func (s *Mutex) LockContext(ctx context.Context) error {
	s.mu.Lock()
	if n-s.cur >= n && s.waiters.Len() == 0 {
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	w := waiter(make(chan struct{}))
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		s.mu.Lock()
		select {
		case <-w:
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancelation.
			err = nil
		default:
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			// If we're at the front and there's extra tokens left, notify other waiters.
			if isFront && n > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return err

	case <-w:
		return nil
	}
}

func (s *Mutex) Lock() {
	_ = s.LockContext(context.Background())
	return
}

// TryLock acquires the semaphore with a weight of n without blocking.
// On success, returns true. On failure, returns false and leaves the semaphore unchanged.
func (s *Mutex) TryLock() bool {
	s.mu.Lock()
	success := n-s.cur >= n && s.waiters.Len() == 0
	if success {
		s.cur += n
	}
	s.mu.Unlock()
	return success
}

// Unlock releases the semaphore with a weight of n.
func (s *Mutex) Unlock() {
	s.mu.Lock()
	s.cur -= n
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
	s.mu.Unlock()
}

func (s *Mutex) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if n-s.cur < n {
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

		s.cur += n
		s.waiters.Remove(next)
		w <- struct{}{}
	}
}

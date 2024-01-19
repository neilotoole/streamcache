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

// New returns a new Mutex ready for use.
func New() *Mutex {
	return &Mutex{waiterPool: sync.Pool{New: newWaiter}}
}

// Mutex is a mutual exclusion lock whose Lock method uses a FIFO queue
// to ensure fairness.
//
// A Mutex must not be copied after first use.
//
// The zero value for a Mutex is an unlocked mutex.
// Mutex implements the same methodset as sync.Mutex, so it can
// be used as a drop-in replacement.
type Mutex struct {
	waiterPool sync.Pool
	waiters    list.List
	cur        int64
	mu         sync.Mutex
}

// LockContext locks m.
//
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available or ctx is done.
//
// On failure, LockContext returns context.Cause(ctx.Err()) and
// leaves the mutex unchanged.
//
// If ctx is already done, LockContext may still succeed without blocking.
func (m *Mutex) LockContext(ctx context.Context) error {
	m.mu.Lock()
	if n-m.cur >= n && m.waiters.Len() == 0 {
		m.cur += n
		m.mu.Unlock()
		return nil
	}

	if m.waiterPool.New == nil {
		// Lazy init so that the zero value of Mutex is usable,
		// like sync.Mutex.
		m.waiterPool.New = newWaiter
	}

	w := m.waiterPool.Get().(waiter)
	elem := m.waiters.PushBack(w)
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		err := context.Cause(ctx)
		m.mu.Lock()
		select {
		case <-w:
			// Acquired the lock after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancellation.
			err = nil
			m.waiterPool.Put(w)
		default:
			isFront := m.waiters.Front() == elem
			m.waiters.Remove(elem)
			// If we're at the front and there's extra tokens left,
			// notify other waiters.
			if isFront && n > m.cur {
				m.notifyWaiters()
			}
		}
		m.mu.Unlock()
		return err

	case <-w:
		m.waiterPool.Put(w)
		return nil
	}
}

// Lock locks m.
//
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() {
	m.mu.Lock()
	if n-m.cur >= n && m.waiters.Len() == 0 {
		m.cur += n
		m.mu.Unlock()
		return
	}

	w := m.waiterPool.Get().(waiter)
	_ = m.waiters.PushBack(w)
	m.mu.Unlock()

	<-w
	m.waiterPool.Put(w)
	return
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *Mutex) TryLock() bool {
	m.mu.Lock()
	success := n-m.cur >= n && m.waiters.Len() == 0
	if success {
		m.cur += n
	}
	m.mu.Unlock()
	return success
}

// Unlock unlocks m.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *Mutex) Unlock() {
	m.mu.Lock()
	m.cur -= n
	if m.cur < 0 {
		m.mu.Unlock()
		panic("semaphore: released more than held")
	}
	m.notifyWaiters()
	m.mu.Unlock()
}

func (m *Mutex) notifyWaiters() {
	for {
		next := m.waiters.Front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value.(waiter)
		if n-m.cur < n {
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

		m.cur += n
		_ = m.waiters.Remove(next)
		w <- struct{}{}
	}
}

type waiter chan struct{}

func newWaiter() any {
	return waiter(make(chan struct{}))
}

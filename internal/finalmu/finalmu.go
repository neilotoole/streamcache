// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package finalmu provides a weighted semaphore implementation.
package finalmu

import (
	"context"
	"sync"
)

var _ sync.Locker = (*Mutex)(nil)

// New returns a new Mutex ready for use.
func New() *Mutex {
	return &Mutex{}
}

// Mutex is a mutual exclusion lock whose Lock method returns
// the lock to callers in FIFO call order.
//
// A Mutex must not be copied after first use.
//
// The zero value for a Mutex is an unlocked mutex.
// Mutex implements the same methodset as sync.Mutex, so it can
// be used as a drop-in replacement. It implements an additional
// method Mutex.LockContext, which provides context-aware locking.
type Mutex struct {
	waiters list[waiter]
	cur     int64
	mu      sync.Mutex
}

// Lock locks m.
//
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() {
	m.mu.Lock()
	if 1-m.cur >= 1 && m.waiters.len == 0 {
		m.cur++
		m.mu.Unlock()
		return
	}

	w := waiterPool.Get().(waiter) //nolint:errcheck
	m.waiters.pushBack(w)
	m.mu.Unlock()

	<-w
	waiterPool.Put(w)
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
	if 1-m.cur >= 1 && m.waiters.len == 0 {
		m.cur++
		m.mu.Unlock()
		return nil
	}

	w := waiterPool.Get().(waiter) //nolint:errcheck
	elem := m.waiters.pushBackElem(w)
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
			waiterPool.Put(w)
		default:
			isFront := m.waiters.front() == elem
			m.waiters.remove(elem)
			// If we're at the front and there's extra tokens left,
			// notify other waiters.
			if isFront && m.cur < 1 {
				// if isFront && 1 > m.cur {
				m.notifyWaiters()
			}
		}
		m.mu.Unlock()
		return err

	case <-w:
		waiterPool.Put(w)
		return nil
	}
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *Mutex) TryLock() bool {
	m.mu.Lock()
	success := 1-m.cur >= 1 && m.waiters.len == 0
	if success {
		m.cur++
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
	m.cur--
	if m.cur < 0 {
		m.mu.Unlock()
		panic("sync: unlock of unlocked mutex")
	}
	m.notifyWaiters()
	m.mu.Unlock()
}

func (m *Mutex) notifyWaiters() {
	for {
		next := m.waiters.front()
		if next == nil {
			break // No more waiters blocked.
		}

		w := next.Value
		if m.cur > 0 {
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

		m.cur++
		m.waiters.remove(next)
		w <- struct{}{}
	}
}

var waiterPool = sync.Pool{New: func() any { return waiter(make(chan struct{})) }}

type waiter chan struct{}

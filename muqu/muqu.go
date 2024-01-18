// Package muqu provides a Mutex that uses a FIFO queue to
// ensure fairness. Mutex implements sync.Locker, so it can
// be used interchangeably with sync.Mutex. Performance is
// significantly worse than sync.Mutex, so it should only be
// used when fairness is required.
//
// Note that sync.Mutex's implementation discusses mutex
// fairness, where the sync.Mutex can be in 2 modes of operations:
// normal and starvation. Starvation mode kicks in when a waiter
// fails to acquire the mutex for more than 1ms. However, for
// some purposes waiting 1ms simply doesn't work, and FIFO
// fairness is required. This is where muqu.Mutex comes in.
package muqu

import (
	"sync"
)

// Mutex is a mutual exclusion lock whose Lock method uses a FIFO queue
// to ensure fairness.
// Use New to instantiate. Mutex implements sync.Locker, so it can
// be used as a drop-in replacement for sync.Mutex.
type Mutex struct {
	// lockCh indicates whether the lock is held.
	// A successful take from this channel acquires the lock.
	lockCh chan struct{}

	// reqQ is the queue of requests waiting for the lock.
	// The Queue impl is safe for concurrent use.
	reqQ *Queue[request]

	// reqPool caches request instances for reuse.
	reqPool sync.Pool
}

// New returns a new Mutex ready for use.
func New() *Mutex {
	m := &Mutex{
		reqPool: sync.Pool{New: func() interface{} {
			return request(make(chan struct{}, 1))
		}},
		lockCh: make(chan struct{}, 1),
		reqQ:   &Queue[request]{},
	}
	m.lockCh <- struct{}{}
	return m
}

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() {
	// Create a new request and enqueue it.
	req := m.newRequest()
	m.reqQ.Enqueue(req)

	for {
		select {
		case <-m.lockCh:
			// The lock is available, but needs to go to the
			// request at the front of the queue.
			frontReq, ok := m.reqQ.Dequeue()
			if !ok {
				// Should be impossible.
				panic("muqu: queue is empty")
			}

			// Notify the front request that it can proceed.
			frontReq <- struct{}{}
			// Now we loop, and enter the select again.
			// Basically, we're waiting until a signal is
			// received on req (which is just a chan struct{}).
		case <-req:
			// We got the signal, it's our turn! We've got the lock.
			// Put the request back in the pool for reuse, and return.
			m.reqPool.Put(req)
			return
		}
	}
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *Mutex) TryLock() bool {
	select {
	case <-m.lockCh:
		// The lock is available.
		if m.reqQ.Size() == 0 {
			// There's nobody waiting, so we can keep the lock,
			// and return.
			return true
		}

		// There's somebody already in the queue, put the lock back.
		m.lockCh <- struct{}{}
		// And return false, because we didn't get the lock.
		return false
	default:
		// The lock is not available.
		return false
	}
}

// Unlock unlocks m.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *Mutex) Unlock() {
	select {
	case m.lockCh <- struct{}{}:
		// Successfully unlocked.
	default:
		panic("muqu: unlock of unlocked mutex")
	}
}

type request chan struct{}

// newRequest returns a request from the pool, or creates a new one.
func (m *Mutex) newRequest() request {
	if req, ok := m.reqPool.Get().(request); ok && req != nil {
		return req
	}

	// Because we construct the sync.Pool with a New func, this
	// should be unreachable?
	req := make(chan struct{}, 1)
	m.reqPool.Put(req)
	return req
}

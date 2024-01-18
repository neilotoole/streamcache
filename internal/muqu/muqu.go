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
// fairness is required, hence the need for this package.
package muqu

import (
	"sync"
)

// Mutex is a mutual exclusion lock whose Lock method uses a FIFO queue
// to ensure fairness.
// The zero value for a Mutex is an unlocked mutex.
// Mutex implements the same methodset as sync.Mutex, so it can
// be used as a drop-in replacement.
type Mutex struct {
	// lockCh indicates whether the lock is held.
	// A successful take from this channel acquires the lock.
	lockCh chan struct{}

	// reqQ is the FIFO queue of requests waiting for the lock.
	// The queue impl is safe for concurrent use.
	reqQ *queue[request]

	// reqPool caches request instances for reuse.
	// For some applications, it may be normal for the Mutex to
	// be locked and unlocked millions of times, so we want to
	// avoid allocating millions of request instances.
	reqPool sync.Pool
}

// init exists so that the zero value of Mutex is usable, like sync.Mutex.
func (m *Mutex) init() {
	if m.lockCh == nil {
		m.lockCh = make(chan struct{}, 1)
		m.reqQ = &queue[request]{}
		m.reqPool = sync.Pool{New: func() interface{} {
			return request(make(chan struct{}, 1))
		}}
		// Initial send on lockCh because a new mutex
		// needs to start life unlocked.
		m.lockCh <- struct{}{}
	}
}

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() {
	m.init()
	req := m.reqPool.Get().(request)
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
	m.init()
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
	m.init()
	select {
	case m.lockCh <- struct{}{}:
		// Successfully unlocked.
	default:
		panic("sync: unlock of unlocked mutex")
	}
}

// request is a request for the lock. It is signalled
// via req<-struct{}{} in Mutex.Lock when the lock is
// available to the request.
type request chan struct{}

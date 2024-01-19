package stdsemamu

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

var _ sync.Locker = (*Mutex)(nil)

var ctx = context.Background()

func New() *Mutex {
	return &Mutex{sema: semaphore.NewWeighted(1)}
}

type Mutex struct {
	sema *semaphore.Weighted
}

func (m *Mutex) Lock() {
	_ = m.sema.Acquire(ctx, 1)
}

func (m *Mutex) Unlock() {
	m.sema.Release(1)
}

func (m *Mutex) TryLock() bool {
	return m.sema.TryAcquire(1)
}

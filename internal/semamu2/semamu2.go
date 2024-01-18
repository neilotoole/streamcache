package semamu2

import (
	"context"
	"sync"
)

var _ sync.Locker = (*Mutex)(nil)

var ctx = context.Background()

func New() *Mutex {
	return &Mutex{}
}

type Mutex struct {
	sema Weighted
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

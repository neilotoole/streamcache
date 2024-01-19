package semamu2

import (
	"context"
	"sync"
)

var _ sync.Locker = (*Mutex)(nil)

var ctx = context.Background()

func New() *Mutex {
	return &Mutex{sema: NewWeighted()}
}

type Mutex struct {
	sema *Weighted
}

func (m *Mutex) Lock() {
	_ = m.sema.Acquire(ctx)
}

func (m *Mutex) Unlock() {
	m.sema.Release()
}

func (m *Mutex) TryLock() bool {
	return m.sema.TryAcquire()
}

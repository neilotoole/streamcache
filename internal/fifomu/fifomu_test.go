// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// GOMAXPROCS=10 go test

package fifomu_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/neilotoole/streamcache/internal/fifomu"
)

// Acknowledgement: Much of the test code in this file is
// copied from stdlib sync/mutex_test.go.

// mutexer is the exported methodset of sync.Mutex.
type mutexer interface {
	sync.Locker
	TryLock() bool
}

var (
	_ mutexer = (*fifomu.Mutex)(nil)
	_ mutexer = (*sync.Mutex)(nil)
	_ mutexer = (*semaphoreMutex)(nil)
)

// newMu is a function that returns a new mutexer.
// We set it to newFifoMu, newStdlibMu or newSemaphoreMu
// for benchmarking.
var newMu = newFifoMu

func newFifoMu() mutexer {
	return &fifomu.Mutex{}
}

func newStdlibMu() mutexer {
	return &sync.Mutex{}
}

func newSemaphoreMu() mutexer {
	return &semaphoreMutex{sema: semaphore.NewWeighted(1)}
}

func benchmarkEachImpl(b *testing.B, fn func(b *testing.B)) {
	b.Cleanup(func() {
		// Restore to default.
		newMu = newFifoMu
	})

	b.Run("stdlib", func(b *testing.B) {
		b.ReportAllocs()
		newMu = newStdlibMu
		fn(b)
	})
	b.Run("fifomu", func(b *testing.B) {
		b.ReportAllocs()
		newMu = newFifoMu
		fn(b)
	})
	b.Run("semaphoreMu", func(b *testing.B) {
		b.ReportAllocs()
		newMu = newSemaphoreMu
		fn(b)
	})
}

func HammerMutex(m mutexer, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		if i%3 == 0 {
			if m.TryLock() {
				m.Unlock()
			}
			continue
		}
		m.Lock()
		m.Unlock() //nolint:staticcheck
	}
	cdone <- true
}

func TestMutex(t *testing.T) {
	if n := runtime.SetMutexProfileFraction(1); n != 0 {
		t.Logf("got mutexrate %d expected 0", n)
	}
	defer runtime.SetMutexProfileFraction(0)

	m := newMu()

	m.Lock()
	if m.TryLock() {
		t.Fatalf("TryLock succeeded with mutex locked")
	}
	m.Unlock()
	if !m.TryLock() {
		t.Fatalf("TryLock failed with mutex unlocked")
	}
	m.Unlock()

	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerMutex(m, 1000, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}

func TestMutexFairness(t *testing.T) {
	mu := newMu()
	stop := make(chan bool)
	defer close(stop)
	go func() {
		for {
			mu.Lock()
			time.Sleep(100 * time.Microsecond)
			mu.Unlock()
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	done := make(chan bool, 1)
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Microsecond)
			mu.Lock()
			mu.Unlock() //nolint:staticcheck
		}
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("can't acquire mutex in 10 seconds")
	}
}

func BenchmarkMutexUncontended(b *testing.B) {
	type PaddedMutex struct {
		mutexer
		pad [128]uint8 //nolint:unused
	}

	benchmarkEachImpl(b, func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			var mu PaddedMutex
			mu.mutexer = newMu()
			for pb.Next() {
				mu.Lock()
				mu.Unlock() //nolint:staticcheck
			}
		})
	})
}

func benchmarkMutex(b *testing.B, slack, work bool) {
	b.ReportAllocs()
	mu := newMu()
	if slack {
		b.SetParallelism(10)
	}
	b.RunParallel(func(pb *testing.PB) {
		foo := 0
		for pb.Next() {
			mu.Lock()
			mu.Unlock() //nolint:staticcheck
			if work {
				for i := 0; i < 100; i++ {
					foo *= 2
					foo /= 2
				}
			}
		}
		_ = foo
	})
}

func BenchmarkMutex(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		benchmarkMutex(b, false, false)
	})
}

func BenchmarkMutexSlack(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		benchmarkMutex(b, true, false)
	})
}

func BenchmarkMutexWork(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		benchmarkMutex(b, false, true)
	})
}

func BenchmarkMutexWorkSlack(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		benchmarkMutex(b, true, true)
	})
}

func BenchmarkMutexNoSpin(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		// This benchmark models a situation where spinning in the mutex should be
		// non-profitable and allows to confirm that spinning does not do harm.
		// To achieve this we create excess of goroutines most of which do local work.
		// These goroutines yield during local work, so that switching from
		// a blocked goroutine to other goroutines is profitable.
		// As a matter of fact, this benchmark still triggers some spinning in the mutex.
		m := newMu()
		var acc0, acc1 uint64
		b.SetParallelism(4)
		b.RunParallel(func(pb *testing.PB) {
			c := make(chan bool)
			var data [4 << 10]uint64
			for i := 0; pb.Next(); i++ {
				if i%4 == 0 {
					m.Lock()
					acc0 -= 100
					acc1 += 100
					m.Unlock()
				} else {
					for i := 0; i < len(data); i += 4 {
						data[i]++
					}
					// Elaborate way to say runtime.Gosched
					// that does not put the goroutine onto global runq.
					go func() {
						c <- true
					}()
					<-c
				}
			}
		})
	})
}

func BenchmarkMutexSpin(b *testing.B) {
	benchmarkEachImpl(b, func(b *testing.B) {
		// This benchmark models a situation where spinning in the mutex should be
		// profitable. To achieve this we create a goroutine per-proc.
		// These goroutines access considerable amount of local data so that
		// unnecessary rescheduling is penalized by cache misses.
		m := newMu()
		var acc0, acc1 uint64
		b.RunParallel(func(pb *testing.PB) {
			var data [16 << 10]uint64
			for i := 0; pb.Next(); i++ {
				m.Lock()
				acc0 -= 100
				acc1 += 100
				m.Unlock()
				for i := 0; i < len(data); i += 4 {
					data[i]++
				}
			}
		})
	})
}

var _ sync.Locker = (*semaphoreMutex)(nil)

// semaphoreMutex is a mutex built on a semaphore.Weighted.
// It exists as a baseline for benchmarking. Like fifomu.Mutex,
// its Lock method returns the lock to callers in FIFO call order.
type semaphoreMutex struct {
	sema *semaphore.Weighted
}

// Lock implements sync.Locker.
func (m *semaphoreMutex) Lock() {
	_ = m.sema.Acquire(context.Background(), 1)
}

// Unlock implements sync.Locker.
func (m *semaphoreMutex) Unlock() {
	m.sema.Release(1)
}

// TryLock tries to lock m and reports whether it succeeded.
func (m *semaphoreMutex) TryLock() bool {
	return m.sema.TryAcquire(1)
}

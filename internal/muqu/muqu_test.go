package muqu_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/neilotoole/streamcache/internal/muqu"
)

var (
	_ sync.Locker = (*muqu.Mutex)(nil)
	_ sync.Locker = (*sync.Mutex)(nil)
)

func sleepy() {
	// time.Sleep(time.Second * 1)
	time.Sleep(time.Millisecond * 2)
}

const (
	iterations              = 10
	alice, bob, carol, dave = "alice", "bob", "carol", "dave"
)

func TestLockQueue2(t *testing.T) {
	mu := muqu.New()
	t.Logf("About to acquire")

	wg := &sync.WaitGroup{}
	// wg.Add(2)
	wg.Add(2)
	go func() {
		defer wg.Done()

		// t.Logf("1: Acquiring")

		for i := 0; i < iterations; i++ {
			sleepy()
			fmt.Println("Alice REQUEST")
			mu.Lock()
			fmt.Println("Alice working")
			sleepy()
			fmt.Println("Alice releasing")
			mu.Unlock()
			// sleepy()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			sleepy()
			fmt.Println("Bob REQUEST")
			mu.Lock()
			fmt.Println("Bob working")
			sleepy()
			fmt.Println("Bob releasing")
			mu.Unlock()
			// sleepy()
		}
	}()

	t.Logf("Waiting")
	wg.Wait()
	t.Logf("Done waiting")
}

// Copied from sync/mutex_test.go.
func TestMutex(t *testing.T) {
	if n := runtime.SetMutexProfileFraction(1); n != 0 {
		t.Logf("got mutexrate %d expected 0", n)
	}
	defer runtime.SetMutexProfileFraction(0)

	m := muqu.New()
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

// HammerMutex is copied from sync/mutex_test.go.
func HammerMutex(m *muqu.Mutex, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		if i%3 == 0 {
			if m.TryLock() {
				m.Unlock()
			}
			continue
		}
		m.Lock()
		m.Unlock()
	}
	cdone <- true
}

package muqu

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var _ sync.Locker = (*Mutex)(nil)
var _ sync.Locker = (*sync.Mutex)(nil)


func sleepy() {
	//time.Sleep(time.Second * 1)
	time.Sleep(time.Millisecond * 2)
}

const iterations = 10
const alice, bob, carol, dave = "alice", "bob", "carol", "dave"

func TestLockQueue2(t *testing.T) {
	mu := New()
	t.Logf("About to acquire")

	wg := &sync.WaitGroup{}
	//wg.Add(2)
	wg.Add(2)
	go func() {
		defer wg.Done()

		//t.Logf("1: Acquiring")

		for i := 0; i < iterations; i++ {
			sleepy()
			fmt.Println("Alice REQUEST")
			mu.Lock()
			fmt.Println("Alice working")
			sleepy()
			fmt.Println("Alice releasing")
			mu.Unlock()
			//sleepy()
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
			//sleepy()
		}
	}()

	t.Logf("Waiting")
	wg.Wait()
	t.Logf("Done waiting")
}

//
//func TestQ_TryLock1(t *testing.T) {
//	q := muqu.New()
//
//	ul1, ok := q.TryLock(alice)
//	require.True(t, ok)
//	require.NotNil(t, ul1)
//
//	ul2, ok := q.TryLock(bob)
//	require.False(t, ok)
//	require.Nil(t, ul2)
//
//	go q.Lock(carol)
//	go q.Lock(dave)
//
//	time.Sleep(time.Second)
//	ul2, ok = q.TryLock(bob)
//	require.False(t, ok)
//	require.Nil(t, ul2)
//}
//
//func TestQ_TryLock2(t *testing.T) {
//	q := muqu.New()
//
//	ul1, ok := q.TryLock(alice)
//	require.True(t, ok)
//	require.NotNil(t, ul1)
//
//	ul2, ok := q.TryLock(bob)
//	require.False(t, ok)
//	require.Nil(t, ul2)
//
//	go func() {
//		ul := q.Lock(carol)
//		defer ul()
//		t.Logf("carol working")
//	}()
//	go func() {
//		ul := q.Lock(dave)
//		defer ul()
//		t.Logf("dave working")
//	}()
//
//	time.Sleep(time.Second)
//	ul2, ok = q.TryLock(bob)
//	require.False(t, ok)
//	require.Nil(t, ul2)
//}

package lockqueue_test

import (
	"github.com/neilotoole/streamcache/lockqueue"
	"sync"
	"testing"
	"time"
)

func sleepy() {
	//time.Sleep(time.Second * 1)
	time.Sleep(time.Millisecond * 10)
}

const iterations = 10

func TestLockQueue(t *testing.T) {
	q := lockqueue.NewQ()
	t.Logf("About to acquire")

	wg := &sync.WaitGroup{}
	//wg.Add(2)
	wg.Add(2)
	go func() {
		defer wg.Done()

		//t.Logf("1: Acquiring")

		for i := 0; i < iterations; i++ {
			sleepy()
			unlock := q.Lock("alice")
			sleepy()
			unlock()
			//sleepy()
		}

	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			sleepy()
			unlock := q.Lock("bob")
			sleepy()
			unlock()
			//sleepy()
		}
	}()

	t.Logf("Waiting")
	wg.Wait()
	t.Logf("Done waiting")
}

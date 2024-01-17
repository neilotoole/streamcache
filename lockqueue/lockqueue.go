package lockqueue

import (
	"fmt"
	"github.com/oleiade/lane/v2"
	"sync"
)

type request struct {
	ch   chan Unlock
	name string
}

type request2 chan Unlock

func newRequest(name string) *request {

	return &request{
		ch:   make(chan Unlock, 1),
		name: name,
	}
}

type Unlock func()

type Q struct {
	muCh     chan *sync.Mutex
	requests *lane.Queue[*request]
}

func NewQ() *Q {
	mu := &sync.Mutex{}

	q := &Q{
		muCh:     make(chan *sync.Mutex, 1),
		requests: lane.NewQueue[*request](),
	}
	q.muCh <- mu
	return q
}

func (q *Q) Lock(name string) Unlock {
	fmt.Println(name, "acquiring...")

	req := newRequest(name)
	q.requests.Enqueue(req)

	//var unlock Unlock
	for {
		select {
		case unlock := <-req.ch:
			return unlock
		case mu := <-q.muCh:
			headReq, ok := q.requests.Dequeue()
			if !ok {
				panic("queue is empty")
			}
			//fmt.Println(name, "Locking mu")
			//mu.Lock()
			fmt.Println(name, "locked")

			headReq.ch <- func() {
				//fmt.Println(name, "Unlocking mu")
				//mu.Unlock()
				fmt.Println(name, "unlocked")
				q.muCh <- mu
			}
			//fmt.Println(name, "Sent unlock func")
		}
	}

}

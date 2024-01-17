package muquold

import (
	"github.com/oleiade/lane/v2"
	"sync"
)

//type request struct {
//	ch   chan Unlock
//	name string
//}

type request2 chan Unlock

func (m *Mutex) newRequest(name string) request2 {
	if r := m.pool.Get().(request2); r != nil {
		return r
	}

	//fmt.Println("allocating new request")
	return make(chan Unlock, 1)
	//return make(chan request2, 1)
	//return &request{
	//	ch:   make(chan Unlock, 1),
	//	name: name,
	//}
}

type Unlock func()

type Mutex struct {
	pool sync.Pool
	//muCh     chan *sync.Mutex
	muCh     chan struct{}
	requests *lane.Queue[request2]
	// See also: https://betterprogramming.pub/golang-queue-implementation-analysis-209629eb600d

}

func New() *Mutex {
	//mu := struct{}{}

	m := &Mutex{
		pool: sync.Pool{New: func() interface{} {
			//fmt.Println("allocating new request via pool.New")
			var req request2
			req = make(chan Unlock, 1)
			return req
		}},
		muCh:     make(chan struct{}, 1),
		requests: lane.NewQueue[request2](),
	}
	m.muCh <- struct{}{}
	return m
}

func (m *Mutex) TryLock(name string) (Unlock, bool) {
	select {
	case <-m.muCh:
		if m.requests.Size() == 0 {
			// There's nobody waiting, so it's all good.
			//fmt.Println(name, "try lock SUCCESS")
			return func() {
				//fmt.Println(name, "release lock")
				m.muCh <- struct{}{}
			}, true
		}
		//fmt.Println(name, "try lock FAILURE")
		// There's somebody already in the queue, put
		// the mu back!
		m.muCh <- struct{}{}
		return nil, false
	default:
		//fmt.Println(name, "try lock FAILURE")
		return nil, false
	}
}
func (m *Mutex) TryLock2() bool {
	select {
	case <-m.muCh:
		if m.requests.Size() == 0 {
			// There's nobody waiting, so it's all good.
			//fmt.Println(name, "try lock SUCCESS")
			return true
		}
		//fmt.Println(name, "try lock FAILURE")
		// There's somebody already in the queue, put
		// the mu back!
		m.muCh <- struct{}{}
		return false
	default:
		//fmt.Println(name, "try lock FAILURE")
		return false
	}
}

func (m *Mutex) Unlock() {
	select {
	case m.muCh <- struct{}{}:
	default:
		panic("unlocking unlocked mutex")
	}
}

func (m *Mutex) Lock2() {
	req := m.newRequest("anon")
	m.requests.Enqueue(req)

	//var unlock Unlock
	for {
		select {
		case <-req:
			return
		case <-m.muCh:
			headReq, ok := m.requests.Dequeue()
			if !ok {
				panic("queue is empty")
			}
			//fmt.Println(name, "Locking mu")
			//mu.Lock()
			//fmt.Println(name, "locked")

			headReq <- func() {
				//fmt.Println(name, "Unlocking mu")
				//mu.Unlock()
				//fmt.Println(name, "unlocked")
				m.muCh <- struct{}{}
				m.pool.Put(headReq)
			}
			//fmt.Println(name, "Sent unlock func")
		}
	}
}

func (m *Mutex) Lock(name string) Unlock {
	//fmt.Println(name, "REQUEST")

	// Use a sync.Pool instead of creating new requests
	req := m.newRequest(name)
	m.requests.Enqueue(req)

	//var unlock Unlock
	for {
		select {
		case unlock := <-req:
			return unlock
		case <-m.muCh:
			headReq, ok := m.requests.Dequeue()
			if !ok {
				panic("queue is empty")
			}
			//fmt.Println(name, "Locking mu")
			//mu.Lock()
			//fmt.Println(name, "locked")

			headReq <- func() {
				//fmt.Println(name, "Unlocking mu")
				//mu.Unlock()
				//fmt.Println(name, "unlocked")
				m.muCh <- struct{}{}
				m.pool.Put(headReq)
			}
			//fmt.Println(name, "Sent unlock func")
		}
	}

}

func (m *Mutex) unlockFunc() {
	m.muCh <- struct{}{}
}

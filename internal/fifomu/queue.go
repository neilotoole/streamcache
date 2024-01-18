package fifomu

import (
	"sync"
)

// queue is a FIFO data structure. Each method has complexity O(1).
// It is safe for concurrent use.
type queue[T any] struct {
	list *list[T]
	mu   sync.RWMutex
}

// Enqueue adds an item at the back of the queue.
func (q *queue[T]) Enqueue(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.list == nil {
		q.list = newList[T]()
	}

	q.list.PushFront(item)
}

// Dequeue removes the front item from the queue.
func (q *queue[T]) Dequeue() (item T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.list == nil {
		return item, false
	}

	lastElement := q.list.Back()
	if lastElement != nil {
		item = q.list.Remove(lastElement)
		ok = true
	}

	return item, ok
}

// Head returns the front item without removing it.
func (q *queue[T]) Head() (item T, ok bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.list == nil {
		return item, ok
	}

	if backItem := q.list.Back(); backItem != nil {
		item = backItem.Value
		ok = true
	}

	return item, ok
}

// Size returns the size of the queue.
func (q *queue[T]) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.list == nil {
		return 0
	}

	return q.list.Len()
}

// list is a doubly-linked list. It is copied from container/list.List.
type list[T any] struct {
	root element[T]
	len  int
}

// init initializes or clears list l.
func (l *list[T]) init() *list[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0

	return l
}

// newList returns an initialized list.
func newList[T any]() *list[T] {
	return new(list[T]).init()
}

// Len returns the number of elements of list l.
func (l *list[T]) Len() int {
	return l.len
}

// Back returns the last element of list l or nil.
func (l *list[T]) Back() *element[T] {
	if l.len == 0 {
		return nil
	}

	return l.root.prev
}

// PushFront inserts a new element e with value v at
// the front of list l and returns e.
func (l *list[T]) PushFront(v T) *element[T] {
	return l.insertValue(v, &l.root)
}

// Remove removes e from l if e is an element of list l.
func (l *list[T]) Remove(e *element[T]) T {
	if e.list == l {
		l.remove(e)
	}

	return e.Value
}

func (l *list[T]) insert(e, at *element[T]) *element[T] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++

	return e
}

func (l *list[T]) insertValue(v T, at *element[T]) *element[T] {
	return l.insert(&element[T]{Value: v}, at)
}

func (l *list[T]) remove(e *element[T]) *element[T] {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
	return e
}

// element is a node of a linked list.
type element[T any] struct {
	next, prev *element[T]

	list *list[T]

	Value T
}

// Next returns the next list element or nil.
func (e *element[T]) Next() *element[T] {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}

	return nil
}

// Prev returns the previous list element or nil.
func (e *element[T]) Prev() *element[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}

	return nil
}

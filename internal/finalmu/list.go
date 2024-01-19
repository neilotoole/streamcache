package finalmu

import "sync"

var elementPool = sync.Pool{New: func() any { return new(element[waiter]) }}

// list represents a doubly linked list.
type list[T any] struct {
	root element[T]
	len  uint
}

// Init initializes or clears list l.
func (l *list[T]) Init() *list[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0

	return l
}

func (l *list[T]) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// Front returns the first element of list l or nil.
func (l *list[T]) Front() *element[T] {
	if l.len == 0 {
		return nil
	}

	return l.root.next
}

// PushBack inserts a new element e with value v at
// the back of list l and returns e.
func (l *list[T]) PushBack(v T) *element[T] {
	l.lazyInit()

	return l.insertValue(v, l.root.prev)
}

// Remove removes e from l if e is an element of list l.
func (l *list[T]) Remove(e *element[T]) {
	if e.list == l {
		l.remove(e)
	}

	elementPool.Put(e)
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
	e := elementPool.Get().(*element[T])
	e.Value = v
	return l.insert(e, at)
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

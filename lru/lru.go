package lru

import "math"

const nullPtr uint32 = math.MaxUint32

// LRU ...
type LRU struct {
	next uint32
	prev uint32
	size uint32
}

// New ...
func New() *LRU {
	return &LRU{
		next: nullPtr,
		prev: nullPtr,
	}
}

// Put ...
func (l *LRU) Put(hash uint32) (uint32, bool) {
	return 0, false
}

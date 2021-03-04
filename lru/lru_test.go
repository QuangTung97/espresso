package lru

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewLRU(t *testing.T) {
	l := New()
	assert.Equal(t, uint32(0), l.size)
	assert.Equal(t, nullPtr, l.next)
	assert.Equal(t, nullPtr, l.prev)
}

func TestLRU_Put(t *testing.T) {
	l := New()
	addr, ok := l.Put(2233)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), addr)
}

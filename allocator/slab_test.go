package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestSlab_Init(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 20, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 100, 12)
	assert.Equal(t, &buddy, slab.buddy)
	assert.Equal(t, uint32(100), slab.elemSize)
	assert.Equal(t, uint32(12), slab.chunkSizeLog)
	assert.Equal(t, uint32((1<<12)/100), slab.numElemPerChunk)
	assert.Equal(t, buddyNullPtr, slab.freeList)
}

func TestSlab_Allocate(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 20, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 128, 12)

	p, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p)

	p, ok = slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(128), p)
}

package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChunkManager_Init(t *testing.T) {
	conf := ChunkConfig{
		ChunkSizeLog:    20,
		MinAllocSizeLog: 12,
		MaxNumChunks:    5,
	}
	chunk := NewChunkManager(conf)
	assert.Equal(t, uint32(20), chunk.chunkSizeLog)
	assert.Equal(t, uint32(12), chunk.minAllocSizeLog)
	assert.Equal(t, uint16(5), chunk.maxNumChunks)
	assert.Equal(t, 0, len(chunk.chunks))
	assert.Equal(t, 5, cap(chunk.chunks))
}

func TestChunkManager_Allocate_Deallocate(t *testing.T) {
	conf := ChunkConfig{
		ChunkSizeLog:    20,
		MinAllocSizeLog: 12,
		MaxNumChunks:    5,
	}
	chunk := NewChunkManager(conf)
	p := chunk.Allocate(16)
	assert.Equal(t, uintptr(0), p)
}

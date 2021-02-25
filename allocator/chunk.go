package allocator

// ChunkConfig ...
type ChunkConfig struct {
	ChunkSizeLog    uint32
	MinAllocSizeLog uint32
	MaxNumChunks    uint16
}

// ChunkManager ...
type ChunkManager struct {
	chunkSizeLog    uint32
	minAllocSizeLog uint32
	maxNumChunks    uint16
	chunks          []Buddy
}

// NewChunkManager ...
func NewChunkManager(conf ChunkConfig) *ChunkManager {
	return &ChunkManager{
		chunkSizeLog:    conf.ChunkSizeLog,
		minAllocSizeLog: conf.MinAllocSizeLog,
		maxNumChunks:    conf.MaxNumChunks,
		chunks:          make([]Buddy, 0, conf.MaxNumChunks),
	}
}

// Allocate ...
func (c *ChunkManager) Allocate(sizeLog uint32) uintptr {
	return 0
}

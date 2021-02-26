package allocator

import (
	"math"
	"unsafe"
)

const (
	buddyNullPtr uint32 = math.MaxUint32
)

// Buddy ...
type Buddy struct {
	minSize      uint32
	maxSize      uint32
	sizeMultiple uint32
	data         unsafe.Pointer
	buckets      []uint32
	bitset       []uint64
}

type buddyListHead struct {
	next         uint32
	prev         uint32
	bucketOffset uint32
}

func findSizeLogList(sizeMultiple uint32) []uint32 {
	var result []uint32
	for pos := uint32(0); sizeMultiple != 0; pos++ {
		if sizeMultiple&0x1 != 0 {
			result = append(result, pos)
		}
		sizeMultiple >>= 1
	}
	return result
}

func makeBitSet(sizeMultiple uint32) []uint64 {
	if sizeMultiple <= 64 {
		return make([]uint64, 1)
	}
	return make([]uint64, (sizeMultiple+63)>>6)
}

func clearBitSet(bitset []uint64) {
	for i := range bitset {
		bitset[i] = 0
	}
}

// BuddyInit ...
func BuddyInit(b *Buddy, minSizeLog uint32, sizeMultiple uint32, data unsafe.Pointer) {
	sizeLogList := findSizeLogList(sizeMultiple)
	last := sizeLogList[len(sizeLogList)-1]
	maxSizeLog := last + minSizeLog

	b.minSize = minSizeLog
	b.maxSize = maxSizeLog
	b.sizeMultiple = sizeMultiple
	b.data = data
	b.buckets = make([]uint32, maxSizeLog-minSizeLog+1)

	b.bitset = makeBitSet(sizeMultiple)
	clearBitSet(b.bitset)

	for i := uint32(0); i <= last; i++ {
		b.buckets[i] = buddyNullPtr
	}

	addr := uint32(0)
	for i := len(sizeLogList) - 1; i >= 0; i-- {
		sizeLog := sizeLogList[i]
		node := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(addr)))
		buddyAddListHead(data, &b.buckets[sizeLog], sizeLog, node)
		b.setBit(addr)

		addr += 1 << (sizeLog + minSizeLog)
	}
}

func (b *Buddy) setBit(addr uint32) {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := uint64(1 << pos)
	b.bitset[index>>6] |= mask
}

func (b *Buddy) clearBit(addr uint32) {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := ^uint64(1 << pos)
	b.bitset[index>>6] &= mask
}

func (b *Buddy) isBitSet(addr uint32) bool {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := uint64(1 << pos)
	return b.bitset[index>>6]&mask != 0
}

func buddyAddListHead(data unsafe.Pointer, root *uint32, offset uint32, node *buddyListHead) {
	nodeAddr := uint32(uintptr(unsafe.Pointer(node)) - uintptr(data))
	if *root != buddyNullPtr {
		next := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(*root)))
		next.prev = nodeAddr
	}

	node.next = *root
	node.prev = buddyNullPtr
	node.bucketOffset = offset
	*root = nodeAddr
}

func buddyRemoveListHead(data unsafe.Pointer, root *uint32, node *buddyListHead) {
	if node.next != buddyNullPtr {
		next := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(node.next)))
		next.prev = node.prev
	}

	if node.prev != buddyNullPtr {
		prev := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(node.prev)))
		prev.next = node.next
	} else {
		*root = node.next
	}
}

func (b *Buddy) contentOfList(order uint32) []uint32 {
	var result []uint32
	offset := order - b.minSize

	addr := b.buckets[offset]
	for addr != buddyNullPtr {
		node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addr)))
		if node.bucketOffset == offset {
			result = append(result, addr)
		}
		addr = node.next
	}

	return result
}

// ToRealAddr ...
func (b *Buddy) ToRealAddr(addr uint32) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.data) + uintptr(addr))
}

// Allocate ...
func (b *Buddy) Allocate(sizeLog uint32) (uint32, bool) {
	offset := sizeLog - b.minSize
	maxOffset := b.maxSize - b.minSize
	emptyOffset := offset
	for ; emptyOffset <= maxOffset && b.buckets[emptyOffset] == buddyNullPtr; emptyOffset++ {
	}
	if emptyOffset > maxOffset {
		return 0, false
	}

	addrIndex := b.buckets[emptyOffset]
	header := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addrIndex)))
	b.buckets[emptyOffset] = header.next
	b.clearBit(addrIndex)

	if emptyOffset == offset {
		return addrIndex, true
	}

	for i := int(emptyOffset) - 1; i >= int(offset); i-- {
		p := addrIndex + (1 << (uint32(i) + b.minSize))
		node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(p)))

		buddyAddListHead(b.data, &b.buckets[i], uint32(i), node)
		b.setBit(p)
	}

	return addrIndex, true
}

func computeRootAndNeighborAddr(addr uint32, sizeLog uint32) (uint32, uint32) {
	mask := uint32(math.MaxUint32) << (sizeLog + 1)
	maskedAddr := addr & mask
	if maskedAddr == addr {
		return maskedAddr, addr + (1 << sizeLog)
	}
	return maskedAddr, maskedAddr
}

// Deallocate ...
func (b *Buddy) Deallocate(addr uint32, sizeLog uint32) {
	offset := sizeLog - b.minSize

	for sizeLog < b.maxSize {
		rootAddr, neighborAddr := computeRootAndNeighborAddr(addr, sizeLog)
		if (neighborAddr >> b.minSize) >= b.sizeMultiple {
			break
		}

		if !b.isBitSet(neighborAddr) {
			break
		}

		neighborHeader := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(neighborAddr)))
		if neighborHeader.bucketOffset != offset {
			break
		}

		buddyRemoveListHead(b.data, &b.buckets[offset], neighborHeader)
		b.clearBit(neighborAddr)

		addr = rootAddr
		sizeLog++
		offset++
	}

	node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addr)))
	buddyAddListHead(b.data, &b.buckets[offset], offset, node)
	b.setBit(addr)
}

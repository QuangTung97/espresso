package espresso

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRational(t *testing.T) {
	r := NewRational(2, 3)
	assert.Equal(t, uint64(2), r.Nominator)
	assert.Equal(t, uint64(3), r.Denominator)
}

func TestRational_MulUint32(t *testing.T) {
	r := NewRational(3, 5)
	result := r.MulUint32(22)
	assert.Equal(t, uint32(13), result)

	r = NewRational(80, 100)
	result = r.MulUint32(1 << 31)
	assert.Equal(t, uint32(1717986918), result)
}

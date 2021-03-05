package espresso

// Rational ...
type Rational struct {
	Nominator   uint64
	Denominator uint64
}

// NewRational ...
func NewRational(nominator uint64, denominator uint64) Rational {
	return Rational{
		Nominator:   nominator,
		Denominator: denominator,
	}
}

// MulUint32 ...
func (r Rational) MulUint32(v uint32) uint32 {
	return uint32(uint64(v) * r.Nominator / r.Denominator)
}

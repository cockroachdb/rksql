package numberutil

import (
	"errors"
	"math"
)

// ErrIntOverflow is the error representing integer overflow
var ErrIntOverflow = errors.New("integer overflow")

// Diff returns the difference between ll and ul as a uint64
func Diff(ll, ul int64) uint64 {
	if (ul > 0 && ll > 0) || (ul < 0 && ll < 0) {
		return uint64(ul - ll)
	}
	if ll == math.MinInt64 {
		return uint64(ul) + math.MaxInt64 + 1
	}
	return uint64(ul) + uint64(-ll)
}

// AddInt64UInt64 Adds an int64 and uint64 and returns an error if there
// is an overflow
func AddInt64UInt64(i int64, u uint64) (int64, error) {
	// for loop is executed twice only when u=MaxUint64 and i=MinInt64
	for u > uint64(math.MaxInt64) && i <= 0 {
		u -= math.MaxInt64
		i += math.MaxInt64
	}

	// Now if u > math.MaxInt64, i is > 0
	if u > uint64(math.MaxInt64) {
		return 0, ErrIntOverflow
	}

	// Now u <= math.MaxInt64 so it can be safely type casted to int64
	tu := int64(u) // type casted u
	if i > math.MaxInt64-tu {
		return 0, ErrIntOverflow
	}

	return i + tu, nil
}

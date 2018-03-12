package numberutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiff(t *testing.T) {
	a := assert.New(t)
	a.Equal(Diff(0, 5), uint64(5))
	a.Equal(Diff(math.MinInt64, 5), uint64(math.MaxInt64)+uint64(6))
	a.Equal(Diff(5, math.MaxInt64), uint64(math.MaxInt64-5))
	a.Equal(Diff(-5, math.MaxInt64), uint64(math.MaxInt64)+uint64(5))
	a.Equal(Diff(math.MinInt64, math.MaxInt64), uint64(math.MaxUint64))
}

type addInt64Uint64Cases struct {
	name string
	i    int64
	u    uint64
	sum  int64
	err  error
}

func runTests(t *testing.T, testCases []addInt64Uint64Cases) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			actual, err := AddInt64UInt64(tc.i, tc.u)
			a.Equal(tc.err, err)
			a.Equal(tc.sum, actual)
		})
	}
}
func TestAddInt64UInt64(t *testing.T) {
	cases := []addInt64Uint64Cases{
		{
			name: "i +ve no overflow",
			i:    100,
			u:    uint64(math.MaxInt64) - 200,
			sum:  math.MaxInt64 + -100,
			err:  nil,
		},
		{
			name: "i +ve no overflow edge case",
			i:    100,
			u:    uint64(math.MaxInt64) - 100,
			sum:  math.MaxInt64,
			err:  nil,
		},
		{
			name: "i +ve overflow",
			i:    900,
			u:    uint64(math.MaxInt64) + 1000,
			sum:  0,
			err:  ErrIntOverflow,
		},
		{
			name: "i +ve overflow edge case",
			i:    100,
			u:    uint64(math.MaxInt64) - 99,
			sum:  0,
			err:  ErrIntOverflow,
		},
		{
			name: "i -ve & u < MaxInt64",
			i:    -10000,
			u:    100000,
			sum:  90000,
			err:  nil,
		},
		{
			name: "i -ve & u > MaxInt64",
			i:    -10000,
			u:    uint64(math.MaxInt64) + 1000,
			sum:  math.MaxInt64 + -9000,
			err:  nil,
		},
		{
			name: "i MinInt64 & u MaxUint64",
			i:    math.MinInt64,
			u:    math.MaxUint64,
			sum:  math.MaxInt64,
			err:  nil,
		},
		{
			name: "i -ve & u > MaxInt64 overflow",
			i:    -100,
			u:    math.MaxUint64 - 105,
			sum:  0,
			err:  ErrIntOverflow,
		},
	}

	runTests(t, cases)
}

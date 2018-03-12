package sqload

import (
	"math/rand"
	"reflect"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name     string
	h        histogram
	errRegex *regexp.Regexp
	min      int64
}

func runTests(t *testing.T, testCases []testCase) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			err := tc.h.validateHistoInterval()
			if !isErrSimilar(tc.errRegex, err) {
				t.Errorf("want err %v, have: %v", tc.errRegex, err)
			}

			a.Equal(tc.min, tc.h.min())
		})
	}
}

func TestHistogramValidation(t *testing.T) {
	cases := []testCase{
		{
			name: "valid histogram single bucket",
			h: histogram{
				{
					Min:        10,
					Max:        1000,
					Percentage: 100,
				},
			},
			min: 10,
		},
		{
			name: "valid histogram multiple bucket",
			h: histogram{
				{
					Min:        100,
					Max:        200,
					Percentage: 30,
				},
				{
					Min:        75,
					Max:        250,
					Percentage: 50,
				},
				{
					Min:        200,
					Max:        500,
					Percentage: 20,
				},
			},
			min: 75,
		},
		{
			name: "valid histogram same min max",
			h: histogram{
				{
					Min:        10,
					Max:        10,
					Percentage: 100,
				},
			},
			min: 10,
		},
		{
			name: "invalid histogram min > max",
			h: histogram{
				{
					Min:        10,
					Max:        8,
					Percentage: 100,
				},
			},
			errRegex: regexp.MustCompile("min 10 is greater than max 8"),
			min:      10,
		},
		{
			name: "invalid histogram pct not summing to 100",
			h: histogram{
				{
					Min:        100,
					Max:        200,
					Percentage: 30,
				},
				{
					Min:        75,
					Max:        250,
					Percentage: 50,
				},
				{
					Min:        200,
					Max:        500,
					Percentage: 18,
				},
			},
			errRegex: regexp.MustCompile("class-intervals aren't mutually-exclusive and collectively-exhaustive on histogram 'len' as they sum up to '98.00%'"),
			min:      75,
		},
	}

	runTests(t, cases)
}

func TestHistogramSelection(t *testing.T) {
	a := assert.New(t)

	b1 := histoInterval{
		Min:        100,
		Max:        200,
		Percentage: 30,
	}
	b2 := histoInterval{
		Min:        712,
		Max:        1050,
		Percentage: 50,
	}
	b3 := histoInterval{
		Min:        300,
		Max:        500,
		Percentage: 20,
	}

	h := histogram{
		b1,
		b2,
		b3,
	}

	c1 := 0
	c2 := 0
	c3 := 0
	c4 := 0
	total := 10000
	r := rand.New(rand.NewSource(0))
	for i := 0; i < total; i++ {
		selBucket, err := h.selectBucket(r)
		a.Nil(err)
		a.NotNil(selBucket)
		if reflect.DeepEqual(selBucket, &b1) {
			c1++
		} else if reflect.DeepEqual(selBucket, &b2) {
			c2++
		} else if reflect.DeepEqual(selBucket, &b3) {
			c3++
		} else {
			c4++
		}
	}

	a.Equal(c4, 0)
	a.InDelta(c1, 3000, 200)
	a.InDelta(c2, 5000, 200)
	a.InDelta(c3, 2000, 200)
}

package sqload

import (
	"math/rand"

	"github.com/pkg/errors"

	"rubrik/util/numberutil"
)

// Config for uniform int generator
// Generates integers with uniform distribution between min and max
type intUniformGenerator struct {
	// Cardinality upper-bounds unique values that may be generated
	Cardinality uint64 `json:"cardinality"`
	// Min value that can be generated (inclusive)
	Min int64 `json:"min"`
	// Max value that can be generated (inclusive)
	Max int64 `json:"max"`
	// RandomSalt is used to control the sequence of random numbers generated
	// Refer to README.md
	RandomSalt uint64 `json:"random_salt"`
	// cachedInterval is Max - Min. It is a derived value which is cached here
	// It is cached so that integer overflow corner cases don't have to be
	// evaluated every time
	cachedInterval uint64
}

func (g *intUniformGenerator) InitParams() {
	g.RandomSalt = randomSaltProvider.next()
}

func (g *intUniformGenerator) Validate() error {
	if g.Min >= g.Max {
		return errors.Errorf("min %v is >= max %v", g.Min, g.Max)
	}

	return nil
}

// interval returns Max - Min. It computes it once and caches it in
// cachedInterval of intUniformGenerator. It checks for integer overflows
func (g *intUniformGenerator) interval() uint64 {
	if g.cachedInterval <= 0 {
		g.cachedInterval = numberutil.Diff(g.Min, g.Max)
	}

	return g.cachedInterval
}

// Generates integers with uniform distribution between min and max
// (both inclusive)
type intUniformColGenerator struct {
	intUniformGenerator
	rand *rand.Rand
}

func (cg *intUniformColGenerator) Next(r uint64) (interface{}, error) {
	cg.rand.Seed(int64((r + cg.RandomSalt) % cg.Cardinality))

	offset := cg.rand.Uint64() % cg.interval()
	v, err := numberutil.AddInt64UInt64(cg.Min, offset)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func newIntUniformColGenerator(
	g intUniformGenerator,
	saltOffset uint64,
) (
	valueGenerator,
	error,
) {
	g.RandomSalt += saltOffset
	colRandSrc := rand.NewSource(0)
	colGen := intUniformColGenerator{
		rand:                rand.New(colRandSrc),
		intUniformGenerator: g,
	}
	return &colGen, nil
}

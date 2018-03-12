package sqload

import (
	"math/rand"
)

// Config for gaussian int generator
// Generates Integers with in a Gaussian Distribution with a given Mean and Standard Deviation
type intGaussianGenerator struct {
	// Cardinality upper-bounds unique values that may be generated
	Cardinality uint64 `json:"cardinality"`
	// Mean is peak (center) of the bell
	Mean float64 `json:"mean"`
	// StdDev is standard-deviation of the distribution
	// (higher value implies wider bell)
	StdDev float64 `json:"sd"`
	// RandomSalt is used to control the sequence of random numbers generated
	// Refer to README.md
	RandomSalt uint64 `json:"random_salt"`
}

func (g *intGaussianGenerator) InitParams() {
	g.RandomSalt = randomSaltProvider.next()
}

func (g *intGaussianGenerator) Validate() error {
	return nil
}

// Generates Integers with in a Gaussian Distribution with a
// given Mean and Standard Deviation
type intGaussianColGenerator struct {
	intGaussianGenerator
	rand *rand.Rand
}

func (cg *intGaussianColGenerator) Next(r uint64) (interface{}, error) {
	cg.rand.Seed(int64((r + cg.RandomSalt) % cg.Cardinality))
	v := int64(cg.rand.NormFloat64()*cg.StdDev + cg.Mean)
	return v, nil
}

func newIntGaussianColGenerator(
	g intGaussianGenerator,
	saltOffset uint64,
) (
	valueGenerator,
	error,
) {
	g.RandomSalt += saltOffset
	colRandSrc := rand.NewSource(0)
	colGen := intGaussianColGenerator{
		rand:                 rand.New(colRandSrc),
		intGaussianGenerator: g,
	}
	return &colGen, nil
}

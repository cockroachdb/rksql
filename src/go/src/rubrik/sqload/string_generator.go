package sqload

import (
	"math"
	"math/rand"

	"github.com/pkg/errors"
)

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Number of bits to represent a character in "alphabet"
var letterIdxBits = uint(math.Ceil(math.Log2(float64(len(alphabet)))))

// All 1-bits, as many as letterIdxBits
var letterIdxMask = int64(1)<<letterIdxBits - 1

// # of letter indices fitting in 63 bits
var letterIdxMax = 63 / letterIdxBits

type strPrefixClassInterval struct {
	Str        string  `json:"str"`
	Percentage float32 `json:"pct"`
}

// Config for string generator
// Generates random strings of given length with certain prefix distribution (prefixes are optional)
type strHistoLenGenerator struct {
	// Cardinalityupper-bounds unique values that may be generated
	// Cardinality is per len interval
	Cardinality uint64 `json:"cardinality"`
	// Len is the total length (including prefix) of the string to generate
	Len histogram `json:"len"`
	// Prefix is the distribution of prefixes to use for string generation. It is optional
	Prefix []strPrefixClassInterval `json:"prefix"`
	// RandomSalt is used to control the sequence of random numbers generated
	// Refer to README.md
	RandomSalt uint64 `json:"random_salt"`
}

func (g *strHistoLenGenerator) InitParams() {
	g.RandomSalt = randomSaltProvider.next()
}

func (g *strHistoLenGenerator) Validate() error {
	if err := g.Len.validateHistoInterval(); err != nil {
		return err
	}

	maxPrefixLen := int64(0)
	if len(g.Prefix) > 0 {
		prefixClassItvlSum := float32(0.0)
		for _, cItvl := range g.Prefix {
			prefixLen := int64(len(cItvl.Str))
			if prefixLen > maxPrefixLen {
				maxPrefixLen = prefixLen
			}

			prefixClassItvlSum += cItvl.Percentage
		}
		if err := validateClassIntervalSum("prefix", prefixClassItvlSum); err != nil {
			return err
		}
	}

	minStrLen := g.Len.min()
	if maxPrefixLen > minStrLen {
		return errors.Errorf("min string length %v is greater than max prefix length %v",
			minStrLen, maxPrefixLen)
	}
	return nil
}

// Generates random strings of given length with certain prefix distribution
// (prefixes are optional)
type strHistoLenColGenerator struct {
	strHistoLenGenerator
	rand *rand.Rand
}

// Selects a string prefix based on distribution of prefixes
func (cg *strHistoLenColGenerator) getPrefix() (string, error) {
	if len(cg.Prefix) > 0 {
		prefixToss := cg.rand.Float32() * 100
		cumulativeProb := float32(0)
		for _, prefixItem := range cg.Prefix {
			cumulativeProb += prefixItem.Percentage
			if prefixToss < cumulativeProb {
				return prefixItem.Str, nil
			}
		}

		return "", errors.New("unable to select prefix in string generator")
	}

	return "", nil
}

// A more efficient way to generate a random string
// compared to using alphabet[rand()%(len(alphabet))]
// Universe of characters is in the const "alphabet".
// A uint64 has 64 bits. A single rand unit64 is used to generate
// multiple characters in the random string. The number of characters
// a single uint64 can generate is 63 / log_2(len(alphabets))
func randString(prefix string, src rand.Source, n int64) string {
	prefixLen := int64(len(prefix))
	b := make([]byte, n+prefixLen)
	copy(b, prefix)

	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(alphabet) {
			b[i+prefixLen] = alphabet[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func (cg *strHistoLenColGenerator) Next(r uint64) (interface{}, error) {
	var err error
	cg.rand.Seed(int64((r + cg.RandomSalt) % cg.Cardinality))
	prefix, err := cg.getPrefix()
	if err != nil {
		return nil, err
	}
	lenBucket, err := cg.Len.selectBucket(cg.rand)
	if err != nil {
		return nil, err
	}
	genLen := lenBucket.Min - int64(len(prefix))
	if lenBucket.Max > lenBucket.Min {
		genLen += cg.rand.Int63() % int64(lenBucket.Max-lenBucket.Min)
	}

	if genLen < 0 {
		return nil, errors.New("length of string to generate is < 0")
	}

	v := randString(prefix, cg.rand, genLen)

	return v, nil
}

func newStrHistoLenColGenerator(
	g strHistoLenGenerator,
	saltOffset uint64,
) (
	valueGenerator,
	error,
) {
	g.RandomSalt += saltOffset
	colRandSrc := rand.NewSource(0)
	colGen := strHistoLenColGenerator{
		rand:                 rand.New(colRandSrc),
		strHistoLenGenerator: g,
	}
	return &colGen, nil
}

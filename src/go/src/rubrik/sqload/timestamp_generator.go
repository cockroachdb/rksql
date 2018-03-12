package sqload

import (
	"math/rand"
	"time"

	"github.com/pkg/errors"
)

const timeFormat = "2006-01-02 15:04:05.000000000-07:00"

// Config for timestamp generator
// Generate timestamps at a particular offset to current time
// Generates timestamps between (Now - Offset - MaxDeviation) to (Now - Offset + MaxDeviation)
type timestampGenerator struct {
	// Offset from current time
	Offset duration `json:"offset"`
	// MaxDeviation from offset
	MaxDeviation duration `json:"max_deviation"`
	// RandomSalt is used to control the sequence of random numbers generated
	// Refer to README.md
	RandomSalt uint64 `json:"random_salt"`
}

func (g *timestampGenerator) InitParams() {
	g.RandomSalt = randomSaltProvider.next()
}

func (g *timestampGenerator) Validate() error {
	return nil
}

// Generate timestamps at a particular offset to current time. Generates timestamps
// uniformly between (Now - Offset +- MaxDeviation)
type timestampColGenerator struct {
	timestampGenerator
	rand *rand.Rand
}

// Outputs a time which is t + offset +- rand() * maxDeviation
func (cg *timestampColGenerator) randomTimeRelativeTo(r uint64, t time.Time) interface{} {
	cg.rand.Seed(int64(r + cg.RandomSalt))

	signMultiplier := int64(1)
	if cg.rand.Float32() < 0.5 {
		signMultiplier = -1
	}

	calculatedOffset := int64(cg.Offset.Duration) + signMultiplier*(cg.rand.Int63()%int64(cg.MaxDeviation.Duration))

	v := t.Add(time.Duration(calculatedOffset))
	return v.Format(timeFormat)
}

// Generates a timestamp string with the given offset from current time
func getOffsetFormattedTime(d *time.Duration) (string, error) {
	if d == nil {
		return "", errors.Errorf("invalid duration %v", d)
	}
	now := time.Now()
	offsetTime := now.Add(*d)
	return offsetTime.Format(timeFormat), nil
}

func (cg *timestampColGenerator) Next(r uint64) (interface{}, error) {
	return cg.randomTimeRelativeTo(r, time.Now()), nil
}

func newTimestampColGenerator(
	g timestampGenerator,
	saltOffset uint64,
) (
	valueGenerator,
	error,
) {
	g.RandomSalt += saltOffset
	colRandSrc := rand.NewSource(0)
	colGen := timestampColGenerator{
		rand:               rand.New(colRandSrc),
		timestampGenerator: g,
	}
	return &colGen, nil
}

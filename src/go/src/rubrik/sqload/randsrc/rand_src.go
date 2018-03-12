package randsrc

import (
	"math"
	"math/rand"
)

const (
	// Size of the channel where random numbers are read from
	defaultRandChanSize = 100
)

// Source is used to generate random numbers in a channel
type Source struct {
	// chanSize: Size of the channel to generate rands
	chanSize int
	// seed: seed to use for random number generation
	seed int64
	// rolloverThresh: threshold after which to repeat random numbers
	rolloverThresh uint64
	// forceIgnoreRollover specifies whether to ignore rolling over
	forceIgnoreRollover bool
	// repeat: whether to repeat values after rolling over.
	// If repeat is false, rand generation stops after reaching rolloverThresh
	repeat bool
}

// generateRand generates random numbers into the given channel c.
// If the generated random number count reaches rolloverThresh,
// rand generation stops if repeat is false, otherwise it resets.
func (rs *Source) generateRand(c chan<- uint64) {
	generatedCount := uint64(0)
	randomSrc := rand.NewSource(rs.seed)
	r := rand.New(randomSrc)
	for {
		c <- r.Uint64()
		generatedCount++
		if !rs.forceIgnoreRollover {
			if generatedCount >= rs.rolloverThresh {
				if rs.repeat {
					generatedCount = uint64(0)
					r.Seed(rs.seed)
				} else {
					close(c)
					break
				}
			}
		}
	}
}

// Seed sets the seed of the rand generation
func Seed(seed int64) func(*Source) {
	return func(rs *Source) {
		rs.seed = seed
	}
}

// ChanSize sets the channel size to use to for rand generation
func ChanSize(size int) func(*Source) {
	return func(rs *Source) {
		rs.chanSize = size
	}
}

// RolloverThresh specifies the number of rand generations after which to
// reset. If this is not set, rand generation does not reset
func RolloverThresh(rolloverThresh uint64) func(*Source) {
	return func(rs *Source) {
		rs.rolloverThresh = rolloverThresh
	}
}

// ForceIgnoreRollover specifies whether RolloverThresh should be ignored.
// If this is set to true, rand generation does not reset.
func ForceIgnoreRollover(forceIgnoreRollover bool) func(*Source) {
	return func(rs *Source) {
		rs.forceIgnoreRollover = forceIgnoreRollover
	}
}

// Repeat specifies whether to repeat rand generation after reaching
// RolloverThresh
func Repeat(repeat bool) func(*Source) {
	return func(rs *Source) {
		rs.repeat = repeat
	}
}

// NewRandSrc returns a channel where random numbers are published
func NewRandSrc(
	options ...func(*Source),
) <-chan uint64 {
	// initialize with defaults
	rs := &Source{
		chanSize:       defaultRandChanSize,
		rolloverThresh: math.MaxUint64,
	}
	for _, o := range options {
		o(rs)
	}
	c := make(chan uint64, rs.chanSize)
	go rs.generateRand(c)
	return c
}

package randsrc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandGeneratorRandomness(t *testing.T) {
	rs := NewRandSrc(Seed(1))
	a := assert.New(t)

	distinctVals := make(map[uint64]bool)
	numIterations := 1000
	for i := 0; i < numIterations; i++ {
		v := <-rs
		_, ok := distinctVals[v]
		a.False(ok)
		distinctVals[v] = true
	}
	a.InDelta(numIterations, len(distinctVals), 0.05*float64(numIterations))
}

func TestRandGeneratorSeed(t *testing.T) {
	rs1 := NewRandSrc(Seed(1))
	rs2 := NewRandSrc(Seed(2))
	a := assert.New(t)

	m1 := make(map[uint64]bool)
	m2 := make(map[uint64]bool)
	numIterations := 1000
	for i := 0; i < numIterations; i++ {
		v1 := <-rs1
		v2 := <-rs2
		m1[v1] = true
		m2[v2] = true
	}

	common := 0
	for key := range m1 {
		if _, ok := m2[key]; ok {
			common++
		}
	}

	a.Equal(numIterations, len(m1))
	a.Equal(numIterations, len(m2))
	a.InDelta(0, common, 0.05*float64(numIterations))
}

func TestRandGeneratorRollOverNoRepeat(t *testing.T) {
	rolloverThresh := uint64(53)
	rs := NewRandSrc(Seed(1), RolloverThresh(rolloverThresh))
	a := assert.New(t)
	for i := uint64(0); i <= rolloverThresh; i++ {
		v, ok := <-rs
		if i == rolloverThresh {
			a.False(ok)
			a.Equal(uint64(0), v)
		} else {
			a.True(ok)
		}
	}
}

func TestRandGeneratorRollOverRepeat(t *testing.T) {
	rolloverThresh := uint64(59)
	rs := NewRandSrc(
		Seed(1),
		RolloverThresh(rolloverThresh),
		Repeat(true),
	)
	a := assert.New(t)
	var genValues []uint64
	for i := uint64(0); i <= 10000; i++ {
		v := <-rs
		if i < rolloverThresh {
			genValues = append(genValues, v)
		} else {
			a.Equal(genValues[int(i%rolloverThresh)], v)
		}
	}
}

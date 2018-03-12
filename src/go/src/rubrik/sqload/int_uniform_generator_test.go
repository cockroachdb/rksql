package sqload

import (
	"context"
	"fmt"
	"math"

	"rubrik/sqload/randsrc"
)

func (s *GeneratorsTestSuite) TestUniformIntGeneratorDataDistribution() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				// Higher cardinality leads to a more uniform distribution
				Cardinality: 10000,
				Min:         0,
				Max:         999,
				RandomSalt:  0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)

	numIterations := 100000
	numBuckets := 10
	buckets := make([]int, numBuckets)

	expectedNumPerBucket := float64(numIterations) / float64(numBuckets)
	s.T().Logf("Bucket frequencies: %#v", buckets)
	s.T().Logf("Expected bucket frequency: %v", expectedNumPerBucket)
	for i := 0; i < numIterations; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		bucketIdx := row[0].(int64) / 100
		buckets[bucketIdx]++
	}

	// Verify uniform distribution
	for bucketNum, bucketFrequency := range buckets {
		s.InDelta(expectedNumPerBucket,
			bucketFrequency,
			0.1*expectedNumPerBucket,
			fmt.Sprintf("Bucket %v has %v elements. Expected %v ", bucketNum, bucketFrequency, expectedNumPerBucket))
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorCardinalityControl() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  0,
			},
		},
	}

	testUniqueValues(
		ctx,
		gens,
		10,     /* cardinality */
		0,      /* delta */
		100000, /* num iterations */
		s,
	)
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorSeqWithSameSalt() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  13,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr1, err := makeGeneratedParamReader(gens, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr2, err := makeGeneratedParamReader(gens, rs2)
	s.Nil(err)
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		s.Equal(row1[0], row2[0])
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorSeqWithSaltOffset() {
	ctx := context.Background()
	gens1 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  10,
			},
			saltOffset: 105,
		},
	}

	gens2 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  100,
			},
			saltOffset: 15,
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr1, err := makeGeneratedParamReader(gens1, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr2, err := makeGeneratedParamReader(gens2, rs2)
	s.Nil(err)
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		s.Equal(row1[0], row2[0])
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorMinMaxEqual() {
	gen := intUniformGenerator{
		Cardinality: 10,
		Min:         1,
		Max:         1,
		RandomSalt:  13,
	}

	err := gen.Validate()
	s.Equal(err.Error(), "min 1 is >= max 1")
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorConstantGeneration() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         1,
				Max:         2,
				RandomSalt:  13,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	for i := 0; i < 10000; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		q := row[0].(int64)
		s.Equal(int64(1), q)
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorCompleteRangeGeneration() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: math.MaxUint64,
				Min:         math.MinInt64,
				Max:         math.MaxInt64,
				RandomSalt:  13,
			},
		},
	}

	testUniqueValues(
		ctx,
		gens,
		100000, /* cardinality */
		100,    /* delta */
		100000, /* num iterations */
		s,
	)
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorNearCompleteGeneration() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: math.MaxUint64,
				Min:         math.MinInt64 + 1,
				Max:         math.MaxInt64 - 1,
				RandomSalt:  13,
			},
		},
	}

	testUniqueValues(
		ctx,
		gens,
		100000, /* cardinality */
		100,    /* delta */
		100000, /* num iterations */
		s,
	)
}

func testUniqueValues(
	ctx context.Context,
	gens []generatorSpec,
	cardinality int,
	delta float64,
	numIter int,
	s *GeneratorsTestSuite,
) {
	rs := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	uniqueVals := make(map[int64]bool)
	for i := 0; i < numIter; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		q := row[0].(int64)
		uniqueVals[q] = true
	}

	s.Nil(err)
	s.InDelta(cardinality, len(uniqueVals), delta)
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorSeqWithDifferentSalt() {
	ctx := context.Background()
	gens1 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  13,
			},
		},
	}
	gens2 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 10,
				Min:         0,
				Max:         100000,
				RandomSalt:  42,
			},
		},
	}

	//they must evolve differently even on the same loader routine
	rs1 := randsrc.NewRandSrc(randsrc.Seed(3))
	rdr1, err := makeGeneratedParamReader(gens1, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(3))
	rdr2, err := makeGeneratedParamReader(gens2, rs2)
	s.Nil(err)
	diffCount := 0
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		if row1[0] != row2[0] {
			diffCount++
		}
	}

	s.InDelta(100000, diffCount, 100)
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorConstantDifference() {
	ctx := context.Background()
	const RangeOffset = 6000000000
	gens1 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Min:         0,
				Max:         math.MaxInt64 - RangeOffset,
				RandomSalt:  13,
				Cardinality: math.MaxUint64,
			},
		},
	}
	gens2 := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: math.MaxUint64,
				Min:         RangeOffset,
				Max:         math.MaxInt64,
				RandomSalt:  13,
			},
		},
	}

	//they must evolve differently even on the same loader routine
	rs1 := randsrc.NewRandSrc(randsrc.Seed(3))
	rdr1, err := makeGeneratedParamReader(gens1, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(3))
	rdr2, err := makeGeneratedParamReader(gens2, rs2)
	s.Nil(err)
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		v1 := row1[0].(int64)
		v2 := row2[0].(int64)
		s.Equal(v1+RangeOffset, v2)
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorRandOrderConcurrencyIndependent() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 1000000,
				Min:         0,
				Max:         100000000,
				RandomSalt:  0,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(0))
	rs2 := randsrc.NewRandSrc(randsrc.Seed(0))

	rdr1, err := makeGeneratedParamReader(gens, rs1)
	s.Nil(err)
	rdr2, err := makeGeneratedParamReader(gens, rs1)
	s.Nil(err)
	rdr3, err := makeGeneratedParamReader(gens, rs2)
	s.Nil(err)
	// rdr1 and rdr2 share a rand generator
	// order of rands by Union(rdr1, rdr2) should be same as rdr3
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		row3, err := rdr3.NextRow(ctx)
		s.Nil(err)
		s.Equal(row1[0], row3[0])
		row3, err = rdr3.NextRow(ctx)
		s.Nil(err)
		s.Equal(row2[0], row3[0])
	}
}

func (s *GeneratorsTestSuite) TestUniformIntGeneratorSeqEvolveDifferentlyAcrossLoaders() {
	ctx := context.Background()
	// Loader is an instance of sqload (not a thread inside sqload)
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 1000000,
				Min:         0,
				Max:         100000000,
				RandomSalt:  0,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr1, err := makeGeneratedParamReader(gens, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(1))
	rdr2, err := makeGeneratedParamReader(gens, rs2)
	s.Nil(err)
	diffCount := 0
	for i := 0; i < 100000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		if row1[0] != row2[0] {
			diffCount++
		}
	}
	s.InDelta(100000, diffCount, 100)
}

func (s *GeneratorsTestSuite) TestUniformIntRollOverNoRepeat() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 1000000000,
				Min:         1,
				Max:         1000,
				RandomSalt:  13,
			},
		},
	}

	rolloverThresh := uint64(25)
	rs := randsrc.NewRandSrc(
		randsrc.Seed(4),
		randsrc.RolloverThresh(rolloverThresh),
	)
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	for i := uint64(0); i <= rolloverThresh; i++ {
		row, err := rdr.NextRow(ctx)
		if i == rolloverThresh {
			s.Nil(err)
			s.Nil(row)
		} else {
			s.Nil(err)
			q := row[0].(int64)
			s.True(q >= 1 && q <= 1000)
		}
	}
}

func (s *GeneratorsTestSuite) TestUniformIntRollOverRepeat() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intUniformGenerator{
				Cardinality: 1000000000,
				Min:         1,
				Max:         1000,
				RandomSalt:  13,
			},
		},
	}

	rolloverThresh := uint64(25)
	rs := randsrc.NewRandSrc(
		randsrc.Seed(4),
		randsrc.RolloverThresh(rolloverThresh),
		randsrc.Repeat(true),
	)
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	var genValues []int64
	for i := uint64(0); i <= 10000; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		q := row[0].(int64)
		s.True(q >= 1 && q <= 1000)
		if i < rolloverThresh {
			genValues = append(genValues, q)
		} else {
			s.Equal(genValues[int(i%rolloverThresh)], q)
		}
	}
}

package sqload

import (
	"context"

	"rubrik/sqload/randsrc"
)

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorDataDistribution() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 100000,
				Mean:        2000,
				StdDev:      300,
				RandomSalt:  0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	// each class-itvl has width == 200
	// here we are dealing with 0 to 2000 range
	wideClassItvlFreqForRise := make([]int, 10)
	// now we are dealing with 2000 to 4000 range
	wideClassItvlFreqForFall := make([]int, 10)
	for i := 0; i < 100000; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		q := row[0].(int64) / 200
		if q >= 10 {
			if q >= 20 {
				continue
			}
			wideClassItvlFreqForFall[q-10]++
		} else {
			if q < 0 {
				continue
			}
			wideClassItvlFreqForRise[q]++
		}
	}
	s.T().Logf("Rise frequenecies: %#v", wideClassItvlFreqForRise)
	s.T().Logf("Fall frequenecies: %#v", wideClassItvlFreqForFall)
	for i := 1; i < 10; i++ {
		if wideClassItvlFreqForRise[i] == 0 {
			s.Equal(0, wideClassItvlFreqForRise[i-1])
		} else {
			s.True(wideClassItvlFreqForRise[i-1] < wideClassItvlFreqForRise[i])
		}

		if wideClassItvlFreqForFall[i-1] == 0 {
			s.Equal(0, wideClassItvlFreqForFall[i])
		} else {
			s.True(wideClassItvlFreqForFall[i-1] > wideClassItvlFreqForFall[i])
		}
	}
	// catch the situation where generator always produces 0
	s.True(wideClassItvlFreqForRise[9] > 0)
	s.True(wideClassItvlFreqForFall[0] > 0)
}

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorCardinalityControl() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 10,
				Mean:        5000,
				StdDev:      1000,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	uniqueValues := make(map[int64]bool)
	for i := 0; i < 100000; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		q := row[0].(int64)
		uniqueValues[q] = true
	}
	s.Equal(10, len(uniqueValues))
}

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorSeqWithSameSalt() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 1000,
				Mean:        5000,
				StdDev:      1000,
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

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorSeqWithSaltOffset() {
	ctx := context.Background()
	gens1 := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 1000,
				Mean:        5000,
				StdDev:      1000,
				RandomSalt:  103,
			},
			saltOffset: 10,
		},
	}
	gens2 := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 1000,
				Mean:        5000,
				StdDev:      1000,
				RandomSalt:  100,
			},
			saltOffset: 13,
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

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorSeqWithDifferentSalt() {
	ctx := context.Background()
	gens1 := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 10000,
				Mean:        5000,
				StdDev:      1000,
				RandomSalt:  13,
			},
		},
	}
	gens2 := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 10000,
				Mean:        5000,
				StdDev:      1000,
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

func (s *GeneratorsTestSuite) TestGaussianIntGeneratorSeqEvolveDifferentlyAcrossLoaders() {
	ctx := context.Background()
	// Loader is an instance of sqload (not a thread inside sqload)
	gens := []generatorSpec{
		{
			gen: &intGaussianGenerator{
				Cardinality: 10000,
				Mean:        5000,
				StdDev:      1000,
				RandomSalt:  13,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(4))
	rdr1, err := makeGeneratedParamReader(gens, rs1)
	s.Nil(err)
	rs2 := randsrc.NewRandSrc(randsrc.Seed(5))
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

package sqload

import (
	"context"
	"fmt"
	"strings"

	"rubrik/sqload/randsrc"
)

func (s *GeneratorsTestSuite) TestStringGeneratorLengthDistribution() {
	ctx := context.Background()
	percentages := []float32{40, 40, 20}
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 0, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},

				RandomSalt: 0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)

	buckets := make([]int, 3)

	numIterations := 10000
	for i := 0; i < numIterations; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		generatedStr := row[0].(string)
		buckets[len(generatedStr)/100]++
	}

	s.T().Logf("Bucket frequencies: %#v", buckets)

	// Verify distribution for string lengths
	for bucketNum, bucketFrequency := range buckets {
		expectedBucketFreq := float64(percentages[bucketNum] / 100.0 * float32(numIterations))
		s.InDelta(expectedBucketFreq,
			bucketFrequency,
			0.1*expectedBucketFreq,
			fmt.Sprintf("Bucket %v has %v elements. Expected %v ", bucketNum, bucketFrequency, expectedBucketFreq))
	}
}

func (s *GeneratorsTestSuite) TestStringGeneratorPrefixDistribution() {
	ctx := context.Background()
	percentages := []float32{20, 30, 50}
	prefixes := []string{"prefix1", "longprefix2", "sp3"}
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)

	buckets := make([]int, 3)

	numIterations := 10000
	for i := 0; i < numIterations; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		generatedStr := row[0].(string)
		foundPrefix := false
		for prefixIndex, prefix := range prefixes {
			if strings.HasPrefix(generatedStr, prefix) {
				buckets[prefixIndex]++
				foundPrefix = true
				break
			}
		}
		s.True(foundPrefix)
	}

	s.T().Logf("Bucket frequencies: %#v", buckets)

	// Verify distribution for string lengths
	for bucketNum, bucketFrequency := range buckets {
		expectedBucketFreq := float64(percentages[bucketNum] / 100.0 * float32(numIterations))
		s.InDelta(expectedBucketFreq,
			bucketFrequency,
			0.1*expectedBucketFreq,
			fmt.Sprintf("Bucket %v has %v elements. Expected %v ", bucketNum, bucketFrequency, expectedBucketFreq))
	}
}

func (s *GeneratorsTestSuite) TestStringGeneratorInvalidLength() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 1, Max: 5, Percentage: 100}, // String length to generate is less than prefix length
				},
				Prefix: []strPrefixClassInterval{
					{Str: "this_is_a_very_long_prefix", Percentage: 50},
					{Str: "", Percentage: 50},
				},
				RandomSalt: 0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	numIterations := 10000
	numErr := 0
	numValid := 0
	for i := 0; i < numIterations; i++ {
		if _, err := rdr.NextRow(ctx); err != nil {
			numValid++
		} else {
			numErr++
		}
	}

	s.InDelta(5000, numValid, 500, "Number of valid strings do not match")
	s.InDelta(5000, numErr, 500, "Number of invalid strings do not match")
}

func (s *GeneratorsTestSuite) TestStringGeneratorCardinality() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 1, Max: 1000, Percentage: 100},
				},
				RandomSalt: 0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	numIterations := 10000
	distinctStrs := make(map[string]bool)
	for i := 0; i < numIterations; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		generatedStr := row[0].(string)
		distinctStrs[generatedStr] = true
	}

	s.InDelta(100, len(distinctStrs), 10)
	s.True(len(distinctStrs) <= 100)
}

func (s *GeneratorsTestSuite) TestStringGeneratorTestEqualMinMax() {
	ctx := context.Background()
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 100, Max: 100, Percentage: 100},
				},
				RandomSalt: 0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)
	numIterations := 10000
	for i := 0; i < numIterations; i++ {
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		generatedStr := row[0].(string)
		s.Equal(100, len(generatedStr))
	}

}

func (s *GeneratorsTestSuite) TestStringGeneratorSeqWithSameSalt() {
	ctx := context.Background()
	percentages := []float32{20, 30, 50}
	prefixes := []string{"prefix1", "longprefix2", "sp3"}
	gen := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 5,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(0))
	rs2 := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr1, err := makeGeneratedParamReader(gen, rs1)
	s.Nil(err)
	rdr2, err := makeGeneratedParamReader(gen, rs2)
	s.Nil(err)

	for i := 0; i < 10000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)

		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)

		s.Equal(row1, row2)
	}
}

func (s *GeneratorsTestSuite) TestStringGeneratorSeqWithSaltOffset() {
	ctx := context.Background()
	percentages := []float32{20, 30, 50}
	prefixes := []string{"prefix1", "longprefix2", "sp3"}
	gens1 := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 5,
			},
			saltOffset: 100,
		},
	}

	gens2 := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 102,
			},
			saltOffset: 3,
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(0))
	rs2 := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr1, err := makeGeneratedParamReader(gens1, rs1)
	s.Nil(err)
	rdr2, err := makeGeneratedParamReader(gens2, rs2)
	s.Nil(err)

	for i := 0; i < 10000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)

		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)

		s.Equal(row1, row2)
	}
}
func (s *GeneratorsTestSuite) TestStringGeneratorSeqWithDifferentSalt() {
	ctx := context.Background()
	percentages := []float32{20, 30, 50}
	prefixes := []string{"prefix1", "longprefix2", "sp3"}
	gen1 := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 1000000,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 0,
			},
		},
	}

	gen2 := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 100000,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 4,
			},
		},
	}

	rs1 := randsrc.NewRandSrc(randsrc.Seed(0))
	rs2 := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr1, err := makeGeneratedParamReader(gen1, rs1)
	s.Nil(err)
	rdr2, err := makeGeneratedParamReader(gen2, rs2)
	s.Nil(err)

	m1 := make(map[string]bool)
	m2 := make(map[string]bool)
	for i := 0; i < 10000; i++ {
		row1, err := rdr1.NextRow(ctx)
		s.Nil(err)
		s1 := row1[0].(string)
		m1[s1] = true

		row2, err := rdr2.NextRow(ctx)
		s.Nil(err)
		s2 := row2[0].(string)
		m2[s2] = true

		s.NotEqual(s1, s2, "Different salts are generating same strings")
	}

	common := numCommonElements(m1, m2)
	s.InDelta(0, common, 100)
}

func (s *GeneratorsTestSuite) TestStringGeneratorSeqEvolveDifferentlyAcrossLoaders() {
	ctx := context.Background()
	// Loader is an instance of sqload (not a thread inside sqload)
	percentages := []float32{20, 30, 50}
	prefixes := []string{"prefix1", "longprefix2", "sp3"}
	gens := []generatorSpec{
		{
			gen: &strHistoLenGenerator{
				Cardinality: 10000,
				Len: histogram{
					{Min: 30, Max: 99, Percentage: percentages[0]},
					{Min: 150, Max: 199, Percentage: percentages[1]},
					{Min: 220, Max: 245, Percentage: percentages[2]},
				},
				Prefix: []strPrefixClassInterval{
					{Str: prefixes[0], Percentage: percentages[0]},
					{Str: prefixes[1], Percentage: percentages[1]},
					{Str: prefixes[2], Percentage: percentages[2]},
				},
				RandomSalt: 0,
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

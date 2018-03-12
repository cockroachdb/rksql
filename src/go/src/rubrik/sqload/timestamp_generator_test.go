package sqload

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"rubrik/sqload/randsrc"
)

const (
	nsMultiplier = int64(time.Second)
)

func (s *GeneratorsTestSuite) TestTimestampGeneratorDistribution() {
	ctx := context.Background()
	offset := -3600 * nsMultiplier
	maxDeviation := 600 * nsMultiplier
	gens := []generatorSpec{
		{
			gen: &timestampGenerator{
				Offset:       duration{Duration: time.Duration(offset)},
				MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
				RandomSalt:   0,
			},
		},
	}

	rs := randsrc.NewRandSrc(randsrc.Seed(0))
	rdr, err := makeGeneratedParamReader(gens, rs)
	s.Nil(err)

	const numBuckets = 20
	const numIterations = 100000

	buckets := make([]int, numBuckets)

	for i := 0; i < numIterations; i++ {
		nowNS := time.Now().Unix() * nsMultiplier
		row, err := rdr.NextRow(ctx)
		s.Nil(err)
		timeStr := row[0].(string)
		generatedTime, err := time.Parse(timeFormat, timeStr)
		s.Nil(err)
		generatedTimeNS := generatedTime.Unix() * nsMultiplier
		// Verify lower and upper bounds of generated timestamp
		s.True(generatedTimeNS >= nowNS+offset-maxDeviation, "Generated timestamp is not within range")
		s.True(generatedTimeNS <= nowNS+offset+maxDeviation, "Generated timestamp is not within range")
		bucket := (generatedTimeNS - (nowNS + offset - maxDeviation) - 1) * 20 / (2 * maxDeviation)
		buckets[bucket]++
	}

	expectedNumPerBucket := float64(numIterations) / float64(numBuckets)
	s.T().Logf("Bucket frequencies: %#v", buckets)
	s.T().Logf("Expected bucket frequency: %v", expectedNumPerBucket)

	// Verify uniform distribution for timestamp
	for bucketNum, bucketFrequency := range buckets {
		s.InDelta(expectedNumPerBucket,
			bucketFrequency,
			0.1*expectedNumPerBucket,
			fmt.Sprintf("Bucket %v has %v elements. Expected %v ", bucketNum, bucketFrequency, expectedNumPerBucket))
	}
}

func (s *GeneratorsTestSuite) TestTimestampGeneratorSeqWithSameSalt() {
	offset := -3600 * nsMultiplier
	maxDeviation := 600 * nsMultiplier
	tGen1 := timestampColGenerator{
		timestampGenerator: timestampGenerator{
			Offset:       duration{Duration: time.Duration(offset)},
			MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
			RandomSalt:   4,
		},
		rand: rand.New(rand.NewSource(0)),
	}

	tGen2 := timestampColGenerator{
		timestampGenerator: timestampGenerator{
			Offset:       duration{Duration: time.Duration(offset)},
			MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
			RandomSalt:   4,
		},
		rand: rand.New(rand.NewSource(0)),
	}
	t := time.Now()
	for i := 0; i < 10000; i++ {
		s.Equal(tGen1.randomTimeRelativeTo(0, t), tGen2.randomTimeRelativeTo(0, t), "Same salt is generating different timestamps")
	}
}

func (s *GeneratorsTestSuite) TestTimestampGeneratorSeqWithDifferentSalt() {
	offset := -3600 * nsMultiplier
	maxDeviation := 600 * nsMultiplier
	tGen1 := timestampColGenerator{
		timestampGenerator: timestampGenerator{
			Offset:       duration{Duration: time.Duration(offset)},
			MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
			RandomSalt:   0,
		},
		rand: rand.New(rand.NewSource(0)),
	}

	tGen2 := timestampColGenerator{
		timestampGenerator: timestampGenerator{
			Offset:       duration{Duration: time.Duration(offset)},
			MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
			RandomSalt:   10,
		},
		rand: rand.New(rand.NewSource(0)),
	}

	t := time.Now()
	m1 := make(map[string]bool)
	m2 := make(map[string]bool)
	for i := 0; i < 10000; i++ {
		m1[tGen1.randomTimeRelativeTo(0, t).(string)] = true
		m2[tGen2.randomTimeRelativeTo(0, t).(string)] = true
	}
	common := numCommonElements(m1, m2)
	s.InDelta(0, common, 10)
}

func (s *GeneratorsTestSuite) TestTimestampGeneratorSeqEvolveDifferentlyAcrossLoaders() {
	ctx := context.Background()
	// Loader is an instance of sqload (not a thread inside sqload)
	offset := -3600 * nsMultiplier
	maxDeviation := 600 * nsMultiplier
	gens := []generatorSpec{
		{
			gen: &timestampGenerator{
				Offset:       duration{Duration: time.Duration(offset)},
				MaxDeviation: duration{Duration: time.Duration(maxDeviation)},
				RandomSalt:   0,
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

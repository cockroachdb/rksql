package sqload

import (
	"math"
	"math/rand"

	"github.com/pkg/errors"
)

type histoInterval struct {
	Min        int64   `json:"min"`
	Max        int64   `json:"max"`
	Percentage float32 `json:"pct"`
}

type histogram []histoInterval

func (h histogram) validateHistoInterval() error {
	lenClassItvlSum := float32(0.0)
	for _, cItvl := range h {
		lenClassItvlSum += cItvl.Percentage
		if cItvl.Min > cItvl.Max {
			return errors.Errorf("min %v is greater than max %v", cItvl.Min, cItvl.Max)
		}
	}

	if err := validateClassIntervalSum("len", lenClassItvlSum); err != nil {
		return err
	}

	return nil
}

func (h histogram) selectBucket(r *rand.Rand) (*histoInterval, error) {
	lenToss := r.Float32() * 100
	cumulativeProb := float32(0)
	for _, lenItem := range h {
		cumulativeProb += lenItem.Percentage
		if lenToss < cumulativeProb {
			return &lenItem, nil
		}
	}

	return nil, errors.New("unable to select element in histogram")
}

func (h histogram) min() int64 {
	min := int64(math.MaxInt64)
	for _, cItvl := range h {
		if cItvl.Min < min {
			min = cItvl.Min
		}
	}
	return min
}

func validateClassIntervalSum(name string, sum float32) error {
	if (sum < 99.9) || (sum > 100.1) {
		return &classIntervalExhaustivenessError{sum, name}
	}
	return nil
}

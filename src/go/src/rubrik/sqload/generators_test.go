package sqload

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type GeneratorsTestSuite struct {
	suite.Suite
}

func (s *GeneratorsTestSuite) SetupTest() {}

func (s *GeneratorsTestSuite) TearDownTest() {}

func TestGenerators(t *testing.T) {
	suite.Run(t, new(GeneratorsTestSuite))
}

func numCommonElements(m1, m2 map[string]bool) int64 {
	common := int64(0)
	for key := range m1 {
		if _, ok := m2[key]; ok {
			common++
		}
	}
	return common
}

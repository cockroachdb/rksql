package sqload

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ParamSrcTestSuite struct {
	suite.Suite
	tmpFiles tmpFileTracker
}

func (s *ParamSrcTestSuite) SetupTest() {}

func (s *ParamSrcTestSuite) TearDownTest() {}

func (s *ParamSrcTestSuite) TestConvertToFloat() {
	v, err := convert("20", typeFloat)
	s.Nil(err)
	s.InDelta(20.0, v, 0.001)

	v, err = convert(20.0, typeFloat)
	s.Nil(err)
	s.InDelta(20.0, v, 0.001)

	v, err = convert("20", typeFloat)
	s.Nil(err)
	s.InDelta(20.0, v, 0.001)
}

func (s *ParamSrcTestSuite) TestConvertToBool() {
	trues := []interface{}{
		"true", "tRuE", "T", "t", "y", "yes", "Y", "YeS", "on", "oN",
		1, 2, -1, int64(10), float64(0.4), float32(0.6), true}
	for _, in := range trues {
		v, err := convert(in, typeBool)
		s.Nil(err)
		s.Equal(true, v)
	}

	falses := []interface{}{
		"false", "FalsE", "F", "f", "n", "no", "N", "nO", "off", "oFF",
		0, int64(0), float64(0.0), float32(0.0), false}
	for _, in := range falses {
		v, err := convert(in, typeBool)
		s.Nil(err)
		s.Equal(false, v)
	}
}

func (s *ParamSrcTestSuite) TestConvertToString() {
	v, err := convert("20", typeString)
	s.Nil(err)
	s.Equal("20", v)

	v, err = convert(20.1, typeString)
	s.Nil(err)
	s.Equal("20.1", v)

	v, err = convert(true, typeString)
	s.Nil(err)
	s.Equal("true", v)
}

type resolvePlaceholderTestCases struct {
	name     string
	cql      string
	params   []interface{}
	expected string
	ErrRegex *regexp.Regexp // regex to match error message
}

func runResolvePlaceholdersTestCases(s *ParamSrcTestSuite, cases []resolvePlaceholderTestCases) {
	for _, tc := range cases {
		s.T().Run(tc.name, func(t *testing.T) {
			actual, err := resolvePlaceholders(tc.cql, tc.params)
			if !isErrSimilar(tc.ErrRegex, err) {
				t.Fatalf("want err %v, have: %v", tc.ErrRegex, err)
			}

			s.Equal(tc.expected, actual)
		})
	}
}

func (s *ParamSrcTestSuite) TestResolvePlaceholders() {
	cases := []resolvePlaceholderTestCases{
		{
			"test_multi_replacement",
			"UPDATE sqload.cql_map_workload set node_id=$1 WHERE job_id='$2' IF job_props['$3'] = '$4'",
			[]interface{}{123, "job_1", "k1", "v1"},
			"UPDATE sqload.cql_map_workload set node_id=123 WHERE job_id='job_1' IF job_props['k1'] = 'v1'",
			nil,
		},
		{
			"test_single_replacement",
			"UPDATE sqload.cql_map_workload set node_id=$1",
			[]interface{}{123},
			"UPDATE sqload.cql_map_workload set node_id=123",
			nil,
		},
		{
			"test_no_replacement",
			"UPDATE sqload.cql_map_workload set",
			[]interface{}{},
			"UPDATE sqload.cql_map_workload set",
			nil,
		},
		{
			"test_extra_params_at_end",
			"UPDATE sqload.cql_map_workload set node_id=$1",
			[]interface{}{123, 456},
			"",
			regexp.MustCompile("num placeholders < num params"),
		},
		{
			"test_extra_params_in_mid",
			"UPDATE sqload.cql_map_workload set node_id=$1 and job_id=123",
			[]interface{}{123, 456},
			"",
			regexp.MustCompile("num placeholders < num params"),
		},
		{
			"test_extra_placeholders_at_end",
			"UPDATE sqload.cql_map_workload set node_id=$1 and job_id=$2",
			[]interface{}{123},
			"",
			regexp.MustCompile("num placeholders > num params"),
		},
		{
			"test_extra_placeholders_in_mid",
			"UPDATE sqload.cql_map_workload set node_id=$1 and job_id=$2 and job_config=''",
			[]interface{}{123},
			"",
			regexp.MustCompile("num placeholders > num params"),
		},
	}
	runResolvePlaceholdersTestCases(s, cases)
}

func TestParamSrc(t *testing.T) {
	suite.Run(t, new(ParamSrcTestSuite))
}

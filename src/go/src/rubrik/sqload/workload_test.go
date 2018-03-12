package sqload

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkloadParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators(
		[]byte(`{
        "int_g1": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300,
                "random_salt": 7
            }
        },
        "str_g1": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 50,
                    "max": 100,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 33.3
                }, {
                    "min":201,
                    "max":1000,
                    "pct": 33.4
                }],
                "prefix": [{
                    "str": "http://",
                    "pct": 66.0
                }, {
                    "str": "ftp://",
                    "pct": 20.0
                }, {
                    "str": "rsync://",
                    "pct": 14.0
                }],
                "random_salt": 42
            }
        },
        "float_g1": {
            "name": "float_histo",
            "params": {
                "cardinality": 20000,
                "value": [{
                    "min": 0.0,
                    "max": 2000.0,
                    "pct": 10.0
                }, {
                    "min": 5000.0,
                    "max": 10000.0,
                    "pct": 20.0
                }, {
                    "min":10000.0,
                    "max":100000.0,
                    "pct": 60.0
                }, {
                    "min":100000.0,
                    "max":500000.0,
                    "pct": 10.0
                }],
                "random_salt": 1729
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_b_from_foo",
    "transaction": [{
        "query":
 "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4",
        "num_rows": 1
    }],
    "params": {
        "generators" : [
            "int_g1",
            "int_g1",
            "str_g1",
            "float_g1"
        ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}, {
    "name": "update_baz_and_quux",
    "transaction": [{
        "query":
 "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4",
        "result_name": "read_foo",
        "num_rows": 1
    }, {
        "query":
 "UPDATE baz SET x = $read_foo:1:int WHERE y = $read_foo:2:string, z = $5",
        "result_name": "write_baz",
        "num_rows": 2
    }, {
        "query":
 "INSERT INTO quux (x_val, y_val, rows_affected) values ($read_foo:1:int, `+
			`$read_foo:2:string, $write_baz:row_count)",
        "num_rows": 1
    }],
    "params": {
        "generators" : [
            "int_g1",
            "int_g1",
            "str_g1",
            "float_g1",
            "str_g1"
        ]
    },
    "concurrency": 2,
    "interval": "10ms",
		"run_without_tx": false,
    "latency_thresh": "1s"
}, {
    "name": "set_a_b_in_bar",
    "transaction": [{
        "query": "UPDATE bar SET a = $1, b = $2 WHERE c = $3",
        "num_rows": 2
    }],
    "params": {
        "generators" : [
            "int_g1",
            "str_g1",
            "float_g1"
        ]
    },
    "concurrency": 1,
    "interval": "1s",
		"run_without_tx": true,
    "latency_thresh": "2s"
}]`),
		gens,
	)
	assert.Nil(err)

	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "get_a_b_from_foo",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"int_g1", "int_g1", "str_g1", "float_g1"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  7,
					},
				},
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  7,
					},
				},
				{
					gen: &strHistoLenGenerator{
						50000,
						histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						[]strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						42,
					},
				},
				{
					gen: &floatHistoGenerator{
						20000,
						[]floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						1729,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		RunWithoutTx:  false,
		LatencyThresh: "1s",
	}, *work[0])

	// Random source is tested separately
	work[1].rs = nil
	assert.Equal(WorkLoad{
		Name: "update_baz_and_quux",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4",
				NumRows:     1,
				ResultName:  "read_foo",
				IsReadQuery: true,
			},
			{
				Query: "UPDATE baz SET x = $read_foo:1:int WHERE y = $read_foo:2:string," +
					" z = $5",
				NumRows:     2,
				ResultName:  "write_baz",
				IsReadQuery: false,
			},
			{
				Query: "INSERT INTO quux (x_val, y_val, rows_affected) values " +
					"($read_foo:1:int, $read_foo:2:string, $write_baz:row_count)",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{
				"int_g1",
				"int_g1",
				"str_g1",
				"float_g1",
				"str_g1",
			},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  7,
					},
				},
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  7,
					},
				},
				{
					gen: &strHistoLenGenerator{
						50000,
						histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						[]strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						42,
					},
				},
				{
					gen: &floatHistoGenerator{
						20000,
						[]floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						1729,
					},
				},
				{
					gen: &strHistoLenGenerator{
						50000,
						histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						[]strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						42,
					},
				},
			},
		},
		Concurrency:   2,
		Interval:      duration{time.Duration(10 * time.Millisecond)},
		AsOf:          "",
		RunWithoutTx:  false,
		LatencyThresh: "1s",
	}, *work[1])

	// Random source is tested separately
	work[2].rs = nil
	assert.Equal(WorkLoad{
		Name: "set_a_b_in_bar",
		Transaction: []query{
			{
				Query:       "UPDATE bar SET a = $1, b = $2 WHERE c = $3",
				NumRows:     2,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"int_g1", "str_g1", "float_g1"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  7,
					},
				},
				{
					gen: &strHistoLenGenerator{
						50000,
						histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						[]strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						42,
					},
				},
				{
					gen: &floatHistoGenerator{
						20000,
						[]floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						1729,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(1 * time.Second)},
		RunWithoutTx:  true,
		AsOf:          "",
		LatencyThresh: "2s",
	}, *work[2])
}

func TestWorkloadEmptyQueryValidation(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "a_b_where_c_e",
    "transaction": [{
        "query": " ",
        "num_rows": 1
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "a_b_where_c_e: could not detect if "+
		"' ' is a read or a wite query")
	assert.Nil(work)

	work, err = ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "num_rows": 1
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo_bar: could not detect if "+
		"'' is a read or a wite query")

	assert.Nil(work)
}

func TestWorkloadShouldDefaultQueryNumRows(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        }
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "a_where_c",
    "transaction": [{
        "query": "select a from foo where c = $1"
    }],
    "params": {
			"generators": ["g1"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	// Random source is tested separately
	work[0].rs = nil
	assert.Nil(err)
	assert.Equal(WorkLoad{
		Name: "a_where_c",
		Transaction: []query{
			{
				Query:       "select a from foo where c = $1",
				NumRows:     0,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])
}

func TestWorkloadGeneratorsTooFewParamsValidation(t *testing.T) {
	ctx := context.Background()
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        }
	    }`))

	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "a_b_where_c_e",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $3",
        "num_rows": 1
    }],
    "params": {
			"generators": ["g1"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "a_b_where_c_e: Param '$2' was never used or "+
		"was used but not defined")

	assert.Nil(work)
}

func TestWorkloadAdditionalGeneratorParamAtTailValidation(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators(
		[]byte(`{
        "int_g1": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300
            }
        },
    "int_g2": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 1000,
                "mean": 200,
                "sd": 30
            }
        }
    }`))

	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "int_g1", "int_g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "get_a_from_foo: Transaction expects '1' "+
		"params, but '2' generators are declared")

	assert.Nil(work)
}

func TestWorkloadTimestampGeneratorParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "ts_g1": {
            "name": "timestamp_offset",
            "params": {
                "offset": "-5m",
                "max_deviation": "10s",
                "random_salt": 10
            }
        },
        "ts_g2": {
            "name": "timestamp_offset",
            "params": {
                "offset": "-5h",
                "max_deviation": "2m",
                "random_salt": 12
            }
        }
    }`))

	assert.Nil(err)

	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "ts_g1", "ts_g2" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.Nil(err)

	nsMultiplier := int64(time.Second)
	assert.NotNil(work)

	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "get_a_from_foo",
		Transaction: []query{
			{
				Query:       "SELECT a FROM foo WHERE c = $1 or d = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"ts_g1", "ts_g2"},
			gens: []generatorSpec{
				{
					gen: &timestampGenerator{
						Offset:       duration{Duration: time.Duration(-300 * nsMultiplier)},
						MaxDeviation: duration{Duration: time.Duration(10 * nsMultiplier)},
						RandomSalt:   10,
					},
				},
				{
					gen: &timestampGenerator{
						Offset:       duration{Duration: time.Duration(-18000 * nsMultiplier)},
						MaxDeviation: duration{Duration: time.Duration(120 * nsMultiplier)},
						RandomSalt:   12,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])
}

func TestWorkloadLatencyThreshParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "as_of_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": ["g1", "g2"]
    },
    "concurrency": 2,
    "interval": "20ms",
    "latency_thresh": "100ms"
}]`),
		gens,
	)
	assert.Nil(err)
	assert.NotNil(work)
	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "as_of_test",
		Transaction: []query{
			{
				Query:       "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  7,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   2,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "100ms",
	}, *work[0])
}

func TestWorkloadAsOfParsingMissingLatencyThresh(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	// $AS_OF variable is missing
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "latency_thresh_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '123' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": [ "g1", "g2" ]
    },
    "concurrency": 2,
    "interval": "20ms"
}]`),
		gens,
	)
	assert.NotNil(err)
	assert.Equal("latency_thresh_test: Latency threshold is missing from workload", err.Error())

	assert.Nil(work)
}

func TestWorkloadAsOfParsingInvalidLatencyThresh(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	// $AS_OF variable is missing
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "latency_thresh_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '123' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": [ "g1", "g2" ]
    },
    "concurrency": 2,
    "interval": "20ms",
	"latency_thresh": "this_is_not_supposed_to_be_here"
}]`),
		gens,
	)
	assert.NotNil(err)
	assert.Equal("latency_thresh_test: Invalid duration this_is_not_supposed_to_be_here", err.Error())

	assert.Nil(work)
}

func TestWorkloadAsOfParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "as_of_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": ["g1", "g2"]
    },
    "concurrency": 2,
    "interval": "20ms",
    "as_of": "-30s",
    "latency_thresh": "1s"
}]`),
		gens,
	)
	assert.Nil(err)
	assert.NotNil(work)
	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "as_of_test",
		Transaction: []query{
			{
				Query:       "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  7,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   2,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "-30s",
		LatencyThresh: "1s",
	}, *work[0])
}

func TestWorkloadAsOfParsingMissingAsOf(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	// $AS_OF variable is missing
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "as_of_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '123' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": [ "g1", "g2" ]
    },
    "concurrency": 2,
    "interval": "20ms",
    "as_of": "-30s",
    "latency_thresh": "1s"
}]`),
		gens,
	)
	assert.NotNil(err)
	assert.Equal("as_of_test: $AS_OF is missing in "+
		"an AS_OF query SELECT * FROM sqload.simple_workload AS OF "+
		"SYSTEM TIME '123' where job_id > $1 and job_id < $2", err.Error())

	assert.Nil(work)
}

func TestWorkloadAsOfParsingInvalidMultiTxn(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	// $AS_OF variable is missing
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "as_of_test",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '123' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }, {
        "query": "SELECT * FROM sqload.simple_workload where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": [ "g1", "g2" ]
    },
    "concurrency": 2,
    "interval": "20ms",
    "as_of": "-30s",
    "latency_thresh": "1s"
}]`),
		gens,
	)
	assert.NotNil(err)
	assert.Equal("as_of_test: Only 1 query is supported in AS_OF workloads", err.Error())

	assert.Nil(work)
}

func TestWorkloadIntUniformGeneratorParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_uniform",
            "params": {
                "min": -20,
                "max": 100,
                "cardinality": 10000,
                "random_salt": 10
            }
        },
        "g2": {
            "name": "int_uniform",
            "params": {
                "min": 40,
                "max": 41,
                "random_salt": 12
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.Nil(err)
	assert.NotNil(work)
	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "get_a_from_foo",
		Transaction: []query{
			{
				Query:       "SELECT a FROM foo WHERE c = $1 or d = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         -20,
						Max:         100,
						RandomSalt:  10,
						Cardinality: 10000,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         40,
						Max:         41,
						RandomSalt:  12,
						Cardinality: math.MaxUint64,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])
}

func TestWorkloadIntUniformGeneratorEqualMinMax(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_uniform",
            "params": {
                "min": 40,
                "max": 40,
                "random_salt": 12
            }
        }
    }`))
	assert.NotNil(err)
	assert.Equal(err.Error(), "g1::generator: min 40 is >= max 40")
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Nil(work)
	assert.Equal(err.Error(), "generator g1 not found in get_a_from_foo")
}

func TestWorkloadInvalidIntUniformGeneratorParsing(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_uniform",
            "params": {
                "min": 100,
                "max": 20,
                "cardinality": 10000,
                "random_salt": 10
            }
        },
        "g2": {
            "name": "int_uniform",
            "params": {
                "min": 40,
                "max": 41,
                "random_salt": 12
            }
        }
    }`))
	assert.NotNil(err)
	assert.Equal("g1::generator: min 100 is >= max 20", err.Error())

	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Nil(work)
	assert.Equal("generator g1 not found in get_a_from_foo", err.Error())
}

func TestWorkloadMissingGenerator(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_uniform",
            "params": {
                "min": 40,
                "max": 41,
                "random_salt": 12
            }
        }
    }`))
	assert.Nil(err)

	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Nil(work)
	assert.Equal("generator g2 not found in get_a_from_foo", err.Error())

	work, err = ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 or d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)
	assert.Equal("generator g1 not found in get_a_from_foo", err.Error())
}

func TestWorkloadMissingGeneratorParamValidation(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300
            }
        },
        "g2": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 1000,
                "mean": 200,
                "sd": 30
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [{
        "query": "SELECT a FROM foo WHERE c = $1 AND d = $3",
        "num_rows": 1
    }],
    "params": {
        "generators": [ "g1", "g2" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "get_a_from_foo: Param '$2' was never used or "+
		"was used but not defined")

	assert.Nil(work)
}

func TestWorkloadTransactionShouldHaveAtleastOneQueryValidation(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "params": {
        "generators": [ "g1" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "get_a_from_foo: Transaction block should "+
		"have atleast one query")

	assert.Nil(work)

	work, err = ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_from_foo",
    "transaction": [],
    "params": {
        "generators": [ "g1" ]
    },
    "concurrency": 5,
    "interval": "20ms"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "get_a_from_foo: Transaction block should have "+
		"atleast one query")

	assert.Nil(work)
}

func TestWorkloadNonTailSelectRowCountGreaterThanOne(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        }
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "read",
        "num_rows": 2
    }, {
        "query": "UPDATE bar SET a = $read:1:string WHERE b = $read:2:int",
        "num_rows": 5
    }],
    "params": {
				"generators": ["g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo_bar: query '1' must return exactly 1 row,"+
		" but returns '2' rows")

	assert.Nil(work)
}

func TestWorkloadNonTailUpdateRowCountGreaterThanOne(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        }
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "query": "UPDATE foo SET a = $1, b = $2 WHERE c = $1",
        "result_name": "updated_rows",
        "num_rows": 2
    }, {
        "query": "UPDATE bar SET a = $1 WHERE b = $updated_rows:row_count",
        "num_rows": 5
    }],
    "params": {
				"generators": ["g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.Nil(err)
	assert.NotNil(work)
}

func TestWorkloadTailSelectRowCountGreaterThanOne(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        },
	        "g3": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 0
	          }
	        }
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "query": "UPDATE foo SET a = $1, b = $2 WHERE c = $3",
        "result_name": "updated_rows",
        "num_rows": 2
    }, {
        "query": "SELECT a, b FROM bar WHERE`+
			` c = $updated_rows:row_count AND e = $2",
        "num_rows": 2
    }],
    "params": {
				"generators": ["g1", "g2", "g3"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.Nil(err)

	assert.NotNil(work)
}

func TestWorkloadValidationForResultDataBasedParamForUpdate(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "write_result_field",
    "transaction": [{
        "query": "UPDATE bar SET a = $1 WHERE b = $2",
        "result_name": "write",
        "num_rows": 5
    }, {
        "query": "UPDATE baz SET a = $write:1:int WHERE b = $write:row_count",
        "num_rows": 5
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "write_result_field: query result column-value"+
		" based parameter '$write:1' is only defined for 'SELECT' queries")

	assert.Nil(work)
}

func TestWorkloadValidationForConflictingTypeDeclForResultParams(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar_baz",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "read",
        "num_rows": 1
    }, {
        "query": "UPDATE bar SET a = $read:1:string WHERE b = $read:2:int",
        "result_name": "write",
        "num_rows": 5
    }, {
        "query": "UPDATE baz SET a = $read:1:int WHERE b = $write:row_count",
        "num_rows": 5
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo_bar_baz: '$read:1' type expectation"+
		" conflicts at query '3'")

	assert.Nil(work)
}

func TestWorkloadValidationForUnknownResultName(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "read",
        "num_rows": 1
    }, {
        "query": "UPDATE bar SET a = $foo:1:string WHERE b = $read:2:int",
        "result_name": "write",
        "num_rows": 5
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo_bar: uses unknown result-name in"+
		" result-param '$foo:1'")

	assert.Nil(work)
}

func TestWorkloadValidationForConflictingResultName(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "foo_a_b",
        "num_rows": 1
    }, {
        "query": "UPDATE foo SET a = $3 WHERE b = $4",
        "result_name": "foo_a_b",
        "num_rows": 5
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo: result name 'foo_a_b' used in query '2'"+
		" conflicts with another query before it")

	assert.Nil(work)
}

func TestWorkloadValidationForConflictingNames(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	            }
	          },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
	          }
	        }
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "read",
        "num_rows": 1
    }],
    "params": {
				"generators": ["g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}, {
    "name": "foo",
    "transaction": [{
        "query": "UPDATE foo SET a = $1 WHERE b = $2",
        "result_name": "read",
        "num_rows": 1
    }],
    "params": {
				"generators": ["g1", "g2"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "name 'foo' used in Workload '2' "+
		"conflicts with another workload before it")

	assert.Nil(work)
}

func TestWorkloadValidationForGeneratorInvalidStrHistoMinMaxDistribution(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 100,
                    "max": 90,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 66.7
                }]
            }
        }
    }`))
	assert.NotNil(err)
	assert.Equal("g1::generator: min 100 is greater than max 90", err.Error())
	workload := `[{
    "name": "write_something",
    "transaction": [{
        "query": "UPDATE bar SET a = 10 WHERE b = $1",
        "num_rows": 2
    }],
    "params": {
        "generators" : [ "g1" ]
    },
    "concurrency": 1,
    "interval": "1s",
    "latency_thresh": "1s"
}]`

	work, err := ParseWorkLoad(ctx, []byte(workload), gens)

	assert.NotNil(err)
	assert.Equal("generator g1 not found in write_something", err.Error())
	assert.Nil(work)

	gens, err = parseGenerators([]byte(`{
        "g1": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 1,
                    "max": 2,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 66.7
                }],
                "prefix": [{
                    "str": "ftp://",
                    "pct": 20.0
                }, {
                    "str": "rsync://",
                    "pct": 80.0
                }]
            }
        }
    }`))
	assert.NotNil(err)
	assert.Equal("g1::generator: min string length 1 is greater than max prefix length 8", err.Error())
	workload = `[{
    "name": "write_something",
    "transaction": [{
        "query": "UPDATE bar SET a = 10 WHERE b = $1",
        "num_rows": 2
    }],
    "params": {
        "generators" : [ "g1" ]
    },
    "concurrency": 1,
    "interval": "1s",
    "latency_thresh": "1s"
}]`

	work, err = ParseWorkLoad(ctx, []byte(workload), gens)

	assert.NotNil(err)
	assert.Equal("generator g1 not found in write_something", err.Error())

	assert.Nil(work)
}

func TestWorkloadValidationForGeneratorClassIntervalCorrectness(t *testing.T) {
	assert := assert.New(t)
	gensTempl := `{
        "g1": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 50,
                    "max": 100,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 33.3
                }, {
                    "min":201,
                    "max":1000,
                    "pct": %s
                }],
                "prefix": [{
                    "str": "http://",
                    "pct": %s
                }, {
                    "str": "ftp://",
                    "pct": 20.0
                }, {
                    "str": "rsync://",
                    "pct": 14.0
                }]
            }
        },
        "g2": {
            "name": "float_histo",
            "params": {
                "cardinality": 20000,
                "value": [{
                    "min": 0.0,
                    "max": 2000.0,
                    "pct": 10.0
                }, {
                    "min": 5000.0,
                    "max": 10000.0,
                    "pct": %s
                }, {
                    "min":10000.0,
                    "max":100000.0,
                    "pct": 60.0
                }, {
                    "min":100000.0,
                    "max":500000.0,
                    "pct": 10.0
                }]
            }
        }
    }`

	work, err := parseGenerators([]byte(
		fmt.Sprintf(gensTempl, "33.2", "66.0", "20.0")))

	assert.NotNil(err)
	assert.Equal(err.Error(), "g1::generator: class-intervals"+
		" aren't mutually-exclusive and collectively-exhaustive on "+
		"histogram 'len' as they sum up to '99.80%'")

	assert.Nil(work)

	work, err = parseGenerators([]byte(
		fmt.Sprintf(gensTempl, "33.4", "65.0", "20.0")))

	assert.NotNil(err)
	assert.Equal(err.Error(), "g1::generator: class-intervals"+
		" aren't mutually-exclusive and collectively-exhaustive on "+
		"histogram 'prefix' as they sum up to '99.00%'")

	assert.Nil(work)

	work, err = parseGenerators([]byte(
		fmt.Sprintf(gensTempl, "33.4", "66.0", "22.0")))

	assert.NotNil(err)
	assert.Equal(err.Error(), "g2::generator: class-intervals"+
		" aren't mutually-exclusive and collectively-exhaustive on "+
		"histogram 'value' as they sum up to '102.00%'")

	assert.Nil(work)
}

func TestWorkloadValidationForParamSources(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1",
        "result_name": "read",
        "num_rows": 1
    }],
    "concurrency": 1,
    "interval": "1s",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo: Transaction expects '1' params,"+
		" but '0' generators are declared")

	assert.Nil(work)

	work, err = ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1",
        "result_name": "read",
        "num_rows": 1
    }],
    "params": {},
    "concurrency": 1,
    "interval": "1s",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo: Transaction expects '1' params,"+
		" but '0' generators are declared")

	assert.Nil(work)
}

func TestWorkloadValidationForMalformedResultName(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "foo_bar",
    "transaction": [{
        "query": "SELECT a, b FROM foo WHERE c = $1 AND e = $2",
        "result_name": "@",
        "num_rows": 1
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.NotNil(err)
	assert.Equal(err.Error(), "foo_bar: result_name for query '0'"+
		" is not a [[:word:]]")

	assert.Nil(work)
}

func TestWorkloadShouldDistinguishReadFromWriteQueries(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
	        "g1": {
	          "name": "int_uniform",
	          "params": {
	            "min": 1,
	            "cardinality": 9223372036854775807,
	            "max": 4611686018427387903,
	            "random_salt": 5
	          }
	        },
	        "g2": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 7
            }
	        },
	        "g3": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 8
	          }
	        },
	        "g4": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 9
	          }
	        },
	        "g5": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 0
	          }
	        },
	        "g6": {
	          "name": "int_uniform",
	          "params": {
	            "min": 4611686018427387903,
	            "cardinality": 9223372036854775807,
	            "max": 9223372036854775807,
	            "random_salt": 123
	           }
					}
	    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "a_where_c",
    "transaction": [{
        "query": " sElEct a from foo where c = $1",
        "num_rows": 1,
        "result_name": "one"
    },{
        "query": " upDAte bar set a = $2 where b = $3",
        "num_rows": 1,
        "result_name": "two"
    },{
        "query": "INSert into quux values ($4, $5)",
        "num_rows": 1,
        "result_name": "three"
    },{
        "query": "delETE from corge where a = $6",
        "num_rows": 1,
        "result_name": "four"
    }],
    "params": {
				"generators": ["g1","g2", "g3", "g4", "g5", "g6"]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)

	assert.Nil(err)
	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "a_where_c",
		Transaction: []query{
			{
				Query:       " sElEct a from foo where c = $1",
				NumRows:     1,
				ResultName:  "one",
				IsReadQuery: true},
			{
				Query:       " upDAte bar set a = $2 where b = $3",
				NumRows:     1,
				ResultName:  "two",
				IsReadQuery: false,
			},
			{
				Query:       "INSert into quux values ($4, $5)",
				NumRows:     1,
				ResultName:  "three",
				IsReadQuery: false,
			},
			{
				Query:       "delETE from corge where a = $6",
				NumRows:     1,
				ResultName:  "four",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2", "g3", "g4", "g5", "g6"},
			gens: []generatorSpec{
				{
					gen: &intUniformGenerator{
						Min:         1,
						Max:         4611686018427387903,
						RandomSalt:  5,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  7,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  8,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  9,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  0,
						Cardinality: 9223372036854775807,
					},
				},
				{
					gen: &intUniformGenerator{
						Min:         4611686018427387903,
						Max:         9223372036854775807,
						RandomSalt:  123,
						Cardinality: 9223372036854775807,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])
}

func TestWorkloadShouldFailWhenQueryTypeDetectionFails(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "a_where_c",
    "transaction": [{
        "query": "blah a from foo where c = $1",
        "num_rows": 1
    }],
    "params": {
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		nil,
	)

	assert.Nil(work)

	assert.NotNil(err)
	assert.Equal(err.Error(), "a_where_c: could not detect if '"+
		"blah a from foo where c = $1"+
		"' is a read or a wite query")
}

func TestGeneratorRandomSaltDefaulting(t *testing.T) {
	ctx := context.Background()
	randomSaltProvider.value = 0
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 1000,
                "mean": 200,
                "sd": 3000
            }
        },
        "g2": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 500,
                "mean": 100,
                "sd": 1500
            }
        },
        "g3": {
            "name": "float_histo",
            "params": {
                "cardinality": 2000,
                "value": [{
                    "min": 0.0,
                    "max": 2000.0,
                    "pct": 80.0
                }, {
                    "min": 5000.0,
                    "max": 10000.0,
                    "pct": 20.0
                }],
                "random_salt": 1337
            }
        },
        "g4": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 500,
                "mean": 100,
                "sd": 1500
            }
        },
        "g5": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300
            }
        },
        "g6": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 50,
                    "max": 100,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 33.3
                }, {
                    "min":201,
                    "max":1000,
                    "pct": 33.4
                }],
                "prefix": [{
                    "str": "http://",
                    "pct": 66.0
                }, {
                    "str": "ftp://",
                    "pct": 20.0
                }, {
                    "str": "rsync://",
                    "pct": 14.0
                }],
                "random_salt": 0
            }
        },
        "g7": {
            "name": "float_histo",
            "params": {
                "cardinality": 20000,
                "value": [{
                    "min": 0.0,
                    "max": 2000.0,
                    "pct": 10.0
                }, {
                    "min": 5000.0,
                    "max": 10000.0,
                    "pct": 20.0
                }, {
                    "min":10000.0,
                    "max":100000.0,
                    "pct": 60.0
                }, {
                    "min":100000.0,
                    "max":500000.0,
                    "pct": 10.0
                }]
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "read_foo",
    "transaction": [{
        "query":
 "SELECT a, b FROM foo WHERE c = $1 AND d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators" : [ "g1", "g2" ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}, {
    "name": "read_bar",
    "transaction": [{
        "query":
 "SELECT a, b FROM bar WHERE c = $1 AND d = $2",
        "num_rows": 1
    }],
    "params": {
        "generators" : [ "g3", "g4" ]
    },
    "concurrency": 2,
    "interval": "10ms",
    "latency_thresh": "1s"
}, {
    "name": "set_a_b_in_bar",
    "transaction": [{
        "query": "UPDATE bar SET a = $1, b = $2 WHERE c = $3",
        "num_rows": 2
    }],
    "params": {
        "generators" : [ "g5", "g6", "g7" ]
    },
    "concurrency": 1,
    "interval": "1s",
    "latency_thresh": "1s"
}]`),
		gens,
	)
	assert.Nil(err)

	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "read_foo",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM foo WHERE c = $1 AND d = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g1", "g2"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 1000,
						Mean:        200,
						StdDev:      3000,
						RandomSalt:  0,
					},
				},
				{
					gen: &intGaussianGenerator{
						Cardinality: 500,
						Mean:        100,
						StdDev:      1500,
						RandomSalt:  1,
					},
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])

	// Random source is tested separately
	work[1].rs = nil
	assert.Equal(WorkLoad{
		Name: "read_bar",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM bar WHERE c = $1 AND d = $2",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"g3", "g4"},
			gens: []generatorSpec{
				{
					gen: &floatHistoGenerator{
						2000,
						[]floatHistoClassInterval{
							{0.0, 2000.0, 80.0},
							{5000.0, 10000.0, 20.0},
						},
						1337,
					},
				},
				{
					gen: &intGaussianGenerator{
						Cardinality: 500,
						Mean:        100,
						StdDev:      1500,
						RandomSalt:  3,
					},
				},
			},
		},
		Concurrency:   2,
		Interval:      duration{time.Duration(10 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[1])

	// Random source is tested separately
	work[2].rs = nil
	assert.Equal(WorkLoad{
		Name: "set_a_b_in_bar",
		Transaction: []query{
			{
				Query:       "UPDATE bar SET a = $1, b = $2 WHERE c = $3",
				NumRows:     2,
				ResultName:  "",
				IsReadQuery: false,
			},
		},
		Params: params{
			GeneratorNames: []string{"g5", "g6", "g7"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  4,
					},
				},
				{
					gen: &strHistoLenGenerator{
						50000,
						histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						[]strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						0,
					},
				},
				{
					gen: &floatHistoGenerator{
						20000,
						[]floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						6,
					},
				},
			},
		},
		Concurrency:   1,
		Interval:      duration{time.Duration(1 * time.Second)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[2])
}

func TestWorkloadRandomSource(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators([]byte(`{
        "g1": {
          "name": "int_uniform",
          "params": {
            "min": 1,
            "cardinality": 9223372036854775807,
            "max": 4611686018427387903,
            "random_salt": 5
            }
          },
        "g2": {
          "name": "int_uniform",
          "params": {
            "min": 4611686018427387903,
            "cardinality": 9223372036854775807,
            "max": 9223372036854775807,
            "random_salt": 7
          }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoadWithLoadOpts(
		ctx,
		[]byte(`[{
    "name": "no_rollover",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": ["g1", "g2"]
    },
    "concurrency": 2,
    "interval": "20ms",
    "latency_thresh": "100ms",
		"force_ignore_rollover": true
}, {
    "name": "rollover",
    "transaction": [{
        "query": "SELECT * FROM sqload.simple_workload AS OF SYSTEM TIME '$AS_OF' where job_id > $1 and job_id < $2",
        "num_rows": 1
      }],
    "params": {
      "generators": ["g1", "g2"]
    },
    "concurrency": 2,
    "interval": "20ms",
    "latency_thresh": "100ms"
}]`),
		gens,
		&sqloadOptions{
			loaderID: 124,
		},
	)
	assert.Nil(err)
	assert.NotNil(work)
	assert.Equal(2, len(work))
	assert.Equal(uint64(0x8e5c099d15d1be4e), <-work[0].rs)
	assert.Equal(true, work[0].ForceIgnoreRollOver)

	assert.Equal(uint64(0x8e5c099d15d1be4e), <-work[1].rs)
	assert.Equal(false, work[1].ForceIgnoreRollOver)
}

func TestWorkloadMissingRandSource(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	work := []*WorkLoad{
		{
			Name: "set_a_b_in_bar",
			Transaction: []query{
				{
					Query:       "UPDATE bar SET a = $1, b = $2 WHERE c = $3",
					NumRows:     2,
					ResultName:  "",
					IsReadQuery: false,
				},
			},
			Params: params{
				GeneratorNames: []string{"g5", "g6", "g7"},
				gens: []generatorSpec{
					{
						gen: &intGaussianGenerator{
							Cardinality: 10000,
							Mean:        2000,
							StdDev:      300,
							RandomSalt:  4,
						},
					},
					{
						gen: &strHistoLenGenerator{
							50000,
							histogram{
								{50, 100, 33.3},
								{101, 200, 33.3},
								{201, 1000, 33.4},
							},
							[]strPrefixClassInterval{
								{"http://", 66.0},
								{"ftp://", 20.0},
								{"rsync://", 14.0},
							},
							0,
						},
					},
					{
						gen: &floatHistoGenerator{
							20000,
							[]floatHistoClassInterval{
								{0.0, 2000.0, 10.0},
								{5000.0, 10000.0, 20.0},
								{10000.0, 100000.0, 60.0},
								{100000.0, 500000.0, 10.0},
							},
							6,
						},
					},
				},
			},
			Concurrency:   1,
			Interval:      duration{time.Duration(1 * time.Second)},
			AsOf:          "",
			LatencyThresh: "1s",
		},
	}
	work[0].rs = nil
	err := defaultAndValidate(ctx, work)
	assert.NotNil(err)
	assert.Equal(err.Error(), "set_a_b_in_bar: Random source not initialized")
}

func TestWorkloadParsingSaltOffset(t *testing.T) {
	ctx := context.Background()
	assert := assert.New(t)
	gens, err := parseGenerators(
		[]byte(`{
        "intg_g": {
            "name": "int_gaussian",
            "params": {
                "cardinality": 10000,
                "mean": 2000,
                "sd": 300,
                "random_salt": 100
            }
        },
        "intu_g": {
            "name": "int_uniform",
            "params": {
                "cardinality": 10000,
								"min": 10,
								"max": 100,
                "random_salt": 200
            }
        },
        "ts_g": {
            "name": "timestamp_offset",
            "params": {
                "offset": "-5m",
                "max_deviation": "10s",
                "random_salt": 300
            }
        },
        "str_g": {
            "name": "str_histo_prefix_len",
            "params": {
                "cardinality": 50000,
                "len": [{
                    "min": 50,
                    "max": 100,
                    "pct": 33.3
                }, {
                    "min": 101,
                    "max": 200,
                    "pct": 33.3
                }, {
                    "min":201,
                    "max":1000,
                    "pct": 33.4
                }],
                "prefix": [{
                    "str": "http://",
                    "pct": 66.0
                }, {
                    "str": "ftp://",
                    "pct": 20.0
                }, {
                    "str": "rsync://",
                    "pct": 14.0
                }],
                "random_salt": 400
            }
        },
        "float_g": {
            "name": "float_histo",
            "params": {
                "cardinality": 20000,
                "value": [{
                    "min": 0.0,
                    "max": 2000.0,
                    "pct": 10.0
                }, {
                    "min": 5000.0,
                    "max": 10000.0,
                    "pct": 20.0
                }, {
                    "min":10000.0,
                    "max":100000.0,
                    "pct": 60.0
                }, {
                    "min":100000.0,
                    "max":500000.0,
                    "pct": 10.0
                }],
                "random_salt": 500
            }
        }
    }`))
	assert.Nil(err)
	work, err := ParseWorkLoad(
		ctx,
		[]byte(`[{
    "name": "get_a_b_from_foo",
    "transaction": [{
        "query":
 "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4 AND g = $5",
        "num_rows": 1
    }],
    "params": {
        "generators" : [
            "intg_g:1",
            "intu_g:2",
            "ts_g:1",
            "str_g:3",
            "float_g:25"
        ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}, {
    "name": "get_a_b_from_foo_2",
    "transaction": [{
        "query":
 "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4 AND g = $5",
        "num_rows": 1
    }],
    "params": {
        "generators" : [
            "intg_g:25",
            "intu_g:26",
            "ts_g:28",
            "str_g:30",
            "float_g:33"
        ]
    },
    "concurrency": 5,
    "interval": "20ms",
    "latency_thresh": "1s"
}]`),
		gens,
	)
	assert.Nil(err)

	// Random source is tested separately
	work[0].rs = nil
	assert.Equal(WorkLoad{
		Name: "get_a_b_from_foo",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4 AND g = $5",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"intg_g:1", "intu_g:2", "ts_g:1", "str_g:3", "float_g:25"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  100,
					},
					saltOffset: 1,
				},
				{
					gen: &intUniformGenerator{
						Cardinality: 10000,
						Min:         10,
						Max:         100,
						RandomSalt:  200,
					},
					saltOffset: 2,
				},
				{
					gen: &timestampGenerator{
						Offset:       duration{-5 * time.Minute},
						MaxDeviation: duration{10 * time.Second},
						RandomSalt:   300,
					},
					saltOffset: 1,
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						Prefix: []strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						RandomSalt: 400,
					},
					saltOffset: 3,
				},
				{
					gen: &floatHistoGenerator{
						Cardinality: 20000,
						Values: []floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						RandomSalt: 500,
					},
					saltOffset: 25,
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[0])

	// Random source is tested separately
	work[1].rs = nil
	assert.Equal(WorkLoad{
		Name: "get_a_b_from_foo_2",
		Transaction: []query{
			{
				Query:       "SELECT a, b FROM foo WHERE c = $1 AND d = $2 AND e = $3 AND f = $4 AND g = $5",
				NumRows:     1,
				ResultName:  "",
				IsReadQuery: true,
			},
		},
		Params: params{
			GeneratorNames: []string{"intg_g:25", "intu_g:26", "ts_g:28", "str_g:30", "float_g:33"},
			gens: []generatorSpec{
				{
					gen: &intGaussianGenerator{
						Cardinality: 10000,
						Mean:        2000,
						StdDev:      300,
						RandomSalt:  100,
					},
					saltOffset: 25,
				},
				{
					gen: &intUniformGenerator{
						Cardinality: 10000,
						Min:         10,
						Max:         100,
						RandomSalt:  200,
					},
					saltOffset: 26,
				},
				{
					gen: &timestampGenerator{
						Offset:       duration{-5 * time.Minute},
						MaxDeviation: duration{10 * time.Second},
						RandomSalt:   300,
					},
					saltOffset: 28,
				},
				{
					gen: &strHistoLenGenerator{
						Cardinality: 50000,
						Len: histogram{
							{50, 100, 33.3},
							{101, 200, 33.3},
							{201, 1000, 33.4},
						},
						Prefix: []strPrefixClassInterval{
							{"http://", 66.0},
							{"ftp://", 20.0},
							{"rsync://", 14.0},
						},
						RandomSalt: 400,
					},
					saltOffset: 30,
				},
				{
					gen: &floatHistoGenerator{
						Cardinality: 20000,
						Values: []floatHistoClassInterval{
							{0.0, 2000.0, 10.0},
							{5000.0, 10000.0, 20.0},
							{10000.0, 100000.0, 60.0},
							{100000.0, 500000.0, 10.0},
						},
						RandomSalt: 500,
					},
					saltOffset: 33,
				},
			},
		},
		Concurrency:   5,
		Interval:      duration{time.Duration(20 * time.Millisecond)},
		AsOf:          "",
		LatencyThresh: "1s",
	}, *work[1])
}

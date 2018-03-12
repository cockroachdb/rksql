package sqload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"rubrik/util/log"
)

type defaultRandomSaltProvider struct {
	value uint64
}

var (
	randomSaltProvider = defaultRandomSaltProvider{0}
)

func (sp *defaultRandomSaltProvider) next() uint64 {
	val := sp.value
	sp.value++
	return val
}

type classIntervalExhaustivenessError struct {
	pctSum float32
	name   string
}

func (e *classIntervalExhaustivenessError) Error() string {
	return fmt.Sprintf("class-intervals aren't "+
		"mutually-exclusive and collectively-exhaustive "+
		"on histogram '%s' as they sum up to '%0.2f%%'", e.name, e.pctSum)
}

// generator Interface for generator configuration
type generator interface {
	InitParams()     // Initialize any params required by generator
	Validate() error // Validate if the parameters configured are valid
}

// generators is a map from generator name to the actual generator
type generators map[string]generator

// Interface for a generator
type valueGenerator interface {
	Next(seed uint64) (interface{}, error) // Return a generated value based on seed
}

type generatedParamReader struct {
	rs       <-chan uint64
	colCount int
	currRow  []interface{}
	colGens  []valueGenerator
}

// Every row generated is seeded by a workload level random number
// generator (rdr.rs). This is shared by all the goroutines
// executing the workload so the order or random numbers used here
// is independent of concurrency
// This random number is used to seed all the generators which are
// generating values for each column of the row. Each individual
// generator "salts" this seed with its random_salt.
// random_salt can be used so that multiple generators of the same
// configuration do not generate the same random numbers
func (rdr *generatedParamReader) NextRow(
	ctx context.Context,
) ([]interface{}, error) {
	r, ok := <-rdr.rs
	if !ok {
		// Channel closed. Load generation has completed
		return nil, nil
	}

	var err error
	for i, cg := range rdr.colGens {
		if log.V(9) {
			log.Infof(ctx, "Got random seed: %d", r)
		}
		rdr.currRow[i], err = cg.Next(r)
		if err != nil {
			return nil, err
		}
		if log.V(9) {
			log.Infof(ctx, "Produced value: %d", rdr.currRow[i])
		}
	}

	return rdr.currRow, nil
}

func (rdr *generatedParamReader) Close() error {
	return nil
}

func colGenerator(gs generatorSpec) (valueGenerator, error) {
	var err error
	var colGen valueGenerator
	switch g := gs.gen.(type) {
	case *intGaussianGenerator:
		colGen, err = newIntGaussianColGenerator(*g, gs.saltOffset)
	case *timestampGenerator:
		colGen, err = newTimestampColGenerator(*g, gs.saltOffset)
	case *strHistoLenGenerator:
		colGen, err = newStrHistoLenColGenerator(*g, gs.saltOffset)
	case *intUniformGenerator:
		colGen, err = newIntUniformColGenerator(*g, gs.saltOffset)
	default:
		return nil, errors.New("generator type not supported yet")
	}
	return colGen, err
}

func makeGeneratedParamReader(
	gSpecs []generatorSpec,
	rs <-chan uint64,
) (
	paramReader,
	error,
) {
	colCount := len(gSpecs)
	resultArray := make([]interface{}, colCount)
	colGens := make([]valueGenerator, colCount)
	for i, gs := range gSpecs {
		var err error
		colGens[i], err = colGenerator(gs)
		if err != nil {
			return nil, err
		}
	}
	rdr := generatedParamReader{
		rs:       rs,
		colCount: colCount,
		currRow:  resultArray,
		colGens:  colGens,
	}
	return &rdr, nil
}

type floatHistoClassInterval struct {
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
	Percentage float32 `json:"pct"`
}

type floatHistoGenerator struct {
	Cardinality uint64                    `json:"cardinality"`
	Values      []floatHistoClassInterval `json:"value"`
	RandomSalt  uint64                    `json:"random_salt"`
}

func (g *floatHistoGenerator) InitParams() {
	g.RandomSalt = randomSaltProvider.next()
}

func (g *floatHistoGenerator) Validate() error {
	classItvlSum := float32(0.0)
	for _, cItvl := range g.Values {
		classItvlSum += cItvl.Percentage
	}
	return validateClassIntervalSum("value", classItvlSum)
}

// UnmarshalJSON is used to convert json bytes to a generator
func (g generators) UnmarshalJSON(bytes []byte) error {
	var rawMsgs map[string]map[string]*json.RawMessage

	if err := json.Unmarshal(bytes, &rawMsgs); err != nil {
		return err
	}

	// Process generators in a deterministic order
	var genIDs []string
	for genID := range rawMsgs {
		genIDs = append(genIDs, genID)
	}
	sort.Strings(genIDs)

	for _, genID := range genIDs {
		rawMsg := rawMsgs[genID]
		nameVal, exists := rawMsg["name"]
		if !exists {
			return errors.New("generator name is absent")
		}

		var name string
		if err := json.Unmarshal(*nameVal, &name); err != nil {
			return err
		}

		rawParams := rawMsg["params"]
		var gen generator
		switch name {
		case "int_gaussian":
			gen = &intGaussianGenerator{
				Cardinality: math.MaxUint64,
			}
		case "int_uniform":
			gen = &intUniformGenerator{
				Min:         math.MinInt64,
				Max:         math.MaxInt64,
				Cardinality: math.MaxUint64,
			}
		case "str_histo_prefix_len":
			gen = &strHistoLenGenerator{
				Cardinality: math.MaxUint64,
			}
		case "float_histo":
			gen = &floatHistoGenerator{
				Cardinality: math.MaxUint64,
			}
		case "timestamp_offset":
			gen = &timestampGenerator{}
		default:
			return fmt.Errorf("unsupported generator: %s", name)
		}
		gen.InitParams()
		if err := json.Unmarshal(*rawParams, gen); err != nil {
			return err
		}
		g[genID] = gen
	}

	return nil
}

func (g generators) validateParamGeneratorDefinition() error {
	for genName, gen := range g {
		err := gen.Validate()
		if err != nil {
			return fmt.Errorf("%s::generator: %s",
				genName, err.Error())
		}
	}
	return nil
}

func (g generators) validate() error {
	if err := g.validateParamGeneratorDefinition(); err != nil {
		return err
	}
	return nil
}

//parseGenerators parses, loads and validates the workload spec content
func parseGenerators(genData []byte) (generators, error) {
	gen := make(generators)
	err := json.Unmarshal(genData, &gen)
	if err != nil {
		return nil, err
	}
	err = gen.validate()
	if err != nil {
		return nil, err
	}
	return gen, nil
}

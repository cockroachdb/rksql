package sqload

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"rubrik/sqload/randsrc"
	"rubrik/util/log"
)

type query struct {
	Query       string `json:"query"`
	NumRows     int    `json:"num_rows"`
	ResultName  string `json:"result_name"`
	IsReadQuery bool
}

//WorkLoad defines a sequence of queries, flow or a txn to be used for load gen
type WorkLoad struct {
	Name                string   `json:"name"`
	Concurrency         int      `json:"concurrency"`
	Interval            duration `json:"interval"`
	AsOf                string   `json:"as_of"`
	ForceIgnoreRollOver bool     `json:"force_ignore_rollover"`
	LatencyThresh       string   `json:"latency_thresh"` // Used by rk_pytest
	RunWithoutTx        bool     `json:"run_without_tx"`
	Transaction         []query
	Params              params
	rs                  <-chan uint64 // rand source
}

func (w WorkLoad) isAsOfQuery() bool {
	return len(w.AsOf) != 0
}

func validationError(name string, fmtStr string, params ...interface{}) error {
	return fmt.Errorf(name+": "+fmtStr, params...)
}

func validateQuery(ctx context.Context, work *WorkLoad) error {
	var prevNumRows int
	prevIsSelect := false
	queryCount := 0
	resultParamRefSites := make(map[string]colType)
	knownResults := make(map[string]resultDetails)
	resultNamePattern := regexp.MustCompile("^" + resultNameFormat + "$")

	if work.rs == nil {
		return validationError(work.Name, "Random source not initialized")
	}
	if len(work.Transaction) == 0 {
		return validationError(work.Name,
			"Transaction block should have atleast one query")
	}

	if work.isAsOfQuery() {
		// As Of Queries cannot be executed in a transaction block
		// It can contain only one statement
		if len(work.Transaction) != 1 {
			return validationError(work.Name,
				"Only 1 query is supported in AS_OF workloads")
		}

		q := work.Transaction[0].Query
		if !strings.Contains(q, "$AS_OF") {
			return validationError(work.Name,
				"$AS_OF is missing in an AS_OF query %s", q)
		}
	}

	// Validate Latency Threshold
	if len(work.LatencyThresh) == 0 {
		return validationError(work.Name, "Latency threshold is missing from workload")
	}
	_, err := time.ParseDuration(work.LatencyThresh)
	if err != nil {
		return validationError(work.Name, "Invalid duration %s", work.LatencyThresh)
	}

	for queryIdx, query := range work.Transaction {
		if prevIsSelect && (prevNumRows != 1) {
			return validationError(work.Name, "query '%d' must return exactly 1 row,"+
				" but returns '%d' rows", queryCount, prevNumRows)
		}
		queryCount++
		prevNumRows = query.NumRows
		prevIsSelect = query.IsReadQuery

		err := validateResultParamRefSites(ctx, work, resultParamRefSites,
			knownResults, query.Query, queryIdx)
		if err != nil {
			return err
		}

		resName := query.ResultName
		if (len(resName) != 0) && !resultNamePattern.MatchString(resName) {
			return validationError(work.Name,
				"result_name for query '%d' is not a [[:word:]]", queryIdx)
		}

		if _, present := knownResults[resName]; present {
			return validationError(work.Name, "result name '%s' used in query '%d'"+
				" conflicts with another query before it", resName, queryIdx+1)
		}

		knownResults[query.ResultName] = resultDetails{query.IsReadQuery}
	}
	return nil
}

func unidentifiableQueryClassError(name string, query string) error {
	return validationError(name, "could not detect if '%s' is "+
		"a read or a wite query", query)
}

func identifyReadOrWriteQuery(ctx context.Context, work *WorkLoad) error {
	queryClassPattern := regexp.MustCompile("^\\s*([[:alpha:]]+).*")

	for qIdx := range work.Transaction {
		query := &work.Transaction[qIdx]
		matchIdxs := queryClassPattern.FindAllStringSubmatchIndex(query.Query, -1)
		if matchIdxs == nil {
			return unidentifiableQueryClassError(work.Name, query.Query)
		}
		fromIdx, toIdx := matchIdxs[0][2], matchIdxs[0][3]
		queryClass := query.Query[fromIdx:toIdx]
		switch strings.ToUpper(queryClass) {
		case "SELECT":
			query.IsReadQuery = true
		case "UPDATE", "INSERT", "DELETE", "UPSERT":
			query.IsReadQuery = false
		default:
			return unidentifiableQueryClassError(work.Name, query.Query)
		}
		log.Infof(ctx, "Query class: %s [query: %s], is-read-query verdict: %v",
			queryClass, query.Query, query.IsReadQuery)
	}

	return nil
}

func defaultOrDiscoverFieldValues(ctx context.Context, work *WorkLoad) error {
	return identifyReadOrWriteQuery(ctx, work)
}

func defaultAndValidate(ctx context.Context, workLoad []*WorkLoad) error {
	knownWorkloadNames := make(map[string]bool)
	for workIdx, work := range workLoad {
		err := defaultOrDiscoverFieldValues(ctx, work)
		if err != nil {
			return err
		}

		if knownWorkloadNames[work.Name] {
			return fmt.Errorf("name '%s' used in Workload '%d'"+
				" conflicts with another workload before it", work.Name, workIdx+1)
		}
		knownWorkloadNames[work.Name] = true

		if err = validateQuery(ctx, work); err != nil {
			return err
		}
		if err := validateQueryParamCountMatch(work); err != nil {
			return err
		}
	}
	return nil
}

//populateGenerators adds generators to w based on the generator names
// in w and generator mapping in gens
func populateGenerators(w *WorkLoad, gens generators) error {
	for _, rawGenName := range w.Params.GeneratorNames {
		var gen generator
		var ok bool
		var err error
		genName := rawGenName
		saltOffset := uint64(0)
		// Generator names can be optionally defined as
		// gen_name:salt_offset
		genNameParts := strings.Split(rawGenName, ":")

		if len(genNameParts) == 2 {
			genName = genNameParts[0]
			saltOffset, err = strconv.ParseUint(genNameParts[1], 10, 64)
			if err != nil {
				return errors.Errorf(
					"exception parsing generator name %s",
					rawGenName,
				)
			}
		}

		if gen, ok = gens[genName]; !ok {
			return errors.Errorf(
				"generator %v not found in %v",
				genName,
				w.Name,
			)
		}

		w.Params.gens = append(
			w.Params.gens,
			generatorSpec{
				gen:        gen,
				saltOffset: saltOffset,
			},
		)
	}
	return nil
}

//ParseWorkLoadWithLoadOpts parses, loads and validates the workload spec content
// The generator names used in the workload are defined in idToGen
// loadOptions are used to configure the rand source
func ParseWorkLoadWithLoadOpts(
	ctx context.Context,
	rawData []byte,
	gens generators,
	opts *sqloadOptions,
) ([]*WorkLoad, error) {
	ctx = log.WithLogTag(ctx, "workload_parsing", nil)
	var workLoad []*WorkLoad
	err := json.Unmarshal(rawData, &workLoad)
	if err != nil {
		return nil, err
	}

	for _, w := range workLoad {
		if err := populateGenerators(w, gens); err != nil {
			return nil, err
		}

		// A single random number generator is used for the whole workload
		// so that concurrency does not impact the numbers generated
		var randSrcOpts []func(*randsrc.Source)
		if opts != nil {
			randSrcOpts = []func(*randsrc.Source){
				randsrc.Seed(int64(opts.loaderID)),
				randsrc.Repeat(opts.repeat),
				randsrc.RolloverThresh(opts.rolloverThresh),
				randsrc.ForceIgnoreRollover(w.ForceIgnoreRollOver),
			}
		}
		w.rs = randsrc.NewRandSrc(
			randSrcOpts...,
		)
	}

	err = defaultAndValidate(ctx, workLoad)
	if err != nil {
		return nil, err
	}

	return workLoad, nil
}

//ParseWorkLoad parses, loads and validates the workload spec content
// The generator names used in the workload are defined in idToGen
func ParseWorkLoad(
	ctx context.Context,
	rawData []byte,
	gens generators,
) ([]*WorkLoad, error) {
	return ParseWorkLoadWithLoadOpts(ctx, rawData, gens, nil)
}

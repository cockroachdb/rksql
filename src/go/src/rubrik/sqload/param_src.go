package sqload

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"rubrik/util/log"
)

type colType uint8

const (
	typeInt colType = iota
	typeFloat
	typeBool
	typeString
)

var errExtraPlaceholders = errors.New("num placeholders > num params")
var errExtraParams = errors.New("num placeholders < num params")

func parseColType(typeName string) (t colType, err error) {
	switch typeName {
	case "int":
		t = typeInt
	case "bool":
		t = typeBool
	case "float":
		t = typeFloat
	case "string":
		t = typeString
	default:
		err = fmt.Errorf("Unknown col-type %s", typeName)
	}
	return
}

func (t *colType) UnmarshalJSON(b []byte) (err error) {
	if len(b) < 2 {
		err = fmt.Errorf("colType value %s is too short to be valid", string(b))
		return
	}
	sd := string(bytes.Trim(b, "\""))
	*t, err = parseColType(sd)
	return
}

// paramReader provides query-param values
type paramReader interface {
	NextRow(ctx context.Context) ([]interface{}, error)
	Close() error
}

type generatorSpec struct {
	gen        generator
	saltOffset uint64
}
type params struct {
	GeneratorNames []string `json:"generators"`
	gens           []generatorSpec
}

func (p *params) Reader(rs <-chan uint64) (paramReader, error) {
	// GeneratedParamReader is controlled by rs (randSource)
	return makeGeneratedParamReader(p.gens, rs)
}

const (
	metaReturnParamRowCount = "row_count"
)

func paramNameForIdx(idx int) string {
	return fmt.Sprintf("$%d", idx+1)
}

func metaParamNameFor(resultName, paramName string) string {
	return fmt.Sprintf("$%s:%s", resultName, paramName)
}

func validateGeneratedParamsUtilization(
	work *WorkLoad,
	maxParamOrdinal int,
) error {
	l := len(work.Params.gens)
	if maxParamOrdinal != l {
		return validationError(work.Name, "Transaction expects '%d' params,"+
			" but '%d' generators are declared", maxParamOrdinal, l)
	}
	return nil
}

//getParamIndexes returns numeric index values for given query
// Eg. select a from foo where b = $1 and c = $2
// will return `[]int {1, 2}`
func getParamIndexes(query string) (paramOrdinals []int, err error) {
	paramsReferred := getParamsReferred(query)

	if paramsReferred == nil {
		paramOrdinals = []int{}
		return
	}
	paramOrdinals = make([]int, 0)
	for _, pRef := range paramsReferred {
		if pRef.resultParam {
			continue
		}
		fromIdx, toIdx := pRef.start+1, pRef.end //skip $
		pOrd, err := strconv.Atoi(query[fromIdx:toIdx])
		if err != nil {
			return nil, err
		}
		paramOrdinals = append(paramOrdinals, pOrd)
	}
	return
}

func validateQueryParamCountMatch(work *WorkLoad) error {
	paramsUsed := make(map[int]bool, 0)
	maxParam := 0
	for _, query := range work.Transaction {
		paramIdxs, err := getParamIndexes(query.Query)
		if err != nil {
			return err
		}
		for _, paramIdx := range paramIdxs {
			paramsUsed[paramIdx] = true
			if maxParam < paramIdx {
				maxParam = paramIdx
			}
		}
	}

	for i := 1; i <= maxParam; i++ {
		if !paramsUsed[i] {
			return validationError(work.Name,
				"Param '$%d' was never used or was used but not defined", i)
		}
	}

	return validateGeneratedParamsUtilization(work, maxParam)
}

const (
	resultParamQualifierDelimiter = ":"
)

func resultParamIdx(name string) (idx int, isMetaDataField bool, err error) {
	sepCount := strings.Count(name, resultParamQualifierDelimiter)
	switch sepCount {
	case 0:
		return -1, true, nil
	case 1:
		idxStart := strings.Index(name, resultParamQualifierDelimiter)
		idxStr := name[idxStart+1:]
		idx64, err := strconv.ParseInt(idxStr, 10, 64)
		if err != nil {
			return -1, false, err
		}
		return int(idx64 - 1), false, nil
	}
	return -1, false, fmt.Errorf("Unknown param-name format: '%s'", name)
}

func convertToInt(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int64:
		return val, nil
	case string:
		vInt, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}
		return vInt, nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	}
	return nil, fmt.Errorf(
		"Convertion of %#v [type: %v] to int is not supported",
		val,
		reflect.TypeOf(val))
}

func convertToFloat(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		vInt, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, err
		}
		return vInt, nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	}
	return nil, fmt.Errorf(
		"Convertion of %#v [type: %s] to float is not supported",
		val,
		reflect.TypeOf(val))
}

var (
	trues  = []string{"true", "t", "yes", "y", "on"}
	falses = []string{"false", "f", "no", "n", "off"}
)

func convertToBool(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float32:
		return v != float32(0.0), nil
	case float64:
		return v != float64(0.0), nil
	case string:
		v = strings.ToLower(v)
		for _, t := range trues {
			if t == v {
				return true, nil
			}
		}
		for _, f := range falses {
			if f == v {
				return false, nil
			}
		}
	case bool:
		return v, nil
	}
	return nil, fmt.Errorf(
		"Convertion of %#v [type: %s] to bool is not supported",
		val,
		reflect.TypeOf(val))
}

func convertToString(val interface{}) (interface{}, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	default:
		return fmt.Sprint(v), nil
	}
}

func convert(val interface{}, typ colType) (interface{}, error) {
	switch typ {
	case typeInt:
		return convertToInt(val)
	case typeFloat:
		return convertToFloat(val)
	case typeBool:
		return convertToBool(val)
	case typeString:
		return convertToString(val)
	}
	return nil, fmt.Errorf("Couldn't convert value %#v to type %d", val, typ)
}

//getResultParamExpectedType splits result-param-reference string
// into result-name, reference-index and type.
// Eg.
//  data-param:
//   refFrag="$foo:1:string" returns
//   resultName="foo", refName = "1", refType = typeString
//  result-attribute-param:
//   refFrag="$foo:row_count" returns
//   resultName="foo", refName = "row_count", refType = typeInt
func getResultParamExpectedType(workloadName, refFrag string) (
	resultName string,
	refName string,
	refType colType,
	err error,
) {
	count := strings.Count(refFrag, resultParamQualifierDelimiter)
	var tailFrag string
	if count > 0 {
		typePartIdx := strings.LastIndex(refFrag, resultParamQualifierDelimiter)
		tailFrag = refFrag[typePartIdx+1:]
		refName = refFrag[0:typePartIdx]
		namePartCount := strings.Count(refName, resultParamQualifierDelimiter)
		if namePartCount > 0 { // skip litteral '$' at idx = 0
			resultName =
				refName[1:strings.LastIndex(refName, resultParamQualifierDelimiter)]
		} else {
			resultName = refName[1:]
		}
	}
	switch count {
	case 2: //result param
		refType, err = parseColType(tailFrag)
		return
	case 1: //row count
		if tailFrag == metaReturnParamRowCount {
			refType = typeInt
			return
		}
	}
	err = validationError(workloadName,
		"found ill-formed result-param '%s'", refFrag)
	return
}

const (
	resultNameFormat  = "[[:word:]]+"
	paramSrcRefFormat = "\\$[[:digit:]]+"
)

var /*const*/ (
	resultColRefFormat = "\\$" +
		resultNameFormat + resultParamQualifierDelimiter +
		"[[:digit:]]+" + resultParamQualifierDelimiter +
		"[[:alpha:]]+"

	resultAttrFormat = "\\$" +
		resultNameFormat + resultParamQualifierDelimiter +
		"[[:word:]]+"

	paramPattern = regexp.MustCompile(".+?(" +
		"(" + paramSrcRefFormat + ")|" +
		"(" + resultColRefFormat + ")|" +
		"(" + resultAttrFormat + ")" +
		").*?")
)

type paramPos struct {
	start, end  int
	resultParam bool
}

func isResultParam(frag string) bool {
	return strings.Count(frag, resultParamQualifierDelimiter) > 0
}

func getParamsReferred(q string) []paramPos {
	matchIdxs := paramPattern.FindAllStringSubmatchIndex(q, -1)
	var paramsReferred []paramPos
	for _, matchIdx := range matchIdxs {
		start, end := matchIdx[2], matchIdx[3]
		paramsReferred = append(
			paramsReferred,
			paramPos{start, end, isResultParam(q[start:end])})
	}
	return paramsReferred
}

type resultDetails struct {
	readQuery bool
}

//validateResultParamRefSites does a few different types of checks
// 1. ensure a result-name referred to actually exists
// 2. ensure no column-data is referred to unless its a SELECT result
// 3. ensure there is no type-expectation conflict
//  (if one ref-site expects an $foo:1:int, another can't expect a string)
func validateResultParamRefSites(
	ctx context.Context,
	work *WorkLoad,
	refSites map[string]colType,
	knownResults map[string]resultDetails,
	query string,
	queryIdx int,
) error {

	paramsReferred := getParamsReferred(query)

	for _, pRef := range paramsReferred {
		if !pRef.resultParam {
			continue
		}
		refFrag := query[pRef.start:pRef.end]
		resultName, refName, expectedType, err :=
			getResultParamExpectedType(work.Name, refFrag)
		if err != nil {
			return err
		}
		if _, known := knownResults[resultName]; !known {
			return validationError(work.Name,
				"uses unknown result-name in result-param '%s'", refName)
		}
		_, isMetadataField, err := resultParamIdx(refName)
		if err != nil {
			return validationError(work.Name, err.Error())
		}
		if log.V(8) {
			log.Infof(ctx, "Result: '%s' has a read-query: '%v' "+
				"AND '%s' is a meta-data field: %v",
				resultName, knownResults[resultName].readQuery,
				refName, isMetadataField)
		}
		if (!knownResults[resultName].readQuery) && (!isMetadataField) {
			return validationError(work.Name, "query result column-value"+
				" based parameter '%s' is only defined for 'SELECT' queries",
				refName)
		}
		historicalType, historicalTypeIsKnown := refSites[refName]
		if historicalTypeIsKnown {
			if historicalType != expectedType {
				return validationError(work.Name, "'%s' type expectation conflicts"+
					" at query '%d'", refName, queryIdx+1)
			}
		} else {
			refSites[refName] = expectedType
		}
	}

	return nil
}

// writeParam writes a param into the byte buffer
func writeParam(b *bytes.Buffer, p interface{}) {
	switch p := p.(type) {
	case string:
		b.WriteString(p)
	default:
		b.WriteString(fmt.Sprintf("%v", p))
	}
}

// Query can have placeholders like $1, $2 etc
// resolvePlaceholders replaces placeholders with params
func resolvePlaceholders(query string, params []interface{}) (string, error) {
	var b bytes.Buffer
	inParam := false
	paramIdx := 0
	for _, char := range query {
		if inParam {
			switch char {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			default:
				if paramIdx >= len(params) {
					return "", errExtraPlaceholders
				}
				writeParam(&b, params[paramIdx])
				b.WriteRune(char)
				inParam = false
				paramIdx++
			}
		} else {
			switch char {
			case '$':
				inParam = true
			default:
				b.WriteRune(char)
			}
		}
	}

	if inParam {
		if paramIdx >= len(params) {
			return "", errExtraPlaceholders
		}
		writeParam(&b, params[paramIdx])
		paramIdx++
	}

	if paramIdx != len(params) {
		return "", errExtraParams
	}

	return b.String(), nil
}

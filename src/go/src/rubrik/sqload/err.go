package sqload

import (
	"regexp"
)

//Check panics on error
func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func isErrSimilar(expected *regexp.Regexp, actual error) bool {
	if actual == nil {
		return expected == nil
	}
	// actual != nil
	return expected != nil && expected.FindString(actual.Error()) != ""
}

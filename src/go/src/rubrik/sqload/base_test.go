package sqload

import (
	"flag"
	"testing"
)

func TestParams(t *testing.T) {
	flag.Set("alsologtostderr", "true")
	flag.Set("v", "8")
}

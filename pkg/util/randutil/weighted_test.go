package randutil

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestWeightedChoice(t *testing.T) {
	rand.Seed(0)
	// positive test
	weights := []float64{1, 2, 3}
	choices := make([]int, 10)
	for i := 0; i < len(choices); i++ {
		choices[i] = WeightedChoice(weights)
		if choices[i] != 0 && choices[i] != 1 && choices[i] != 2 {
			t.Error("choice not amongst possibilities")
		}
	}
	expectedChoices := []int{2, 1, 2, 0, 1, 1, 1, 2, 2, 1}
	if !reflect.DeepEqual(choices, expectedChoices) {
		t.Errorf("Expected ouput: %v, have output: %v", choices, expectedChoices)
	}

	// negative test
	weights = []float64{1, 2, -3}
	choice := WeightedChoice(weights)
	if choice != -1 {
		t.Errorf("with negative weights, expected: -1, got: %d", choice)
	}
}

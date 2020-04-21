package internal

import (
	"testing"
)

func Test_bid2path(t *testing.T) {
	_, err := bid2path("_internal~753~B7F6C781-615D-4C57-B63E-69477156E71B")
	if err != nil {
		t.Errorf("%v", err)
	}
}
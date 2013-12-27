package gordon_test

import (
	"testing"

	. "launchpad.net/gocheck"
)

type WSuite struct{}

func Test(t *testing.T) { TestingT(t) }

func init() {
	Suite(&WSuite{})
}

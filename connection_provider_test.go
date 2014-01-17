package gordon_test

import (
	"net"

	. "launchpad.net/gocheck"

	"github.com/vito/gordon"
)

func (w *WSuite) TestConnectionProviderConnectsToNetworkAddr(c *C) {
	l, err := net.Listen("tcp", ":0")
	c.Assert(err, IsNil)

	info := &gordon.ConnectionInfo{
		Network: l.Addr().Network(),
		Addr:    l.Addr().String(),
	}

	conn, err := info.ProvideConnection()
	c.Assert(err, IsNil)
	c.Assert(conn, NotNil)
}

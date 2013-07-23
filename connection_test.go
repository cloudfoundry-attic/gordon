package warden

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	. "launchpad.net/gocheck"
)

func (w *WSuite) TestConnectionCreating(c *C) {
	conn := &fakeConn{
		ReadBuffer: messages(&CreateResponse{
			Handle: proto.String("foohandle"),
		}),

		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Create()
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&CreateRequest{}).Bytes()),
	)

	c.Assert(resp.GetHandle(), Equals, "foohandle")
}

func (w *WSuite) TestConnectionDestroying(c *C) {
	conn := &fakeConn{
		ReadBuffer:  messages(&DestroyResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	_, err := connection.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&DestroyRequest{Handle: proto.String("foo")}).Bytes()),
	)
}

func (w *WSuite) TestConnectionSpawn(c *C) {
	conn := &fakeConn{
		ReadBuffer:  messages(&SpawnResponse{JobId: proto.Uint32(42)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Spawn("foo-handle", "echo hi")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&SpawnRequest{
			Handle: proto.String("foo-handle"),
			Script: proto.String("echo hi"),
		}).Bytes()),
	)

	c.Assert(resp.GetJobId(), Equals, uint32(42))
}

func (w *WSuite) TestConnectionNetIn(c *C) {
	conn := &fakeConn{
		ReadBuffer: messages(
			&NetInResponse{
				HostPort:      proto.Uint32(7331),
				ContainerPort: proto.Uint32(7331),
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.NetIn("foo-handle")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&NetInRequest{
			Handle: proto.String("foo-handle"),
		}).Bytes()),
	)

	c.Assert(resp.GetHostPort(), Equals, uint32(7331))
	c.Assert(resp.GetContainerPort(), Equals, uint32(7331))
}

func (w *WSuite) TestConnectionCopyIn(c *C) {
	conn := &fakeConn{
		ReadBuffer:  messages(&CopyInResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	_, err := connection.CopyIn("foo-handle", "/foo", "/bar")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&CopyInRequest{
			Handle:  proto.String("foo-handle"),
			SrcPath: proto.String("/foo"),
			DstPath: proto.String("/bar"),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionRun(c *C) {
	conn := &fakeConn{
		ReadBuffer:  messages(&RunResponse{ExitStatus: proto.Uint32(137)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&RunRequest{
			Handle: proto.String("foo-handle"),
			Script: proto.String("echo hi"),
		}).Bytes()),
	)

	c.Assert(resp.GetExitStatus(), Equals, uint32(137))
}

func (w *WSuite) TestConnectionStream(c *C) {
	conn := &fakeConn{
		ReadBuffer: messages(
			&StreamResponse{Name: proto.String("stdout"), Data: proto.String("1")},
			&StreamResponse{Name: proto.String("stderr"), Data: proto.String("2")},
			&StreamResponse{ExitStatus: proto.Uint32(3)},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Stream("foo-handle", 42)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(messages(&StreamRequest{
			Handle: proto.String("foo-handle"),
			JobId:  proto.Uint32(42),
		}).Bytes()),
	)

	res1 := <-resp
	c.Assert(res1.GetName(), Equals, "stdout")
	c.Assert(res1.GetData(), Equals, "1")

	res2 := <-resp
	c.Assert(res2.GetName(), Equals, "stderr")
	c.Assert(res2.GetData(), Equals, "2")

	res3, ok := <-resp
	c.Assert(res3.GetExitStatus(), Equals, uint32(3))
	c.Assert(ok, Equals, true)
}

func (w *WSuite) TestConnectionError(c *C) {
	conn := &fakeConn{
		ReadBuffer:  messages(&ErrorResponse{Message: proto.String("boo")}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(resp, IsNil)
	c.Assert(err, Not(IsNil))

	c.Assert(err.Error(), Equals, "boo")
}

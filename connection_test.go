package warden

import (
	"bytes"
	"math"
	"time"

	"code.google.com/p/goprotobuf/proto"
	. "launchpad.net/gocheck"

	protocol "github.com/vito/gordon/protocol"
)

func (w *WSuite) TestConnectionCreating(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(&protocol.CreateResponse{
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
		string(protocol.Messages(&protocol.CreateRequest{}).Bytes()),
	)

	c.Assert(resp.GetHandle(), Equals, "foohandle")
}

func (w *WSuite) TestConnectionDestroying(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.DestroyResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	_, err := connection.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.DestroyRequest{Handle: proto.String("foo")}).Bytes()),
	)
}

func (w *WSuite) TestMemoryLimiting(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	res, err := connection.LimitMemory("foo", 42)
	c.Assert(err, IsNil)

	c.Assert(res.GetLimitInBytes(), Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.LimitMemoryRequest{
					Handle:       proto.String("foo"),
					LimitInBytes: proto.Uint64(42),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingMemoryLimit(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	memoryLimit, err := connection.GetMemoryLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(memoryLimit, Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.LimitMemoryRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingMemoryLimitThatLooksFishy(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.LimitMemoryResponse{LimitInBytes: proto.Uint64(math.MaxInt64)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	memoryLimit, err := connection.GetMemoryLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(memoryLimit, Equals, uint64(0))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.LimitMemoryRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestDiskLimiting(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.LimitDiskResponse{ByteLimit: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	res, err := connection.LimitDisk("foo", 42)
	c.Assert(err, IsNil)

	c.Assert(res.GetByteLimit(), Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.LimitDiskRequest{
					Handle:    proto.String("foo"),
					ByteLimit: proto.Uint64(42),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingDiskLimit(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.LimitDiskResponse{ByteLimit: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	diskLimit, err := connection.GetDiskLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(diskLimit, Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.LimitDiskRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestConnectionSpawn(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(
			&protocol.SpawnResponse{JobId: proto.Uint32(42)},
			&protocol.SpawnResponse{JobId: proto.Uint32(43)},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Spawn("foo-handle", "echo hi", true)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.SpawnRequest{
			Handle:        proto.String("foo-handle"),
			Script:        proto.String("echo hi"),
			DiscardOutput: proto.Bool(true),
		}).Bytes()),
	)

	c.Assert(resp.GetJobId(), Equals, uint32(42))

	conn.WriteBuffer.Reset()

	resp, err = connection.Spawn("foo-handle", "echo hi", false)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.SpawnRequest{
			Handle:        proto.String("foo-handle"),
			Script:        proto.String("echo hi"),
			DiscardOutput: proto.Bool(false),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionNetIn(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(
			&protocol.NetInResponse{
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
		string(protocol.Messages(&protocol.NetInRequest{
			Handle: proto.String("foo-handle"),
		}).Bytes()),
	)

	c.Assert(resp.GetHostPort(), Equals, uint32(7331))
	c.Assert(resp.GetContainerPort(), Equals, uint32(7331))
}

func (w *WSuite) TestConnectionList(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(
			&protocol.ListResponse{
				Handles: []string{"container1", "container2", "container3"},
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.List()
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.ListRequest{}).Bytes()),
	)

	c.Assert(resp.GetHandles(), DeepEquals, []string{"container1", "container2", "container3"})
}

func (w *WSuite) TestConnectionInfo(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(
			&protocol.InfoResponse{
				State: proto.String("active"),
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Info("handle")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.InfoRequest{
			Handle: proto.String("handle"),
		}).Bytes()),
	)

	c.Assert(resp.GetState(), Equals, "active")
}

func (w *WSuite) TestConnectionCopyIn(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.CopyInResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	_, err := connection.CopyIn("foo-handle", "/foo", "/bar")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.CopyInRequest{
			Handle:  proto.String("foo-handle"),
			SrcPath: proto.String("/foo"),
			DstPath: proto.String("/bar"),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionRun(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.RunResponse{ExitStatus: proto.Uint32(137)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.RunRequest{
			Handle: proto.String("foo-handle"),
			Script: proto.String("echo hi"),
		}).Bytes()),
	)

	c.Assert(resp.GetExitStatus(), Equals, uint32(137))
}

func (w *WSuite) TestConnectionStream(c *C) {
	conn := &fakeConn{
		ReadBuffer: protocol.Messages(
			&protocol.StreamResponse{Name: proto.String("stdout"), Data: proto.String("1")},
			&protocol.StreamResponse{Name: proto.String("stderr"), Data: proto.String("2")},
			&protocol.StreamResponse{ExitStatus: proto.Uint32(3)},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, done, err := connection.Stream("foo-handle", 42)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(protocol.Messages(&protocol.StreamRequest{
			Handle: proto.String("foo-handle"),
			JobId:  proto.Uint32(42),
		}).Bytes()),
	)

	res1 := <-resp
	c.Assert(res1.GetName(), Equals, "stdout")
	c.Assert(res1.GetData(), Equals, "1")

	select {
	case <-done:
		c.Error("done channel should not have been readable")
	default:
	}

	res2 := <-resp
	c.Assert(res2.GetName(), Equals, "stderr")
	c.Assert(res2.GetData(), Equals, "2")

	select {
	case <-done:
		c.Error("done channel should not have been readable")
	default:
	}

	res3, ok := <-resp
	c.Assert(res3.GetExitStatus(), Equals, uint32(3))
	c.Assert(ok, Equals, true)

	select {
	case _, ok := <-done:
		c.Assert(ok, Equals, false)
	case <-time.After(1 * time.Second):
		c.Error("done channel should have closed")
	}
}

func (w *WSuite) TestConnectionError(c *C) {
	conn := &fakeConn{
		ReadBuffer:  protocol.Messages(&protocol.ErrorResponse{Message: proto.String("boo")}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := NewConnection(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(resp, IsNil)
	c.Assert(err, Not(IsNil))

	c.Assert(err.Error(), Equals, "boo")
}

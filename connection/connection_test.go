package connection_test

import (
	"bytes"
	"math"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	. "launchpad.net/gocheck"

	"github.com/vito/gordon/connection"
	. "github.com/vito/gordon/test_helpers"
	"github.com/vito/gordon/warden"
)

func (w *WSuite) TestConnectionCreating(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(&warden.CreateResponse{
			Handle: proto.String("foohandle"),
		}),

		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Create()
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.CreateRequest{}).Bytes()),
	)

	c.Assert(resp.GetHandle(), Equals, "foohandle")
}

func (w *WSuite) TestConnectionStopping(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.StopResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	_, err := connection.Stop("foo", true, true)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.StopRequest{
			Handle:     proto.String("foo"),
			Background: proto.Bool(true),
			Kill:       proto.Bool(true),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionDestroying(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.DestroyResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	_, err := connection.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.DestroyRequest{Handle: proto.String("foo")}).Bytes()),
	)
}

func (w *WSuite) TestMemoryLimiting(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	res, err := connection.LimitMemory("foo", 42)
	c.Assert(err, IsNil)

	c.Assert(res.GetLimitInBytes(), Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.LimitMemoryRequest{
					Handle:       proto.String("foo"),
					LimitInBytes: proto.Uint64(42),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingMemoryLimit(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	memoryLimit, err := connection.GetMemoryLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(memoryLimit, Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.LimitMemoryRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingMemoryLimitThatLooksFishy(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.LimitMemoryResponse{LimitInBytes: proto.Uint64(math.MaxInt64)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	memoryLimit, err := connection.GetMemoryLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(memoryLimit, Equals, uint64(0))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.LimitMemoryRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestDiskLimiting(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.LimitDiskResponse{ByteLimit: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	res, err := connection.LimitDisk("foo", 42)
	c.Assert(err, IsNil)

	c.Assert(res.GetByteLimit(), Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.LimitDiskRequest{
					Handle:    proto.String("foo"),
					ByteLimit: proto.Uint64(42),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestGettingDiskLimit(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.LimitDiskResponse{ByteLimit: proto.Uint64(40)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	diskLimit, err := connection.GetDiskLimit("foo")
	c.Assert(err, IsNil)
	c.Assert(diskLimit, Equals, uint64(40))

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.LimitDiskRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestConnectionSpawn(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.SpawnResponse{JobId: proto.Uint32(42)},
			&warden.SpawnResponse{JobId: proto.Uint32(43)},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Spawn("foo-handle", "echo hi", true)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.SpawnRequest{
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
		string(warden.Messages(&warden.SpawnRequest{
			Handle:        proto.String("foo-handle"),
			Script:        proto.String("echo hi"),
			DiscardOutput: proto.Bool(false),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionNetIn(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.NetInResponse{
				HostPort:      proto.Uint32(7331),
				ContainerPort: proto.Uint32(7331),
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.NetIn("foo-handle")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.NetInRequest{
			Handle: proto.String("foo-handle"),
		}).Bytes()),
	)

	c.Assert(resp.GetHostPort(), Equals, uint32(7331))
	c.Assert(resp.GetContainerPort(), Equals, uint32(7331))
}

func (w *WSuite) TestConnectionList(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.ListResponse{
				Handles: []string{"container1", "container2", "container3"},
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.List()
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.ListRequest{}).Bytes()),
	)

	c.Assert(resp.GetHandles(), DeepEquals, []string{"container1", "container2", "container3"})
}

func (w *WSuite) TestConnectionInfo(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.InfoResponse{
				State: proto.String("active"),
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Info("handle")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.InfoRequest{
			Handle: proto.String("handle"),
		}).Bytes()),
	)

	c.Assert(resp.GetState(), Equals, "active")
}

func (w *WSuite) TestConnectionCopyIn(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.CopyInResponse{}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	_, err := connection.CopyIn("foo-handle", "/foo", "/bar")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.CopyInRequest{
			Handle:  proto.String("foo-handle"),
			SrcPath: proto.String("/foo"),
			DstPath: proto.String("/bar"),
		}).Bytes()),
	)
}

func (w *WSuite) TestConnectionLink(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(&warden.LinkResponse{
			Stdout:     proto.String("some data for stdout"),
			Stderr:     proto.String("some data for stderr"),
			ExitStatus: proto.Uint32(137),
		}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Link("foo-handle", 42)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.LinkRequest{
			Handle: proto.String("foo-handle"),
			JobId:  proto.Uint32(42),
		}).Bytes()),
	)

	c.Assert(resp.GetExitStatus(), Equals, uint32(137))
	c.Assert(resp.GetStdout(), Equals, "some data for stdout")
	c.Assert(resp.GetStderr(), Equals, "some data for stderr")
}

func (w *WSuite) TestConnectionRun(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.RunResponse{ExitStatus: proto.Uint32(137)}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.RunRequest{
			Handle: proto.String("foo-handle"),
			Script: proto.String("echo hi"),
		}).Bytes()),
	)

	c.Assert(resp.GetExitStatus(), Equals, uint32(137))
}

func (w *WSuite) TestConnectionStream(c *C) {
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.StreamResponse{Name: proto.String("stdout"), Data: proto.String("1")},
			&warden.StreamResponse{Name: proto.String("stderr"), Data: proto.String("2")},
			&warden.StreamResponse{ExitStatus: proto.Uint32(3)},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, done, err := connection.Stream("foo-handle", 42)
	c.Assert(err, IsNil)

	c.Assert(
		string(conn.WriteBuffer.Bytes()),
		Equals,
		string(warden.Messages(&warden.StreamRequest{
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
	conn := &FakeConn{
		ReadBuffer: warden.Messages(
			&warden.DestroyResponse{},
			// EOF
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Destroy("foo-handle")
	c.Assert(resp, Not(IsNil))
	c.Assert(err, IsNil)

	select {
	case <-connection.Disconnected:
	case <-time.After(1 * time.Second):
		c.Error("should have disconnected due to EOF")
	}
}

func (w *WSuite) TestConnectionDisconnects(c *C) {
	conn := &FakeConn{
		ReadBuffer:  warden.Messages(&warden.ErrorResponse{Message: proto.String("boo")}),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	connection := connection.New(conn)

	resp, err := connection.Run("foo-handle", "echo hi")
	c.Assert(resp, IsNil)
	c.Assert(err, Not(IsNil))

	c.Assert(err.Error(), Equals, "boo")
}

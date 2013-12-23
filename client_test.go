package warden

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"errors"
	. "launchpad.net/gocheck"
	"runtime"
)

func (w *WSuite) TestClientConnectWithFailingProvider(c *C) {
	client := NewClient(&FailingConnectionProvider{})
	err := client.Connect()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "nope!")
}

func (w *WSuite) TestClientConnectWithSuccessfulProvider(c *C) {
	client := NewClient(&FakeConnectionProvider{})
	err := client.Connect()
	c.Assert(err, IsNil)
}

func (w *WSuite) TestClientContainerLifecycle(c *C) {
	fcp := &FakeConnectionProvider{
		ReadBuffer: messages(
			&CreateResponse{Handle: proto.String("foo")},
			&DestroyResponse{},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Create()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandle(), Equals, "foo")

	_, err = client.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(fcp.WriteBuffer.Bytes()),
		Equals,
		string(
			messages(
				&CreateRequest{},
				&DestroyRequest{Handle: proto.String("foo")},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientSpawnAndStreaming(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ReadBuffers: []*bytes.Buffer{
			messages(
				&SpawnResponse{
					JobId: proto.Uint32(42),
				},
			),
			messages(
				&StreamResponse{
					Name: proto.String("stdout"),
					Data: proto.String("some data for stdout"),
				},
			),
		},
		WriteBuffers: []*bytes.Buffer{
			firstWriteBuf,
			secondWriteBuf,
		},
	}

	client := NewClient(mcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	spawned, err := client.Spawn("foo", "echo some data for stdout", true)
	c.Assert(err, IsNil)

	responses, err := client.Stream("foo", spawned.GetJobId())
	c.Assert(err, IsNil)

	c.Assert(
		string(firstWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&SpawnRequest{
					Handle:        proto.String("foo"),
					Script:        proto.String("echo some data for stdout"),
					DiscardOutput: proto.Bool(true),
				},
			).Bytes(),
		),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&StreamRequest{Handle: proto.String("foo"), JobId: proto.Uint32(42)},
			).Bytes(),
		),
	)

	res := <-responses
	c.Assert(res.GetName(), Equals, "stdout")
	c.Assert(res.GetData(), Equals, "some data for stdout")
}

func (w *WSuite) TestClientRunningAndDestroying(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ReadBuffers: []*bytes.Buffer{
			messages(
				&DestroyResponse{},
			),
			messages(
				&RunResponse{
					ExitStatus: proto.Uint32(255),
				},
			),
		},
		WriteBuffers: []*bytes.Buffer{
			firstWriteBuf,
			secondWriteBuf,
		},
	}

	client := NewClient(mcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	ran, err := client.Run("foo", "echo hi")
	c.Assert(err, IsNil)

	_, err = client.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(ran.GetExitStatus(), Equals, uint32(255))

	c.Assert(
		string(firstWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&DestroyRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&RunRequest{
					Handle: proto.String("foo"),
					Script: proto.String("echo hi"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientContainerInfo(c *C) {
	fcp := &FakeConnectionProvider{
		ReadBuffer: messages(
			&InfoResponse{
				State: proto.String("stopped"),
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Info("handle")
	c.Assert(err, IsNil)
	c.Assert(res.GetState(), Equals, "stopped")

	c.Assert(
		string(fcp.WriteBuffer.Bytes()),
		Equals,
		string(
			messages(
				&InfoRequest{
					Handle: proto.String("handle"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientContainerList(c *C) {
	fcp := &FakeConnectionProvider{
		ReadBuffer: messages(
			&ListResponse{
				Handles: []string{"container1", "container6"},
			},
		),
		WriteBuffer: bytes.NewBuffer([]byte{}),
	}

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.List()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandles(), DeepEquals, []string{"container1", "container6"})

	c.Assert(
		string(fcp.WriteBuffer.Bytes()),
		Equals,
		string(
			messages(
				&ListRequest{},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientCopyingInAndDestroying(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ReadBuffers: []*bytes.Buffer{
			messages(&DestroyResponse{}),
			messages(&CopyInResponse{}),
		},
		WriteBuffers: []*bytes.Buffer{
			firstWriteBuf,
			secondWriteBuf,
		},
	}

	client := NewClient(mcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	_, err = client.CopyIn("foo", "/foo", "/bar")
	c.Assert(err, IsNil)

	_, err = client.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(firstWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&DestroyRequest{
					Handle: proto.String("foo"),
				},
			).Bytes(),
		),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&CopyInRequest{
					Handle:  proto.String("foo"),
					SrcPath: proto.String("/foo"),
					DstPath: proto.String("/bar"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientReconnects(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ReadBuffers: []*bytes.Buffer{
			messages(
				&CreateResponse{Handle: proto.String("handle a")},
				// no response for Create #2
			),
			messages(
				&DestroyResponse{},
				&DestroyResponse{},
			),
		},
		WriteBuffers: []*bytes.Buffer{
			firstWriteBuf,
			secondWriteBuf,
		},
	}

	client := NewClient(mcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	c1, err := client.Create()
	c.Assert(err, IsNil)

	_, err = client.Create()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "EOF")

	// let the client notice its connection was dropped
	runtime.Gosched()

	_, err = client.Destroy(c1.GetHandle())
	c.Assert(err, IsNil)

	c.Assert(
		string(firstWriteBuf.Bytes()),
		Equals,
		string(messages(&CreateRequest{}, &CreateRequest{}).Bytes()),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			messages(
				&DestroyRequest{
					Handle: proto.String("handle a"),
				},
			).Bytes(),
		),
	)
}

type FailingConnectionProvider struct{}

func (c *FailingConnectionProvider) ProvideConnection() (*Connection, error) {
	return nil, errors.New("nope!")
}

type FakeConnectionProvider struct {
	ReadBuffer  *bytes.Buffer
	WriteBuffer *bytes.Buffer
}

func (c *FakeConnectionProvider) ProvideConnection() (*Connection, error) {
	return NewConnection(
		&fakeConn{
			ReadBuffer:  c.ReadBuffer,
			WriteBuffer: c.WriteBuffer,
		},
	), nil
}

type ManyConnectionProvider struct {
	ReadBuffers  []*bytes.Buffer
	WriteBuffers []*bytes.Buffer
}

func (c *ManyConnectionProvider) ProvideConnection() (*Connection, error) {
	if len(c.ReadBuffers) == 0 {
		return nil, errors.New("no more connections")
	}

	rbuf := c.ReadBuffers[0]
	c.ReadBuffers = c.ReadBuffers[1:]

	wbuf := c.WriteBuffers[0]
	c.WriteBuffers = c.WriteBuffers[1:]

	return NewConnection(
		&fakeConn{
			ReadBuffer:  rbuf,
			WriteBuffer: wbuf,
		},
	), nil
}

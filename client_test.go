package warden

import (
	"bytes"
	"errors"
	"runtime"

	"code.google.com/p/goprotobuf/proto"
	. "launchpad.net/gocheck"

	protocol "github.com/vito/gordon/protocol"
)

func (w *WSuite) TestClientConnectWithFailingProvider(c *C) {
	client := NewClient(&FailingConnectionProvider{})
	err := client.Connect()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "nope!")
}

func (w *WSuite) TestClientConnectWithSuccessfulProvider(c *C) {
	client := NewClient(NewFakeConnectionProvider(new(bytes.Buffer), new(bytes.Buffer)))
	err := client.Connect()
	c.Assert(err, IsNil)
}

func (w *WSuite) TestClientContainerLifecycle(c *C) {
	writeBuffer := new(bytes.Buffer)

	fcp := NewFakeConnectionProvider(
		protocol.Messages(
			&protocol.CreateResponse{Handle: proto.String("foo")},
			&protocol.DestroyResponse{},
		),
		writeBuffer,
	)

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Create()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandle(), Equals, "foo")

	_, err = client.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.CreateRequest{},
				&protocol.DestroyRequest{Handle: proto.String("foo")},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientSpawnAndStreaming(c *C) {
	writeBuf := new(bytes.Buffer)

	client := NewClient(NewFakeConnectionProvider(
		protocol.Messages(
			&protocol.SpawnResponse{
				JobId: proto.Uint32(42),
			},
			&protocol.StreamResponse{
				Name: proto.String("stdout"),
				Data: proto.String("some data for stdout"),
			},
		),
		writeBuf,
	))

	err := client.Connect()
	c.Assert(err, IsNil)

	spawned, err := client.Spawn("foo", "echo some data for stdout", true)
	c.Assert(err, IsNil)

	responses, err := client.Stream("foo", spawned.GetJobId())
	c.Assert(err, IsNil)

	c.Assert(
		string(writeBuf.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.SpawnRequest{
					Handle:        proto.String("foo"),
					Script:        proto.String("echo some data for stdout"),
					DiscardOutput: proto.Bool(true),
				},
				&protocol.StreamRequest{Handle: proto.String("foo"), JobId: proto.Uint32(42)},
			).Bytes(),
		),
	)

	res := <-responses
	c.Assert(res.GetName(), Equals, "stdout")
	c.Assert(res.GetData(), Equals, "some data for stdout")
}

func (w *WSuite) TestClientContainerInfo(c *C) {
	writeBuffer := new(bytes.Buffer)

	fcp := NewFakeConnectionProvider(
		protocol.Messages(
			&protocol.InfoResponse{
				State: proto.String("stopped"),
			},
		),
		writeBuffer,
	)

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Info("handle")
	c.Assert(err, IsNil)
	c.Assert(res.GetState(), Equals, "stopped")

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.InfoRequest{
					Handle: proto.String("handle"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientContainerList(c *C) {
	writeBuffer := new(bytes.Buffer)

	fcp := NewFakeConnectionProvider(
		protocol.Messages(
			&protocol.ListResponse{
				Handles: []string{"container1", "container6"},
			},
		),
		writeBuffer,
	)

	client := NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.List()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandles(), DeepEquals, []string{"container1", "container6"})

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.ListRequest{},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientReconnects(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ConnectionProviders: []ConnectionProvider{
			NewFakeConnectionProvider(
				protocol.Messages(
					&protocol.CreateResponse{Handle: proto.String("handle a")},
					// no response for Create #2
				),
				firstWriteBuf,
			),
			NewFakeConnectionProvider(
				protocol.Messages(
					&protocol.DestroyResponse{},
					&protocol.DestroyResponse{},
				),
				secondWriteBuf,
			),
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
		string(protocol.Messages(&protocol.CreateRequest{}, &protocol.CreateRequest{}).Bytes()),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			protocol.Messages(
				&protocol.DestroyRequest{
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
	connection *Connection
}

func NewFakeConnectionProvider(readBuffer, writeBuffer *bytes.Buffer) *FakeConnectionProvider {
	return &FakeConnectionProvider{
		connection: NewConnection(
			&fakeConn{
				ReadBuffer:  readBuffer,
				WriteBuffer: writeBuffer,
			},
		),
	}
}

func (c *FakeConnectionProvider) ProvideConnection() (*Connection, error) {
	return c.connection, nil
}

type ManyConnectionProvider struct {
	ConnectionProviders []ConnectionProvider
}

func (c *ManyConnectionProvider) ProvideConnection() (*Connection, error) {
	if len(c.ConnectionProviders) == 0 {
		return nil, errors.New("no more connections")
	}

	cp := c.ConnectionProviders[0]
	c.ConnectionProviders = c.ConnectionProviders[1:]

	return cp.ProvideConnection()
}

package gordon_test

import (
	"bytes"
	"errors"
	"runtime"

	"code.google.com/p/goprotobuf/proto"
	. "launchpad.net/gocheck"

	"github.com/vito/gordon"
	. "github.com/vito/gordon/test_helpers"
	"github.com/vito/gordon/connection"
	"github.com/vito/gordon/warden"
)

func (w *WSuite) TestClientConnectWithFailingProvider(c *C) {
	client := gordon.NewClient(&FailingConnectionProvider{})
	err := client.Connect()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "nope!")
}

func (w *WSuite) TestClientConnectWithSuccessfulProvider(c *C) {
	client := gordon.NewClient(NewFakeConnectionProvider(new(bytes.Buffer), new(bytes.Buffer)))
	err := client.Connect()
	c.Assert(err, IsNil)
}

func (w *WSuite) TestClientContainerLifecycle(c *C) {
	writeBuffer := new(bytes.Buffer)

	fcp := NewFakeConnectionProvider(
		warden.Messages(
			&warden.CreateResponse{Handle: proto.String("foo")},
			&warden.StopResponse{},
			&warden.DestroyResponse{},
		),
		writeBuffer,
	)

	client := gordon.NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Create()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandle(), Equals, "foo")

	_, err = client.Stop("foo", true, true)
	c.Assert(err, IsNil)

	_, err = client.Destroy("foo")
	c.Assert(err, IsNil)

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.CreateRequest{},
				&warden.StopRequest{
					Handle:     proto.String("foo"),
					Background: proto.Bool(true),
					Kill:       proto.Bool(true),
				},
				&warden.DestroyRequest{Handle: proto.String("foo")},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientSpawnAndStreaming(c *C) {
	writeBuf := new(bytes.Buffer)

	client := gordon.NewClient(NewFakeConnectionProvider(
		warden.Messages(
			&warden.SpawnResponse{
				JobId: proto.Uint32(42),
			},
			&warden.StreamResponse{
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
			warden.Messages(
				&warden.SpawnRequest{
					Handle:        proto.String("foo"),
					Script:        proto.String("echo some data for stdout"),
					DiscardOutput: proto.Bool(true),
				},
				&warden.StreamRequest{Handle: proto.String("foo"), JobId: proto.Uint32(42)},
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
		warden.Messages(
			&warden.InfoResponse{
				State: proto.String("stopped"),
			},
		),
		writeBuffer,
	)

	client := gordon.NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.Info("handle")
	c.Assert(err, IsNil)
	c.Assert(res.GetState(), Equals, "stopped")

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.InfoRequest{
					Handle: proto.String("handle"),
				},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientContainerList(c *C) {
	writeBuffer := new(bytes.Buffer)

	fcp := NewFakeConnectionProvider(
		warden.Messages(
			&warden.ListResponse{
				Handles: []string{"container1", "container6"},
			},
		),
		writeBuffer,
	)

	client := gordon.NewClient(fcp)

	err := client.Connect()
	c.Assert(err, IsNil)

	res, err := client.List()
	c.Assert(err, IsNil)
	c.Assert(res.GetHandles(), DeepEquals, []string{"container1", "container6"})

	c.Assert(
		string(writeBuffer.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.ListRequest{},
			).Bytes(),
		),
	)
}

func (w *WSuite) TestClientReconnects(c *C) {
	firstWriteBuf := bytes.NewBuffer([]byte{})
	secondWriteBuf := bytes.NewBuffer([]byte{})

	mcp := &ManyConnectionProvider{
		ConnectionProviders: []gordon.ConnectionProvider{
			NewFakeConnectionProvider(
				warden.Messages(
					&warden.CreateResponse{Handle: proto.String("handle a")},
					// no response for Create #2
				),
				firstWriteBuf,
			),
			NewFakeConnectionProvider(
				warden.Messages(
					&warden.DestroyResponse{},
					&warden.DestroyResponse{},
				),
				secondWriteBuf,
			),
		},
	}

	client := gordon.NewClient(mcp)

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
		string(warden.Messages(&warden.CreateRequest{}, &warden.CreateRequest{}).Bytes()),
	)

	c.Assert(
		string(secondWriteBuf.Bytes()),
		Equals,
		string(
			warden.Messages(
				&warden.DestroyRequest{
					Handle: proto.String("handle a"),
				},
			).Bytes(),
		),
	)
}

type FailingConnectionProvider struct{}

func (c *FailingConnectionProvider) ProvideConnection() (*connection.Connection, error) {
	return nil, errors.New("nope!")
}

type FakeConnectionProvider struct {
	connection *connection.Connection
}

func NewFakeConnectionProvider(readBuffer, writeBuffer *bytes.Buffer) *FakeConnectionProvider {
	return &FakeConnectionProvider{
		connection: connection.New(
			&FakeConn{
				ReadBuffer:  readBuffer,
				WriteBuffer: writeBuffer,
			},
		),
	}
}

func (c *FakeConnectionProvider) ProvideConnection() (*connection.Connection, error) {
	return c.connection, nil
}

type ManyConnectionProvider struct {
	ConnectionProviders []gordon.ConnectionProvider
}

func (c *ManyConnectionProvider) ProvideConnection() (*connection.Connection, error) {
	if len(c.ConnectionProviders) == 0 {
		return nil, errors.New("no more connections")
	}

	cp := c.ConnectionProviders[0]
	c.ConnectionProviders = c.ConnectionProviders[1:]

	return cp.ProvideConnection()
}

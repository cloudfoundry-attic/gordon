package warden

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

type Connection struct {
	conn      net.Conn
	read      *bufio.Reader
	writeLock sync.Mutex

	disconnected chan bool
}

type WardenError struct {
	Message   string
	Data      string
	Backtrace []string
}

func (e *WardenError) Error() string {
	return e.Message
}

func Connect(socket_path string) (*Connection, error) {
	conn, err := net.Dial("unix", socket_path)
	if err != nil {
		return nil, err
	}

	return NewConnection(conn), nil
}

func NewConnection(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
		read: bufio.NewReader(conn),

		// buffer size of 1 so that read and write errors
		// can both send without blocking
		disconnected: make(chan bool, 1),
	}
}

func (c *Connection) Close() {
	c.conn.Close()
}

func (c *Connection) Create() (*CreateResponse, error) {
	res, err := c.roundTrip(&CreateRequest{}, &CreateResponse{})
	if err != nil {
		return nil, err
	}

	return res.(*CreateResponse), nil
}

func (c *Connection) Destroy(handle string) (*DestroyResponse, error) {
	res, err := c.roundTrip(
		&DestroyRequest{Handle: proto.String(handle)},
		&DestroyResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*DestroyResponse), nil
}

func (c *Connection) Spawn(handle, script string) (*SpawnResponse, error) {
	res, err := c.roundTrip(
		&SpawnRequest{
			Handle: proto.String(handle),
			Script: proto.String(script),
		},
		&SpawnResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*SpawnResponse), nil
}

func (c *Connection) Run(handle, script string) (*RunResponse, error) {
	res, err := c.roundTrip(
		&RunRequest{
			Handle: proto.String(handle),
			Script: proto.String(script),
		},
		&RunResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*RunResponse), nil
}

func (c *Connection) Stream(handle string, jobId uint32) (chan *StreamResponse, error) {
	err := c.sendMessage(
		&StreamRequest{
			Handle: proto.String(handle),
			JobId:  proto.Uint32(jobId),
		},
	)

	if err != nil {
		return nil, err
	}

	responses := make(chan *StreamResponse)

	go func() {
		for {
			resMsg, err := c.readResponse(&StreamResponse{})
			if err != nil {
				close(responses)
				break
			}

			response := resMsg.(*StreamResponse)

			responses <- response

			if response.ExitStatus != nil {
				close(responses)
				break
			}
		}
	}()

	return responses, nil
}

func (c *Connection) NetIn(handle string) (*NetInResponse, error) {
	res, err := c.roundTrip(
		&NetInRequest{Handle: proto.String(handle)},
		&NetInResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*NetInResponse), nil
}

func (c *Connection) LimitMemory(handle string, limit uint64) (*LimitMemoryResponse, error) {
	res, err := c.roundTrip(
		&LimitMemoryRequest{
			Handle:       proto.String(handle),
			LimitInBytes: proto.Uint64(limit),
		},
		&LimitMemoryResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*LimitMemoryResponse), nil
}

func (c *Connection) LimitDisk(handle string, limit uint64) (*LimitDiskResponse, error) {
	res, err := c.roundTrip(
		&LimitDiskRequest{
			Handle:    proto.String(handle),
			ByteLimit: proto.Uint64(limit),
		},
		&LimitDiskResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*LimitDiskResponse), nil
}

func (c *Connection) CopyIn(handle, src, dst string) (*CopyInResponse, error) {
	res, err := c.roundTrip(
		&CopyInRequest{
			Handle:  proto.String(handle),
			SrcPath: proto.String(src),
			DstPath: proto.String(dst),
		},
		&CopyInResponse{},
	)

	if err != nil {
		return nil, err
	}

	return res.(*CopyInResponse), nil
}

func (c *Connection) List() (*ListResponse, error) {
	res, err := c.roundTrip(&ListRequest{}, &ListResponse{})
	if err != nil {
		return nil, err
	}

	return res.(*ListResponse), nil
}

func (c *Connection) Info(handle string) (*InfoResponse, error) {
	res, err := c.roundTrip(&InfoRequest{
		Handle: proto.String(handle),
	}, &InfoResponse{})
	if err != nil {
		return nil, err
	}

	return res.(*InfoResponse), nil
}

func (c *Connection) roundTrip(request proto.Message, response proto.Message) (proto.Message, error) {
	err := c.sendMessage(request)
	if err != nil {
		return nil, err
	}

	resp, err := c.readResponse(response)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Connection) sendMessage(req proto.Message) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	request, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	msg := &Message{
		Type:    Message_Type(message2type(req)).Enum(),
		Payload: request,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(
		[]byte(
			fmt.Sprintf(
				"%d\r\n%s\r\n",
				len(data),
				data,
			),
		),
	)

	if err != nil {
		c.disconnected <- true
		return err
	}

	return nil
}

func (c *Connection) readResponse(response proto.Message) (proto.Message, error) {
	payload, err := c.readPayload()
	if err != nil {
		c.disconnected <- true
		return nil, err
	}

	message := &Message{}
	err = proto.Unmarshal(payload, message)
	if err != nil {
		return nil, err
	}

	// error response from server
	if message.GetType() == Message_Type(1) {
		errorResponse := &ErrorResponse{}
		err = proto.Unmarshal(message.Payload, errorResponse)
		if err != nil {
			return nil, errors.New("error unmarshalling error!")
		}

		return nil, &WardenError{
			Message:   errorResponse.GetMessage(),
			Data:      errorResponse.GetData(),
			Backtrace: errorResponse.GetBacktrace(),
		}
	}

	response_type := Message_Type(message2type(response))
	if message.GetType() != response_type {
		return nil, errors.New(
			fmt.Sprintf(
				"expected message type %s, got %s\n",
				response_type.String(),
				message.GetType().String(),
			),
		)
	}

	err = proto.Unmarshal(message.GetPayload(), response)
	return response, err
}

func (c *Connection) readPayload() ([]byte, error) {
	msgHeader, err := c.read.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	msgLen, err := strconv.ParseUint(string(msgHeader[0:len(msgHeader)-2]), 10, 0)
	if err != nil {
		return nil, err
	}

	payload, err := readNBytes(int(msgLen), c.read)
	if err != nil {
		return nil, err
	}

	_, err = readNBytes(2, c.read) // CRLN
	if err != nil {
		return nil, err
	}

	return payload, err
}

func message2type(msg proto.Message) int32 {
	switch msg.(type) {
	case *ErrorResponse:
		return 1

	case *CreateRequest, *CreateResponse:
		return 11
	case *StopRequest, *StopResponse:
		return 12
	case *DestroyRequest, *DestroyResponse:
		return 13
	case *InfoRequest, *InfoResponse:
		return 14

	case *SpawnRequest, *SpawnResponse:
		return 21
	case *LinkRequest, *LinkResponse:
		return 22
	case *RunRequest, *RunResponse:
		return 23
	case *StreamRequest, *StreamResponse:
		return 24

	case *NetInRequest, *NetInResponse:
		return 31
	case *NetOutRequest, *NetOutResponse:
		return 32

	case *CopyInRequest, *CopyInResponse:
		return 41
	case *CopyOutRequest, *CopyOutResponse:
		return 42

	case *LimitMemoryRequest, *LimitMemoryResponse:
		return 51
	case *LimitDiskRequest, *LimitDiskResponse:
		return 52
	case *LimitBandwidthRequest, *LimitBandwidthResponse:
		return 53

	case *PingRequest, *PingResponse:
		return 91
	case *ListRequest, *ListResponse:
		return 92
	case *EchoRequest, *EchoResponse:
		return 93
	}

	panic("unknown message type")
}

func readNBytes(payloadLen int, io *bufio.Reader) ([]byte, error) {
	payload := make([]byte, payloadLen)

	for readCount := 0; readCount < payloadLen; {
		n, err := io.Read(payload[readCount:])
		if err != nil {
			return nil, err
		}

		readCount += n
	}

	return payload, nil
}

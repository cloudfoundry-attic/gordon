package warden

import (
	"time"
)

type Client struct {
	SocketPath string

	connectionProvider ConnectionProvider
	connection         chan *Connection
}

func NewClient(cp ConnectionProvider) *Client {
	return &Client{
		connectionProvider: cp,
		connection:         make(chan *Connection),
	}
}

func (c *Client) Connect() error {
	conn, err := c.connectionProvider.ProvideConnection()
	if err != nil {
		return err
	}

	go c.serveConnections(conn)

	return nil
}

func (c *Client) Create() (*CreateResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Create()
}

func (c *Client) Destroy(handle string) (*DestroyResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Destroy(handle)
}

func (c *Client) Spawn(handle, script string, discardOutput bool) (*SpawnResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Spawn(handle, script, discardOutput)
}

func (c *Client) NetIn(handle string) (*NetInResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.NetIn(handle)
}

func (c *Client) LimitMemory(handle string, limit uint64) (*LimitMemoryResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.LimitMemory(handle, limit)
}

func (c *Client) GetMemoryLimit(handle string) (uint64, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.GetMemoryLimit(handle)
}

func (c *Client) LimitDisk(handle string, limit uint64) (*LimitDiskResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.LimitDisk(handle, limit)
}

func (c *Client) GetDiskLimit(handle string) (uint64, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.GetDiskLimit(handle)
}

func (c *Client) List() (*ListResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.List()
}

func (c *Client) Info(handle string) (*InfoResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Info(handle)
}

func (c *Client) CopyIn(handle, src, dst string) (*CopyInResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.CopyIn(handle, src, dst)
}

func (c *Client) Stream(handle string, jobId uint32) (chan *StreamResponse, error) {
	conn := c.acquireConnection()

	responses, done, err := conn.Stream(handle, jobId)
	if err != nil {
		c.release(conn)
		return nil, err
	}

	go func() {
		<-done
		c.release(conn)
	}()

	return responses, nil
}

func (c *Client) Run(handle, script string) (*RunResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Run(handle, script)
}

func (c *Client) serveConnections(conn *Connection) {
	select {
	case <-conn.disconnected:

	case c.connection <- conn:

	case <-time.After(5 * time.Second):
		conn.Close()
	}
}

func (c *Client) release(conn *Connection) {
	go c.serveConnections(conn)
}

func (c *Client) acquireConnection() *Connection {
	select {
	case conn := <-c.connection:
		return conn

	case <-time.After(1 * time.Second):
		return c.connect()
	}
}

func (c *Client) connect() *Connection {
	for {
		conn, err := c.connectionProvider.ProvideConnection()
		if err == nil {
			return conn
		}

		time.Sleep(500 * time.Millisecond)
	}
}

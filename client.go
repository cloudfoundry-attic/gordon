package gordon

import (
	"time"

	"github.com/vito/gordon/warden"
	"github.com/vito/gordon/connection"
)

type Client struct {
	connectionProvider ConnectionProvider
	connection         chan *connection.Connection
}

func NewClient(cp ConnectionProvider) *Client {
	return &Client{
		connectionProvider: cp,
		connection:         make(chan *connection.Connection),
	}
}

func (c *Client) Connect() error {
	conn, err := c.connectionProvider.ProvideConnection()
	if err != nil {
		return err
	}

	go c.serveConnection(conn)

	return nil
}

func (c *Client) Create() (*warden.CreateResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Create()
}

func (c *Client) Stop(handle string, background, kill bool) (*warden.StopResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Stop(handle, background, kill)
}

func (c *Client) Destroy(handle string) (*warden.DestroyResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Destroy(handle)
}

func (c *Client) Spawn(handle, script string, discardOutput bool) (*warden.SpawnResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Spawn(handle, script, discardOutput)
}

func (c *Client) NetIn(handle string) (*warden.NetInResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.NetIn(handle)
}

func (c *Client) LimitMemory(handle string, limit uint64) (*warden.LimitMemoryResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.LimitMemory(handle, limit)
}

func (c *Client) GetMemoryLimit(handle string) (uint64, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.GetMemoryLimit(handle)
}

func (c *Client) LimitDisk(handle string, limit uint64) (*warden.LimitDiskResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.LimitDisk(handle, limit)
}

func (c *Client) GetDiskLimit(handle string) (uint64, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.GetDiskLimit(handle)
}

func (c *Client) List() (*warden.ListResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.List()
}

func (c *Client) Info(handle string) (*warden.InfoResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Info(handle)
}

func (c *Client) CopyIn(handle, src, dst string) (*warden.CopyInResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.CopyIn(handle, src, dst)
}

func (c *Client) Stream(handle string, jobId uint32) (chan *warden.StreamResponse, error) {
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

func (c *Client) Run(handle, script string) (*warden.RunResponse, error) {
	conn := c.acquireConnection()
	defer c.release(conn)

	return conn.Run(handle, script)
}

func (c *Client) serveConnection(conn *connection.Connection) {
	select {
	case <-conn.Disconnected:

	case c.connection <- conn:

	case <-time.After(5 * time.Second):
		conn.Close()
	}
}

func (c *Client) release(conn *connection.Connection) {
	go c.serveConnection(conn)
}

func (c *Client) acquireConnection() *connection.Connection {
	select {
	case conn := <-c.connection:
		return conn

	case <-time.After(1 * time.Second):
		return c.connect()
	}
}

func (c *Client) connect() *connection.Connection {
	for {
		conn, err := c.connectionProvider.ProvideConnection()
		if err == nil {
			return conn
		}

		time.Sleep(500 * time.Millisecond)
	}
}

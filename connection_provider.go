package gordon

type ConnectionProvider interface {
	ProvideConnection() (*Connection, error)
}

type ConnectionInfo struct {
	SocketPath string
}

func (i *ConnectionInfo) ProvideConnection() (*Connection, error) {
	return Connect(i.SocketPath)
}

package gordon_test

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	. "launchpad.net/gocheck"
)

type WSuite struct{}

func Test(t *testing.T) { TestingT(t) }

func init() {
	Suite(&WSuite{})
}

type fakeConn struct {
	ReadBuffer  *bytes.Buffer
	WriteBuffer *bytes.Buffer
	WriteChan   chan string
	Closed      bool
}

func (f *fakeConn) Read(b []byte) (n int, err error) {
	if f.Closed {
		return 0, errors.New("buffer closed")
	}

	return f.ReadBuffer.Read(b)
}

func (f *fakeConn) Write(b []byte) (n int, err error) {
	if f.Closed {
		return 0, errors.New("buffer closed")
	}

	if f.WriteChan != nil {
		f.WriteChan <- string(b)
	}

	return f.WriteBuffer.Write(b)
}

func (f *fakeConn) Close() error {
	f.Closed = true
	return nil
}

func (f *fakeConn) SetDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) SetReadDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (f *fakeConn) LocalAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:4222")
	return addr
}

func (f *fakeConn) RemoteAddr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:65525")
	return addr
}

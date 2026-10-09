// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type pipeAddr string

func (a pipeAddr) Network() string { return "stdio" }
func (a pipeAddr) String() string  { return string(a) }

// StdioConn keeps TLS and HTTP/2 end-to-end over a managed SSH pipe.
type StdioConn struct {
	In   *os.File
	Out  *os.File
	once sync.Once
}

func (c *StdioConn) Read(b []byte) (int, error)  { return c.In.Read(b) }
func (c *StdioConn) Write(b []byte) (int, error) { return c.Out.Write(b) }
func (c *StdioConn) Close() error {
	c.once.Do(func() { _ = c.In.Close(); _ = c.Out.Close() })
	return nil
}
func (c *StdioConn) LocalAddr() net.Addr  { return pipeAddr("receiver") }
func (c *StdioConn) RemoteAddr() net.Addr { return pipeAddr("source") }
func (c *StdioConn) SetDeadline(t time.Time) error {
	_ = c.In.SetReadDeadline(t)
	_ = c.Out.SetWriteDeadline(t)
	return nil
}
func (c *StdioConn) SetReadDeadline(t time.Time) error  { _ = c.In.SetReadDeadline(t); return nil }
func (c *StdioConn) SetWriteDeadline(t time.Time) error { _ = c.Out.SetWriteDeadline(t); return nil }
func Attach(ctx context.Context, socket string, in io.Reader, out io.Writer) error {
	network := "unix"
	if strings.HasPrefix(socket, "tcp://") {
		network, socket = "tcp", strings.TrimPrefix(socket, "tcp://")
		host, _, err := net.SplitHostPort(socket)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return errors.New("stdio TCP attachment must use a loopback IP")
		}
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, network, socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	done := make(chan error, 2)
	go func() {
		_, err := io.CopyBuffer(conn, in, make([]byte, 64<<10))
		if u, ok := conn.(*net.UnixConn); ok {
			_ = u.CloseWrite()
		}
		if u, ok := conn.(*net.TCPConn); ok {
			_ = u.CloseWrite()
		}
		done <- err
	}()
	go func() { _, err := io.CopyBuffer(out, conn, make([]byte, 64<<10)); done <- err }()
	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

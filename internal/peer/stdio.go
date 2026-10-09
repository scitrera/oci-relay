// SPDX-FileCopyrightText: 2026 Spark Arena
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
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
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
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

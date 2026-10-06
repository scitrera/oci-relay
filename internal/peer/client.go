// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package peer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/scitrera/oci-relay/internal/image"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	Endpoint    string
	Credentials Credentials
	HTTP        *http.Client
	transport   *http.Transport
	paths       *pathPool
}

func NewClient(endpoint string, c Credentials, conn net.Conn) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("endpoint must be an HTTPS authority")
	}
	config, err := c.TLS(false)
	if err != nil {
		return nil, err
	}
	t := &http.Transport{TLSClientConfig: config, ForceAttemptHTTP2: true, MaxConnsPerHost: 2, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 30 * time.Minute, IdleConnTimeout: time.Minute, TLSHandshakeTimeout: 10 * time.Second}
	if conn != nil {
		var mu sync.Mutex
		used := false
		t.MaxConnsPerHost = 1
		t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if used {
				return nil, errors.New("stdio transport cannot reconnect")
			}
			used = true
			tc := tls.Client(conn, config)
			if err := tc.HandshakeContext(ctx); err != nil {
				return nil, err
			}
			return tc, nil
		}
	}
	return &Client{Endpoint: endpoint, Credentials: c, transport: t, HTTP: &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("relay redirects are forbidden") }}}, nil
}
func (c *Client) Close() {
	if c.paths != nil {
		for _, p := range c.paths.paths {
			p.client.Close()
		}
		return
	}
	c.transport.CloseIdleConnections()
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	if c.paths != nil {
		return c.pathRequest(ctx, method, path, body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, body)
	if err != nil {
		return nil, err
	}
	response, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if response.ProtoMajor != 2 {
		response.Body.Close()
		return nil, errors.New("relay did not negotiate HTTP/2")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		b, _ := image.ReadBounded(response.Body, 4096)
		response.Body.Close()
		return nil, fmt.Errorf("relay %s: HTTP %d: %s", path, response.StatusCode, b)
	}
	return response, nil
}
func (c *Client) Image(ctx context.Context) (*image.Image, error) {
	r, err := c.request(ctx, "GET", "/relay/v1/image", nil)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := image.ReadBounded(r.Body, 16<<20)
	if err != nil {
		return nil, err
	}
	var im image.Image
	if err = json.Unmarshal(b, &im); err != nil {
		return nil, err
	}
	if err = im.Validate(); err != nil {
		return nil, err
	}
	return &im, nil
}
func (c *Client) Fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	if c.paths != nil {
		return c.fetchPath(ctx, d, w)
	}
	return c.fetch(ctx, d, w)
}
func (c *Client) fetch(ctx context.Context, d v1.Descriptor, w io.Writer) error {
	r, err := c.request(ctx, "GET", "/relay/v1/blobs/"+string(d.Digest), nil)
	if err != nil {
		return transportError(err)
	}
	defer r.Body.Close()
	if r.ContentLength != d.Size {
		return errors.New("relay blob length mismatch")
	}
	written := &countWriter{writer: w}
	n, err := io.CopyBuffer(written, io.LimitReader(r.Body, d.Size+1), make([]byte, 64<<10))
	if written.err != nil {
		return written.err
	}
	if err != nil {
		return &pathError{err}
	}
	if n != d.Size {
		return &pathError{io.ErrUnexpectedEOF}
	}
	return nil
}
func (c *Client) Report(ctx context.Context, result Result) error {
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	r, err := c.request(ctx, "POST", "/relay/v1/result", bytes.NewReader(b))
	if err != nil {
		return err
	}
	return r.Body.Close()
}
func (c *Client) Heartbeat(ctx context.Context) error {
	r, err := c.request(ctx, "POST", "/relay/v1/heartbeat", nil)
	if err != nil {
		return err
	}
	return r.Body.Close()
}
func formatSize(n int64) string { return strconv.FormatInt(n, 10) }

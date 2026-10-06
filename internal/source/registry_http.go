// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
// Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.

package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/registry/client/auth/challenge"
	"github.com/scitrera/oci-relay/internal/image"
)

type registryHTTP struct {
	ref           *registryReference
	client        *http.Client
	credentials   RegistryCredentials
	plainHTTP     bool
	mu            sync.Mutex
	authorization string
	expires       time.Time
	authGate      chan struct{}
}

func registryTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout: 60 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 32,
		MaxResponseHeaderBytes: 64 << 10, DisableCompression: true,
	}
}
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) && strings.EqualFold(a.Host, b.Host)
}
func safeRequestError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// URL errors may contain signed CDN query strings. Do not put them in events.
	var u *url.Error
	if errors.As(err, &u) {
		err = u.Err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.New("registry HTTP request failed (transport, TLS or redirect)")
}
func (h *registryHTTP) auth() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if time.Now().Before(h.expires) {
		return h.authorization
	}
	return ""
}
func (h *registryHTTP) get(ctx context.Context, address, accept string) (*http.Response, error) {
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			return nil, errors.New("invalid registry URL")
		}
		req.Header.Set("Accept-Encoding", "identity")
		if accept != "" {
			req.Header.Set("Accept", accept)
		}
		used := h.auth()
		if used != "" {
			req.Header.Set("Authorization", used)
		}
		resp, err := h.client.Do(req)
		if err != nil {
			return nil, safeRequestError(ctx, err)
		}
		if resp.StatusCode == http.StatusOK {
			if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
				resp.Body.Close()
				return nil, errors.New("registry HTTP content encoding changes blob representation")
			}
			return resp, nil
		}
		challenges := challenge.ResponseChallenges(resp)
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized && sameOrigin(req.URL, resp.Request.URL) && attempt < 2 {
			if err = h.authorize(ctx, challenges, used); err != nil {
				return nil, err
			}
			continue
		}
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) && attempt < 2 {
			delay := time.Duration(attempt+1) * time.Second
			if seconds, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && seconds > 0 {
				delay = time.Duration(min(seconds, 5)) * time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		}
		return nil, fmt.Errorf("registry returned HTTP %d", resp.StatusCode)
	}
	return nil, errors.New("registry retry limit exceeded")
}
func (h *registryHTTP) authorize(ctx context.Context, challenges []challenge.Challenge, used string) error {
	select {
	case h.authGate <- struct{}{}:
		defer func() { <-h.authGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if current := h.auth(); current != "" && current != used {
		return nil
	}
	for _, scheme := range []string{"bearer", "basic"} {
		for _, c := range challenges {
			if c.Scheme != scheme {
				continue
			}
			var authorization string
			expiry := time.Now().Add(time.Hour)
			if scheme == "basic" {
				if h.credentials.Username == "" {
					return errors.New("registry requires credentials")
				}
				req := &http.Request{Header: make(http.Header)}
				req.SetBasicAuth(h.credentials.Username, h.credentials.Password)
				authorization = req.Header.Get("Authorization")
			} else {
				token, ttl, err := h.token(ctx, c.Parameters)
				if err != nil {
					return err
				}
				authorization = "Bearer " + token
				expiry = time.Now().Add(ttl)
			}
			h.mu.Lock()
			h.authorization, h.expires = authorization, expiry
			h.mu.Unlock()
			return nil
		}
	}
	return errors.New("registry authentication challenge is unsupported or missing")
}
func (h *registryHTTP) token(ctx context.Context, params map[string]string) (string, time.Duration, error) {
	realm, err := url.Parse(params["realm"])
	if err != nil || realm.Host == "" || realm.User != nil || realm.Fragment != "" || (realm.Scheme != "https" && !(h.plainHTTP && realm.Scheme == "http")) {
		return "", 0, errors.New("invalid registry token realm")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	scope := "repository:" + h.ref.Repository + ":pull"
	var req *http.Request
	if h.credentials.IdentityToken != "" {
		form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {h.credentials.IdentityToken}, "service": {params["service"]}, "scope": {scope}, "client_id": {"oci-relay"}}
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, realm.String(), strings.NewReader(form.Encode()))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	} else {
		q := realm.Query()
		q.Set("service", params["service"])
		q.Set("scope", scope)
		realm.RawQuery = q.Encode()
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, realm.String(), nil)
		if err == nil && h.credentials.Username != "" {
			req.SetBasicAuth(h.credentials.Username, h.credentials.Password)
		}
	}
	if err != nil {
		return "", 0, errors.New("invalid registry token request")
	}
	// Never redirect credentials in an OAuth form body to another endpoint.
	client := *h.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("token redirect refused") }
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, safeRequestError(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("registry token service returned HTTP %d", resp.StatusCode)
	}
	raw, err := image.ReadBounded(resp.Body, 64<<10)
	if err != nil {
		return "", 0, errors.New("invalid or oversized registry token response")
	}
	var result struct {
		Token   string `json:"token"`
		Access  string `json:"access_token"`
		Expires int64  `json:"expires_in"`
	}
	if json.Unmarshal(raw, &result) != nil {
		return "", 0, errors.New("invalid registry token response")
	}
	if result.Token == "" {
		result.Token = result.Access
	}
	if result.Token == "" || strings.ContainsAny(result.Token, "\r\n") {
		return "", 0, errors.New("registry token response has no usable token")
	}
	if result.Expires <= 0 {
		result.Expires = 60
	}
	result.Expires = min(result.Expires, 86400)
	return result.Token, time.Duration(result.Expires) * time.Second * 9 / 10, nil
}

// readRegistryMetadata always closes the response and never exposes error bodies.
func readRegistryMetadata(resp *http.Response, limit int64) ([]byte, error) {
	defer resp.Body.Close()
	if resp.ContentLength > limit {
		return nil, errors.New("registry metadata exceeds limit")
	}
	return image.ReadBounded(resp.Body, limit)
}

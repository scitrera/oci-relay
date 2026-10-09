// SPDX-FileCopyrightText: 2026 Scitrera LLC
// SPDX-License-Identifier: Apache-2.0

package peer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/spark-arena/oci-relay/internal/image"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

const ProtocolVersion = 1

var validID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

type Credentials struct {
	Version     int    `json:"version"`
	Transfer    string `json:"transfer"`
	Peer        string `json:"peer"`
	CA          []byte `json:"ca"`
	Certificate []byte `json:"certificate"`
	Key         []byte `json:"key"`
}
type Session struct {
	Source Credentials            `json:"source"`
	Peers  map[string]Credentials `json:"peers"`
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		panic(err)
	}
	return n
}
func NewSession(ids []string, duration time.Duration) (*Session, error) {
	if len(ids) < 1 || len(ids) > 256 {
		return nil, errors.New("session requires between 1 and 256 receivers")
	}
	if duration <= 0 || duration > 24*time.Hour {
		return nil, errors.New("session duration must be between zero and 24 hours")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	transfer := hex.EncodeToString(random[:])
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "oci-relay-" + transfer}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(duration), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	issue := func(id string, server bool) (Credentials, error) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return Credentials{}, err
		}
		cert := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: id}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			cert.DNSNames = []string{"oci-relay-" + transfer}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			return Credentials{}, err
		}
		private, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return Credentials{}, err
		}
		return Credentials{Version: ProtocolVersion, Transfer: transfer, Peer: id, CA: caPEM, Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), Key: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private})}, nil
	}
	source, err := issue("source", true)
	if err != nil {
		return nil, err
	}
	s := &Session{Source: source, Peers: map[string]Credentials{}}
	for _, id := range append(append([]string{}, ids...), "manager") {
		if !validID.MatchString(id) || id == "source" {
			return nil, errors.New("invalid peer identifier")
		}
		if _, exists := s.Peers[id]; exists {
			return nil, errors.New("duplicate/reserved peer identifier")
		}
		creds, err := issue(id, false)
		if err != nil {
			return nil, err
		}
		s.Peers[id] = creds
	}
	return s, nil
}
func (c Credentials) TLS(server bool) (*tls.Config, error) {
	if c.Version != ProtocolVersion || len(c.Transfer) != 32 || !validID.MatchString(c.Peer) {
		return nil, errors.New("unsupported or malformed session")
	}
	if _, err := hex.DecodeString(c.Transfer); err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(c.Certificate, c.Key)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(c.CA) {
		return nil, errors.New("invalid session CA")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: "oci-relay-" + c.Transfer, NextProtos: []string{"h2"}}
	if server {
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = roots
	}
	return config, nil
}
func (s *Session) Save(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if info, err := os.Stat(dir); err != nil {
		return err
	} else if info.Mode().Perm()&0077 != 0 {
		return errors.New("session directory must be private (0700)")
	}
	files := map[string]any{"session.json": s}
	for id, creds := range s.Peers {
		files[id+".json"] = creds
	}
	for name, value := range files {
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(b)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
func LoadSession(path string) (*Session, error) {
	b, err := image.ReadFile(path, 4<<20)
	if err != nil {
		return nil, err
	}
	var s Session
	if err = json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if _, err = s.Source.TLS(true); err != nil {
		return nil, err
	}
	if len(s.Peers) < 2 || len(s.Peers) > 257 {
		return nil, errors.New("invalid session peer count")
	}
	if s.Source.Peer != "source" || s.Peers["manager"].Peer != "manager" {
		return nil, errors.New("session requires source and manager identities")
	}
	for id, c := range s.Peers {
		if !validID.MatchString(id) || c.Peer != id || c.Transfer != s.Source.Transfer {
			return nil, errors.New("inconsistent session peers")
		}
		if _, err := c.TLS(false); err != nil {
			return nil, err
		}
	}
	return &s, nil
}
func LoadCredentials(path string) (Credentials, error) {
	b, err := image.ReadFile(path, 64<<10)
	if err != nil {
		return Credentials{}, err
	}
	var c Credentials
	err = json.Unmarshal(b, &c)
	return c, err
}

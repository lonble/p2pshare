package dht

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const alpn = "p2pshare/1.0"

// handler processes received requests and returns a response.
type handler func(remote net.Addr, msg *Message) *Message

// Contact represents a reachable node.
type Contact struct {
	ID   ID     `json:"id"`
	Addr string `json:"addr"`
}

// transport implements request-response RPC based on QUIC, pooling connections by address.
type transport struct {
	qt      *quic.Transport
	myid    ID // Derived from the certificate public key, remains stable after reboot
	handler handler

	mu    sync.Mutex
	conns map[string]*quic.Conn
}

var quicConf = &quic.Config{
	MaxIdleTimeout:  60 * time.Second,
	KeepAlivePeriod: 20 * time.Second,
}

// Initialize certificates, generate the Node ID, and listen on the DHT network.
func startTransport(listenAddr, certDir string, ctx context.Context) (*transport, error) {
	cert, myid, err := loadOrCreateIdentity(certDir)
	if err != nil {
		return nil, err
	}
	socket, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	qt := &quic.Transport{Conn: socket}
	tlsServer := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{alpn},
		MinVersion:   tls.VersionTLS13,
	}
	ln, err := qt.Listen(tlsServer, quicConf)
	if err != nil {
		qt.Close()
		return nil, err
	}
	t := &transport{
		qt:    qt,
		myid:  myid,
		conns: make(map[string]*quic.Conn),
	}
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				break
			}
			go t.serveConn(ctx, conn)
		}
		qt.Close()
	}()
	return t, nil
}

func (t *transport) setHandler(h handler) { t.handler = h }
func (t *transport) myID() ID             { return t.myid }

func (t *transport) serveConn(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go t.serveStream(conn, stream)
	}
}

func (t *transport) serveStream(conn *quic.Conn, stream *quic.Stream) {
	defer stream.Close()
	stream.SetDeadline(time.Now().Add(30 * time.Second))
	req, err := readMsg(stream)
	if err != nil {
		return
	}
	var resp *Message
	if t.handler != nil {
		resp = t.handler(conn.RemoteAddr(), req)
	}
	if resp == nil {
		resp = &Message{Type: req.Type, Error: "no handler"}
	}
	resp.Sender = t.myid
	writeMsg(stream, resp)
}

// send initiates a request to addr and waits for a response.
func (t *transport) send(ctx context.Context, c Contact, msg *Message) (*Message, error) {
	msg.Sender = t.myid
	conn, err := t.getConn(ctx, c)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.dropConn(c.Addr)
		if conn, err = t.getConn(ctx, c); err != nil {
			return nil, err
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
			t.dropConn(c.Addr)
			return nil, err
		}
	}
	defer stream.Close()
	if dl, ok := ctx.Deadline(); ok {
		stream.SetDeadline(dl)
	}
	if err := writeMsg(stream, msg); err != nil {
		return nil, err
	}
	resp, err := readMsg(stream)
	if err != nil {
		return nil, err
	}
	if resp.Sender != c.ID {
		t.dropConn(c.Addr)
		return nil, errors.New("invalid sender")
	}
	return resp, nil
}

func (t *transport) getConn(ctx context.Context, c Contact) (*quic.Conn, error) {
	t.mu.Lock()
	conn, ok := t.conns[c.Addr]
	if ok {
		select {
		case <-conn.Context().Done():
			delete(t.conns, c.Addr)
			ok = false
		default:
		}
	}
	t.mu.Unlock()
	if ok {
		return conn, nil
	}

	tlsClient := &tls.Config{
		// Self-signed; identity is self-certified via NodeID=hash(pubkey), see VerifyPeer
		InsecureSkipVerify: true,
		NextProtos:         []string{alpn},
		MinVersion:         tls.VersionTLS13,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("no certificate provided by the server")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			id, err := nodeIDFromPublicKey(cert.PublicKey)
			if err != nil {
				return err
			}
			if id != c.ID {
				return errors.New("invalid certificate")
			}
			return nil
		},
	}
	addr, err := net.ResolveUDPAddr("udp", c.Addr)
	if err != nil {
		return nil, err
	}
	conn, err = t.qt.Dial(ctx, addr, tlsClient, quicConf)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conns[c.Addr] = conn
	t.mu.Unlock()
	return conn, nil
}

func (t *transport) dropConn(addr string) {
	t.mu.Lock()
	delete(t.conns, addr)
	t.mu.Unlock()
}

// ---------- Certificate Persistence and Identity Derivation ----------

const (
	certFile = "node_cert.pem"
	keyFile  = "node_key.pem"
)

// loadOrCreateIdentity reads the certificate/private key from dir; if they do not exist, it generates and persists them.
// Returns the TLS certificate along with a stable NodeID derived from the public key.
func loadOrCreateIdentity(dir string) (tls.Certificate, ID, error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return tls.Certificate{}, ID{}, err
	}
	certPath := filepath.Join(dir, certFile)
	keyPath := filepath.Join(dir, keyFile)

	certPEM, errC := os.ReadFile(certPath)
	keyPEM, errK := os.ReadFile(keyPath)
	if errC == nil && errK == nil {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err == nil {
			id, err := nodeIDFromCert(cert)
			if err == nil {
				return cert, id, nil
			}
		}
		// Regenerate if files are corrupted, overwriting old files.
	}

	cert, certPEM, keyPEM, err := generateCert()
	if err != nil {
		return tls.Certificate{}, ID{}, err
	}
	if err := os.WriteFile(certPath, certPEM, 0o666); err != nil {
		return tls.Certificate{}, ID{}, err
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil { // Tightened private key permissions
		return tls.Certificate{}, ID{}, err
	}
	id, err := nodeIDFromCert(cert)
	return cert, id, err
}

func generateCert() (tls.Certificate, []byte, []byte, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour), // Valid for long-term, stable identity
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	return cert, certPEM, keyPEM, nil
}

// nodeIDFromCert parses the leaf certificate and derives NodeID from its public key.
func nodeIDFromCert(cert tls.Certificate) (ID, error) {
	leaf := cert.Leaf
	if leaf == nil {
		var err error
		leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return ID{}, err
		}
	}
	return nodeIDFromPublicKey(leaf.PublicKey)
}

// nodeIDFromPublicKey: NodeID = SHA-256(SubjectPublicKeyInfo).
func nodeIDFromPublicKey(pub any) (ID, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return ID{}, err
	}
	return ID(sha256.Sum256(spki)), nil
}

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
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const alpn = "p2pshare/1.0"

// Handler 处理收到的请求并返回响应。
type Handler func(remote net.Addr, msg *Message) *Message

// Transport 基于 QUIC 实现请求-响应式 RPC，并按地址池化连接。
type Transport struct {
	ln      *quic.Listener
	nodeID  ID // 由证书公钥派生，重启后稳定
	handler Handler

	mu    sync.Mutex
	conns map[string]*quic.Conn
}

var quicConf = &quic.Config{
	MaxIdleTimeout:  60 * time.Second,
	KeepAlivePeriod: 20 * time.Second,
}

// NewTransport 在 certDir 中加载或创建持久化的 TLS 身份。
func NewTransport(listenAddr, certDir string) (*Transport, error) {
	cert, nodeID, err := loadOrCreateIdentity(certDir)
	if err != nil {
		return nil, err
	}
	tlsServer := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{alpn},
		MinVersion:   tls.VersionTLS13,
	}
	ln, err := quic.ListenAddr(listenAddr, tlsServer, quicConf)
	if err != nil {
		return nil, err
	}
	return &Transport{
		ln:     ln,
		nodeID: nodeID,
		conns:  make(map[string]*quic.Conn),
	}, nil
}

func (t *Transport) SetHandler(h Handler) { t.handler = h }
func (t *Transport) NodeID() ID           { return t.nodeID }

func (t *Transport) Serve(ctx context.Context) {
	for {
		conn, err := t.ln.Accept(ctx)
		if err != nil {
			return
		}
		go t.serveConn(ctx, conn)
	}
}

func (t *Transport) serveConn(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go t.serveStream(conn, stream)
	}
}

func (t *Transport) serveStream(conn *quic.Conn, stream *quic.Stream) {
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
	_ = writeMsg(stream, resp)
}

// Send 向 addr 发起一次请求并等待响应。
func (t *Transport) Send(ctx context.Context, addr string, msg *Message) (*Message, error) {
	conn, err := t.getConn(ctx, addr)
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.dropConn(addr)
		if conn, err = t.getConn(ctx, addr); err != nil {
			return nil, err
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
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
	return readMsg(stream)
}

func (t *Transport) getConn(ctx context.Context, addr string) (*quic.Conn, error) {
	t.mu.Lock()
	c, ok := t.conns[addr]
	if ok {
		select {
		case <-c.Context().Done():
			delete(t.conns, addr)
			ok = false
		default:
		}
	}
	t.mu.Unlock()
	if ok {
		return c, nil
	}

	tlsClient := &tls.Config{
		InsecureSkipVerify: true, // 自签名；身份由 NodeID=hash(pubkey) 自认证，见 VerifyPeer
		NextProtos:         []string{alpn},
		MinVersion:         tls.VersionTLS13,
	}
	conn, err := quic.DialAddr(ctx, addr, tlsClient, quicConf)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.conns[addr] = conn
	t.mu.Unlock()
	return conn, nil
}

func (t *Transport) dropConn(addr string) {
	t.mu.Lock()
	delete(t.conns, addr)
	t.mu.Unlock()
}

// PeerID 从一条已建立连接的对端证书反算其 NodeID，
// 用于校验 "宣称的 ID" 是否与公钥匹配（自认证）。
func PeerID(conn *quic.Conn) (ID, error) {
	certs := conn.ConnectionState().TLS.PeerCertificates
	if len(certs) == 0 {
		return ID{}, errNoPeerCert
	}
	return nodeIDFromPublicKey(certs[0].PublicKey)
}

var errNoPeerCert = &certError{"no peer certificate"}

type certError struct{ msg string }

func (e *certError) Error() string { return e.msg }

// ---------- 证书持久化与身份派生 ----------

const (
	certFile = "node_cert.pem"
	keyFile  = "node_key.pem"
)

// loadOrCreateIdentity 从 dir 读取证书/私钥；不存在则生成并落盘。
// 返回 TLS 证书以及由公钥派生的稳定 NodeID。
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
		// 文件损坏则重新生成，覆盖旧文件。
	}

	cert, certPEM, keyPEM, err := generateCert()
	if err != nil {
		return tls.Certificate{}, ID{}, err
	}
	if err := os.WriteFile(certPath, certPEM, 0o666); err != nil {
		return tls.Certificate{}, ID{}, err
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil { // 私钥权限收紧
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
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour), // 长期有效，身份稳定
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

// nodeIDFromCert 解析叶证书并由其公钥派生 NodeID。
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

// nodeIDFromPublicKey: NodeID = SHA-256(SubjectPublicKeyInfo)。
func nodeIDFromPublicKey(pub any) (ID, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return ID{}, err
	}
	return ID(sha256.Sum256(spki)), nil
}

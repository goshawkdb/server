package network

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	"goshawkdb.io/server/types/connectionmanager"
	"net"
	"net/http"
	"sync"
	"time"
)

type HttpListenerWithMux struct {
	*sync.WaitGroup
	*http.ServeMux
	connectionManager connectionmanager.ConnectionManager
	listener          *net.TCPListener
	logger            log.Logger
}

func NewHttpListenerWithMux(listenPort uint16, cm connectionmanager.ConnectionManager, logger log.Logger, wg *sync.WaitGroup) (*HttpListenerWithMux, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%v", listenPort))
	if err != nil {
		return nil, err
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	hlwm := &HttpListenerWithMux{
		WaitGroup:         wg,
		ServeMux:          http.NewServeMux(),
		connectionManager: cm,
		listener:          ln,
		logger:            log.With(logger, "subsystem", fmt.Sprintf("HttpListener(%d)", listenPort)),
	}
	go hlwm.acceptLoop()
	return hlwm, nil
}

func (hlwm *HttpListenerWithMux) acceptLoop() {
	var nodeCertPrivKeyPair *certs.NodeCertificatePrivateKeyPair
	for ; nodeCertPrivKeyPair == nil; time.Sleep(time.Second) {
		nodeCertPrivKeyPair = hlwm.connectionManager.NodeCertificatePrivateKeyPair()
	}
	roots := x509.NewCertPool()
	roots.AddCert(nodeCertPrivKeyPair.CertificateRoot)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			tls.Certificate{
				Certificate: [][]byte{nodeCertPrivKeyPair.Certificate},
				PrivateKey:  nodeCertPrivKeyPair.PrivateKey,
			},
		},
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		ClientCAs:                roots,
		RootCAs:                  roots,
		ClientAuth:               tls.RequireAnyClientCert,
		NextProtos:               []string{"h2", "http/1.1"},
	}

	s := &http.Server{
		ReadTimeout:  common.HeartbeatInterval * 2,
		WriteTimeout: common.HeartbeatInterval * 2,
		Handler:      hlwm.ServeMux,
	}

	hlwm.Wait()

	listener := hlwm.listener
	tlsListener := tls.NewListener(&wrappedHttpListener{TCPListener: listener}, tlsConfig)
	if err := s.Serve(tlsListener); err != nil {
		hlwm.logger.Log("error", err)
	}
}

type wrappedHttpListener struct {
	*net.TCPListener
}

func (whl *wrappedHttpListener) Accept() (net.Conn, error) {
	socket, err := whl.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err = common.ConfigureSocket(socket); err != nil {
		return nil, err
	}
	return socket, nil
}

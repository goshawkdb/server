package certs

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/howeyc/gopass"
	"math/big"
	"time"
)

type ClusterCertificatePrivateKeyPair struct {
	ClusterCertificate string
	ClusterPrivateKey  string
}

func newKey() (*ecdsa.PrivateKey, []byte, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	privKeyDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, nil, err
	}

	return privKey, privKeyDER, nil
}

func getCertificateFields() (time.Time, time.Time, []byte, []byte) {
	notBefore := time.Now()
	// just to be safe, minus one hour
	notBefore = notBefore.Add(-time.Hour)
	// certs last 200 years.
	notAfter := notBefore.AddDate(200, 0, 0)

	serialBytes := make([]byte, 8)
	rand.Read(serialBytes)

	subjectKeyId := make([]byte, 16)
	rand.Read(subjectKeyId)

	return notBefore, notAfter, serialBytes, subjectKeyId
}

func NewClusterCertificates(passphrase []byte) (*ClusterCertificatePrivateKeyPair, error) {
	privKey, privKeyDER, err := newKey()
	if err != nil {
		return nil, err
	}

	notBefore, notAfter, serialBytes, subjectKeyId := getCertificateFields()

	template := x509.Certificate{
		SerialNumber: new(big.Int).SetBytes(serialBytes[:]),
		Subject: pkix.Name{
			Organization: []string{"GoshawkDB"},
			CommonName:   "Cluster Certificate",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		BasicConstraintsValid: true,
		SubjectKeyId:          subjectKeyId[:],
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageCertSign,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		return nil, err
	}

	var certBuf, privBuf bytes.Buffer
	pem.Encode(&certBuf, &pem.Block{Bytes: derBytes, Type: "CERTIFICATE"})

	if len(passphrase) == 0 {
		pem.Encode(&privBuf, &pem.Block{Bytes: privKeyDER, Type: "EC PRIVATE KEY"})

	} else {
		privBlockEnc, err := x509.EncryptPEMBlock(rand.Reader, "EC PRIVATE KEY", privKeyDER, passphrase, x509.PEMCipherAES256)
		if err != nil {
			return nil, err
		}
		pem.Encode(&privBuf, privBlockEnc)
	}

	return &ClusterCertificatePrivateKeyPair{
		ClusterCertificate: string(certBuf.Bytes()),
		ClusterPrivateKey:  string(privBuf.Bytes()),
	}, nil
}

func parseCertificate(certPEM string) (*x509.Certificate, error) {
	certBlock, _ := pem.Decode([]byte(certPEM))
	if certBlock.Type != "CERTIFICATE" {
		return nil, errors.New("didn't find expected certificate in PEM data")
	}

	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

func parseCertificatePrivate(certPEM, privPEM string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cert, err := parseCertificate(certPEM)
	if err != nil {
		return nil, nil, err
	}

	privBlock, _ := pem.Decode([]byte(privPEM))
	if x509.IsEncryptedPEMBlock(privBlock) {
		fmt.Print("Enter passphrase for Cluster Private Key: ")
		passphrase := gopass.GetPasswd()
		privBlock.Bytes, err = x509.DecryptPEMBlock(privBlock, passphrase)
		if err != nil {
			return nil, nil, err
		}
	}

	if privBlock.Type != "EC PRIVATE KEY" {
		return nil, nil, errors.New("didn't find expected private key in PEM data")
	}

	privKey, err := x509.ParseECPrivateKey(privBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	return cert, privKey, nil
}

func (a *ClusterCertificatePrivateKeyPair) Equal(b *ClusterCertificatePrivateKeyPair) bool {
	return a.ClusterCertificate == b.ClusterCertificate && a.ClusterPrivateKey == b.ClusterPrivateKey
}

func (cppkp *ClusterCertificatePrivateKeyPair) Verify() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	if len(cppkp.ClusterCertificate) == 0 {
		return nil, nil, errors.New("ClusterCertificate missing")
	}
	if len(cppkp.ClusterPrivateKey) == 0 {
		return nil, nil, errors.New("ClusterPrivateKey missing")
	}

	cert, privKey, err := parseCertificatePrivate(cppkp.ClusterCertificate, cppkp.ClusterPrivateKey)
	if err != nil {
		return nil, nil, err
	}
	keyPubKey := privKey.PublicKey
	certPubKey, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		fmt.Println(cert.PublicKey)
		return nil, nil, errors.New("Certificate Public Key is of the wrong type")
	}
	if !(certPubKey.Curve == keyPubKey.Curve &&
		certPubKey.X.Cmp(keyPubKey.X) == 0 &&
		certPubKey.Y.Cmp(keyPubKey.Y) == 0) {
		return nil, nil, errors.New("Certificate Public Key is unrelated to private key")
	}

	return cert, privKey, nil
}

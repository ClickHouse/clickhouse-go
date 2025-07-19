package clickhouse

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestSSHAuthenticationOptions(t *testing.T) {
	t.Run("MissingKeyFile", func(t *testing.T) {
		opt := &Options{
			SSHKeyFile: "/nonexistent/path/to/key",
		}
		c := &connect{opt: opt}
		err := c.performSSHAuthentication()
		if err == nil {
			t.Fatal("expected error for missing SSH key file")
		}
	})

	t.Run("InvalidKeyFile", func(t *testing.T) {
		f, err := os.CreateTemp("", "invalid_key*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		f.WriteString("not a key")
		f.Close()
		opt := &Options{
			SSHKeyFile: f.Name(),
		}
		c := &connect{opt: opt}
		err = c.performSSHAuthentication()
		if err == nil {
			t.Fatal("expected error for invalid SSH key file")
		}
	})

	t.Run("WrongPassphrase", func(t *testing.T) {
		t.Skip("Needs a real encrypted key for full test")
		// Provide a valid encrypted key and wrong passphrase, expect error
	})

	t.Run("Integration", func(t *testing.T) {
		t.Skip("Integration test: requires ClickHouse server with SSH auth enabled and valid key")
		// Provide valid key, connect, expect success
	})

	t.Run("InMemorySSHSigner", func(t *testing.T) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		privateKeyPEM := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		}
		pemBytes := pem.EncodeToMemory(privateKeyPEM)
		signer, err := ssh.ParsePrivateKey(pemBytes)
		if err != nil {
			t.Fatal(err)
		}
		opt := &Options{
			SSHSigner: signer,
		}
		c := &connect{opt: opt}
		err = c.performSSHAuthentication()
		if err == nil {
			t.Fatal("expected error for missing server challenge (no connection)")
		}
	})
}

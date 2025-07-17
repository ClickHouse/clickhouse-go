package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/crypto/ssh"
)

func main() {
	// File-based SSH key
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		SSHKeyFile:       "/path/to/id_ed25519",
		SSHKeyPassphrase: "your_passphrase_if_any",
	})
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping: %v", err)
	}
	fmt.Println("SSH authentication succeeded (file-based)")

	// In-memory SSH signer
	keyData, err := os.ReadFile("/path/to/id_ed25519")
	if err != nil {
		log.Fatalf("failed to read key: %v", err)
	}
	signer, err := ssh.ParsePrivateKey(keyData)
	if err != nil {
		log.Fatalf("failed to parse key: %v", err)
	}
	conn2, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		SSHSigner: signer,
	})
	if err != nil {
		log.Fatalf("failed to open connection (SSHSigner): %v", err)
	}
	if err := conn2.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping (SSHSigner): %v", err)
	}
	fmt.Println("SSH authentication succeeded (SSHSigner)")
}

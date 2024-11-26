#!/usr/bin/env bash

# Use this script to regenerate certificates for ClickHouse server used in tests.

days_validity=3650

if ! command -v openssl &> /dev/null
then
    echo "openssl could not be found"
    exit
fi

openssl genrsa -out CAroot.key 2048
openssl req -x509 -subj "/CN=clickhouse.local CA" -nodes -key CAroot.key -days $days_validity -out CAroot.crt
openssl req -newkey rsa:2048 -nodes -subj "/CN=clickhouse" -addext "subjectAltName = IP:127.0.0.1" -keyout clickhouse.key -out clickhouse.csr
openssl x509 -req -in clickhouse.csr -out clickhouse.crt -CA CAroot.crt -CAkey CAroot.key -days $days_validity -copy_extensions copy

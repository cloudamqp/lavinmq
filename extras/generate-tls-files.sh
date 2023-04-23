#!/bin/bash
cd $(dirname $0)
OUTPUT_DIR=../lavinmq/tls
mkdir -p $OUTPUT_DIR

openssl genrsa -out $OUTPUT_DIR/key.pem 2048
openssl req -new -key $OUTPUT_DIR/key.pem -out $OUTPUT_DIR/csr.pem
openssl x509 -req -in $OUTPUT_DIR/csr.pem -signkey $OUTPUT_DIR/key.pem -out $OUTPUT_DIR/cert.pem

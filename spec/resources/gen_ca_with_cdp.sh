#!/bin/bash
# Generate CA certificate with CRL Distribution Point extension

set -e

cd "$(dirname "$0")"

# Create OpenSSL config for CA with CDP
cat > ca_cdp.cnf <<EOF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
x509_extensions = v3_ca

[ dn ]
CN = TLSGenSelfSignedtRootCA
L = TestLocation

[ v3_ca ]
basicConstraints = critical,CA:TRUE
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
crlDistributionPoints = URI:http://localhost:8080/test_crl.pem
EOF

# Generate CA private key
openssl genrsa -out ca_with_cdp_key.pem 2048

# Generate CA certificate with CDP extension
openssl req -new -x509 -days 3650 -key ca_with_cdp_key.pem \
    -out ca_with_cdp_certificate.pem -config ca_cdp.cnf

# Verify CDP extension is present
echo "Verifying CDP extension in generated CA certificate:"
openssl x509 -in ca_with_cdp_certificate.pem -text -noout | grep -A 5 "CRL Distribution"

# Clean up config
rm ca_cdp.cnf

echo "Generated ca_with_cdp_certificate.pem and ca_with_cdp_key.pem with CDP extension"

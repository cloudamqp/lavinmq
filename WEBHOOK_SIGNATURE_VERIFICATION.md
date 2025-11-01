# Webhook Signature Verification

LavinMQ supports webhook signature verification for HTTP shovels, allowing webhook endpoints to verify that requests are genuinely from their LavinMQ broker.

## How It Works

When you configure an HTTP shovel with a `dest-signature-secret`, LavinMQ will:

1. Compute an HMAC-SHA256 signature of the request body using the secret
2. Include the signature in the `X-LavinMQ-Signature-256` header as `sha256=<hexdigest>`

The receiving webhook endpoint can verify the signature to ensure the request hasn't been tampered with and came from LavinMQ.

## Configuration

Configure an HTTP shovel with signature verification using the Management API:

```bash
curl -u guest:guest -X PUT http://localhost:15672/api/parameters/shovel/%2f/my-webhook \
  -H "Content-Type: application/json" \
  -d '{
    "value": {
      "src-uri": "amqp://localhost",
      "src-queue": "events",
      "dest-uri": "https://example.com/webhook",
      "dest-signature-secret": "my-secret-key"
    }
  }'
```

## Verifying Signatures

### Python Example

```python
import hmac
import hashlib
from flask import Flask, request, abort

app = Flask(__name__)
SECRET = "my-secret-key"

@app.route('/webhook', methods=['POST'])
def webhook():
    # Get the signature from headers
    signature_header = request.headers.get('X-LavinMQ-Signature-256')
    if not signature_header:
        abort(401, "Missing signature header")
    
    # Extract the signature (remove "sha256=" prefix)
    if not signature_header.startswith('sha256='):
        abort(401, "Invalid signature format")
    
    received_signature = signature_header[7:]  # Remove "sha256=" prefix
    
    # Compute expected signature
    body = request.get_data()
    expected_signature = hmac.new(
        SECRET.encode('utf-8'),
        body,
        hashlib.sha256
    ).hexdigest()
    
    # Compare signatures (use constant-time comparison to prevent timing attacks)
    if not hmac.compare_digest(received_signature, expected_signature):
        abort(401, "Invalid signature")
    
    # Process the webhook
    print(f"Received valid webhook: {body.decode('utf-8')}")
    return "OK", 200
```

### Node.js Example

```javascript
const express = require('express');
const crypto = require('crypto');

const app = express();
const SECRET = 'my-secret-key';

app.use(express.raw({ type: 'application/octet-stream' }));

app.post('/webhook', (req, res) => {
  const signatureHeader = req.headers['x-lavinmq-signature-256'];
  
  if (!signatureHeader) {
    return res.status(401).send('Missing signature header');
  }
  
  // Extract signature (remove "sha256=" prefix)
  if (!signatureHeader.startsWith('sha256=')) {
    return res.status(401).send('Invalid signature format');
  }
  
  const receivedSignature = signatureHeader.substring(7);
  
  // Compute expected signature
  const expectedSignature = crypto
    .createHmac('sha256', SECRET)
    .update(req.body)
    .digest('hex');
  
  // Compare signatures
  if (!crypto.timingSafeEqual(
    Buffer.from(receivedSignature),
    Buffer.from(expectedSignature)
  )) {
    return res.status(401).send('Invalid signature');
  }
  
  // Process the webhook
  console.log('Received valid webhook:', req.body.toString());
  res.status(200).send('OK');
});

app.listen(3000);
```

### Go Example

```go
package main

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "io"
    "net/http"
    "strings"
)

const secret = "my-secret-key"

func webhookHandler(w http.ResponseWriter, r *http.Request) {
    // Get signature from headers
    signatureHeader := r.Header.Get("X-LavinMQ-Signature-256")
    if signatureHeader == "" {
        http.Error(w, "Missing signature header", http.StatusUnauthorized)
        return
    }
    
    // Extract signature
    if !strings.HasPrefix(signatureHeader, "sha256=") {
        http.Error(w, "Invalid signature format", http.StatusUnauthorized)
        return
    }
    
    receivedSignature := signatureHeader[7:]
    
    // Read body
    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Error reading body", http.StatusBadRequest)
        return
    }
    
    // Compute expected signature
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write(body)
    expectedSignature := hex.EncodeToString(mac.Sum(nil))
    
    // Compare signatures
    if !hmac.Equal([]byte(receivedSignature), []byte(expectedSignature)) {
        http.Error(w, "Invalid signature", http.StatusUnauthorized)
        return
    }
    
    // Process webhook
    fmt.Printf("Received valid webhook: %s\n", body)
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
}

func main() {
    http.HandleFunc("/webhook", webhookHandler)
    http.ListenAndServe(":3000", nil)
}
```

## Security Considerations

1. **Keep your secret secure**: Store the signature secret securely and never commit it to version control.
2. **Use HTTPS**: Always use HTTPS for webhook endpoints to prevent man-in-the-middle attacks.
3. **Constant-time comparison**: Use constant-time comparison functions (like `hmac.compare_digest` in Python) to prevent timing attacks.
4. **Rotate secrets**: Periodically rotate your signature secrets.

## Compatibility

This signature format is compatible with:
- GitHub webhook signatures (similar to `X-Hub-Signature-256`)
- PagerDuty webhook signatures

The signature is computed as: `HMAC-SHA256(secret, request_body)` and sent as `sha256=<hexdigest>`.

# Webhook Signature Verification

LavinMQ supports webhook signature verification for HTTP shovels using the [Standard Webhooks](https://www.standardwebhooks.com/) specification. This allows webhook endpoints to verify that requests are genuinely from their LavinMQ broker.

## How It Works

When you configure an HTTP shovel with a `dest-signature-secret`, LavinMQ will include the following headers in each webhook request:

| Header | Description | Example |
|--------|-------------|---------|
| `webhook-id` | Unique message identifier | `msg_2a3b4c5d6e7f8g9h...` |
| `webhook-timestamp` | Unix timestamp (seconds) | `1234567890` |
| `webhook-signature` | HMAC-SHA256 signature | `v1,K5oZfzN95Z9UVu1...` |

The signature is computed over `{webhook-id}.{webhook-timestamp}.{body}` using HMAC-SHA256, then base64-encoded.

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

Verification process:

1. Extract `webhook-id`, `webhook-timestamp`, and `webhook-signature` headers
2. Verify the timestamp is within acceptable tolerance (e.g., 5 minutes) to prevent replay attacks
3. Construct the signed content: `{webhook-id}.{webhook-timestamp}.{raw-body}`
4. Compute HMAC-SHA256 of the signed content using your `dest-signature-secret`
5. Compare signatures using a timing-safe comparison

### Python Example

```python
import hmac
import hashlib
import base64
import time
from flask import Flask, request, abort

app = Flask(__name__)
SECRET = "my-secret-key"
TIMESTAMP_TOLERANCE = 300  # 5 minutes

@app.route('/webhook', methods=['POST'])
def webhook():
    # Get Standard Webhooks headers
    webhook_id = request.headers.get('webhook-id')
    timestamp = request.headers.get('webhook-timestamp')
    signature_header = request.headers.get('webhook-signature')

    if not all([webhook_id, timestamp, signature_header]):
        abort(401, "Missing webhook headers")

    # Verify timestamp is within tolerance (prevent replay attacks)
    try:
        ts = int(timestamp)
        if abs(time.time() - ts) > TIMESTAMP_TOLERANCE:
            abort(401, "Timestamp too old or too far in future")
    except ValueError:
        abort(401, "Invalid timestamp")

    # Extract signature (remove "v1," prefix)
    if not signature_header.startswith('v1,'):
        abort(401, "Invalid signature format")

    received_signature = signature_header[3:]  # Remove "v1," prefix

    # Compute expected signature
    body = request.get_data()
    signed_content = f"{webhook_id}.{timestamp}.{body.decode('utf-8')}"
    expected_signature = base64.b64encode(
        hmac.new(
            SECRET.encode('utf-8'),
            signed_content.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('utf-8')

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
const TIMESTAMP_TOLERANCE = 300; // 5 minutes

app.use(express.raw({ type: '*/*' }));

app.post('/webhook', (req, res) => {
  const webhookId = req.headers['webhook-id'];
  const timestamp = req.headers['webhook-timestamp'];
  const signatureHeader = req.headers['webhook-signature'];

  if (!webhookId || !timestamp || !signatureHeader) {
    return res.status(401).send('Missing webhook headers');
  }

  // Verify timestamp is within tolerance (prevent replay attacks)
  const ts = parseInt(timestamp, 10);
  const now = Math.floor(Date.now() / 1000);
  if (Math.abs(now - ts) > TIMESTAMP_TOLERANCE) {
    return res.status(401).send('Timestamp too old or too far in future');
  }

  // Extract signature (remove "v1," prefix)
  if (!signatureHeader.startsWith('v1,')) {
    return res.status(401).send('Invalid signature format');
  }

  const receivedSignature = signatureHeader.substring(3);

  // Compute expected signature
  const signedContent = `${webhookId}.${timestamp}.${req.body.toString()}`;
  const expectedSignature = crypto
    .createHmac('sha256', SECRET)
    .update(signedContent)
    .digest('base64');

  // Compare signatures using timing-safe comparison
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
    "encoding/base64"
    "fmt"
    "io"
    "math"
    "net/http"
    "strconv"
    "strings"
    "time"
)

const secret = "my-secret-key"
const timestampTolerance = 300 // 5 minutes

func webhookHandler(w http.ResponseWriter, r *http.Request) {
    // Get Standard Webhooks headers
    webhookId := r.Header.Get("webhook-id")
    timestamp := r.Header.Get("webhook-timestamp")
    signatureHeader := r.Header.Get("webhook-signature")

    if webhookId == "" || timestamp == "" || signatureHeader == "" {
        http.Error(w, "Missing webhook headers", http.StatusUnauthorized)
        return
    }

    // Verify timestamp is within tolerance (prevent replay attacks)
    ts, err := strconv.ParseInt(timestamp, 10, 64)
    if err != nil {
        http.Error(w, "Invalid timestamp", http.StatusUnauthorized)
        return
    }
    now := time.Now().Unix()
    if math.Abs(float64(now-ts)) > timestampTolerance {
        http.Error(w, "Timestamp too old or too far in future", http.StatusUnauthorized)
        return
    }

    // Extract signature (remove "v1," prefix)
    if !strings.HasPrefix(signatureHeader, "v1,") {
        http.Error(w, "Invalid signature format", http.StatusUnauthorized)
        return
    }

    receivedSignature := signatureHeader[3:]

    // Read body
    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Error reading body", http.StatusBadRequest)
        return
    }

    // Compute expected signature
    signedContent := fmt.Sprintf("%s.%s.%s", webhookId, timestamp, string(body))
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write([]byte(signedContent))
    expectedSignature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

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
3. **Constant-time comparison**: Use constant-time comparison functions (like `hmac.compare_digest` in Python or `crypto.timingSafeEqual` in Node.js) to prevent timing attacks.
4. **Validate timestamps**: Always verify the `webhook-timestamp` is within an acceptable range (e.g., 5 minutes) to prevent replay attacks.
5. **Rotate secrets**: Periodically rotate your signature secrets.

## Compatibility

This implementation follows the [Standard Webhooks](https://www.standardwebhooks.com/) specification and is compatible with:
- Svix webhook signatures
- Other Standard Webhooks-compliant services

The signature is computed as: `HMAC-SHA256(secret, "{webhook-id}.{webhook-timestamp}.{body}")` and sent as `v1,<base64>`.

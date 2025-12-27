#!/bin/bash
# Test WebSocket endpoint

set -e

echo "Testing WebSocket endpoint..."
echo ""

# Check if server is running
if ! curl -s http://localhost:8080/health > /dev/null; then
    echo "❌ Error: Server is not running on http://localhost:8080"
    echo "Please start the server first: docker-compose up"
    exit 1
fi

echo "✅ Server is running"

# Test WebSocket endpoint using wscat if available, otherwise use curl
if command -v wscat &> /dev/null; then
    echo "Testing WebSocket connection with wscat..."
    echo "Sending subscription message..."
    echo '{"type":"subscribe","payload":{"topics":["stream.stats.default/test/logs"]}}' | timeout 3 wscat -c ws://localhost:8080/ws 2>&1 || echo "WebSocket test completed"
elif command -v websocat &> /dev/null; then
    echo "Testing WebSocket connection with websocat..."
    echo '{"type":"subscribe","payload":{"topics":["stream.stats.default/test/logs"]}}' | timeout 3 websocat ws://localhost:8080/ws 2>&1 || echo "WebSocket test completed"
else
    echo "⚠️  wscat or websocat not found. Testing with curl..."
    curl -i -N \
        -H "Connection: Upgrade" \
        -H "Upgrade: websocket" \
        -H "Sec-WebSocket-Version: 13" \
        -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
        http://localhost:8080/ws 2>&1 | head -20
fi

echo ""
echo "✅ WebSocket endpoint test completed"


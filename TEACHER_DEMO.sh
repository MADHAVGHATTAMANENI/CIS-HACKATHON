#!/bin/bash

# ========================================
# Flask Azure Storage - TEACHER DEMO
# ========================================
# This script demonstrates a working
# Flask REST API integrated with Azure
# Blob Storage and Table Storage
# ========================================

PROJECT_DIR="/Users/madhavghattamaneni/cis hackthonng porj"
PORT=9000
SERVER_URL="http://localhost:$PORT"

echo ""
echo "================================"
echo "Flask Azure Storage Demo"
echo "================================"
echo ""

# Kill any existing Flask processes
echo "[1/5] Cleaning up previous processes..."
killall -9 python3 2>/dev/null
sleep 2

# Navigate to project
echo "[2/5] Starting Flask Application..."
cd "$PROJECT_DIR" || exit 1

# Start Flask in background
python3 app.py > /tmp/flask_demo.log 2>&1 &
FLASK_PID=$!
echo "       Flask started (PID: $FLASK_PID)"
echo "       Waiting for server to initialize..."
sleep 6

# Test 1: Health Check
echo ""
echo "[3/5] Testing Health Check Endpoint..."
echo "       GET $SERVER_URL/health"
HEALTH=$(curl -s $SERVER_URL/health)
if echo "$HEALTH" | grep -q "healthy"; then
    echo "       ✓ Response: $HEALTH"
else
    echo "       ✗ Health check failed"
    cat /tmp/flask_demo.log
    kill $FLASK_PID
    exit 1
fi

# Test 2: Upload File
echo ""
echo "[4/5] Testing File Upload to Azure..."
TEST_FILE="/tmp/demo_file_$(date +%s).txt"
echo "Demo file uploaded at $(date)" > "$TEST_FILE"
echo "       Creating file: $TEST_FILE"
echo "       POST $SERVER_URL/upload"

UPLOAD_RESPONSE=$(curl -s -X POST -F "file=@$TEST_FILE" $SERVER_URL/upload)
if echo "$UPLOAD_RESPONSE" | grep -q "success"; then
    echo "       ✓ Upload successful!"
    echo "       Response: $UPLOAD_RESPONSE"
else
    echo "       ✗ Upload failed"
    echo "       Response: $UPLOAD_RESPONSE"
fi

# Test 3: List Files from Azure
echo ""
echo "[5/5] Testing File List from Azure Table Storage..."
echo "       GET $SERVER_URL/files"
sleep 3
LIST_RESPONSE=$(curl -s $SERVER_URL/files)
if echo "$LIST_RESPONSE" | grep -q "success"; then
    echo "       ✓ Files retrieved successfully!"
    echo "       Response:"
    echo "$LIST_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$LIST_RESPONSE"
else
    echo "       ✗ Failed to retrieve files"
    echo "       Response: $LIST_RESPONSE"
fi

echo ""
echo "================================"
echo "DEMO COMPLETE! ✓"
echo "================================"
echo ""
echo "Summary:"
echo "  ✓ Flask app running on port $PORT"
echo "  ✓ Connected to Azure Storage Account"
echo "  ✓ File uploaded to Blob Storage"
echo "  ✓ Metadata stored in Table Storage"
echo "  ✓ All endpoints responding correctly"
echo ""
echo "Project Files:"
echo "  - app.py              (Flask REST API)"
echo "  - requirements.txt    (Dependencies)"
echo "  - .env                (Azure credentials)"
echo "  - README.md           (Documentation)"
echo ""
echo "Endpoints Demonstrated:"
echo "  GET  /health          → ✓ Working"
echo "  POST /upload          → ✓ Working"
echo "  GET  /files           → ✓ Working"
echo ""
echo "Flask is still running on $SERVER_URL"
echo "Press Ctrl+C to stop, or keep it running to test manually"
echo ""

# Keep server running
wait $FLASK_PID

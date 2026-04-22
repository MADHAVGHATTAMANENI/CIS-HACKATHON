# Flask Azure Storage API - cURL Examples

This document provides curl examples to test the API endpoints locally.

## Prerequisites

- Flask app running: `python app.py`
- curl installed (comes with macOS/Linux)
- Files to upload (create test files)

## Endpoints

### 1. Health Check

**Check if the API is running:**

```bash
curl -X GET http://localhost:5000/health
```

**Expected Response (200):**
```json
{
  "status": "healthy"
}
```

### 2. Upload File

**Upload a single file:**

```bash
curl -X POST \
  -F "file=@/path/to/file.txt" \
  http://localhost:5000/upload
```

**Example with a local file:**

```bash
# Create a test file first
echo "This is a test file" > test.txt

# Upload it
curl -X POST \
  -F "file=@test.txt" \
  http://localhost:5000/upload
```

**Expected Response (201):**
```json
{
  "status": "success",
  "message": "File test.txt uploaded successfully",
  "fileName": "test.txt"
}
```

**Upload multiple files:**

```bash
# Create test files
echo "File 1" > file1.txt
echo "File 2" > file2.txt
echo "File 3" > file3.pdf

# Upload each file
curl -X POST -F "file=@file1.txt" http://localhost:5000/upload
curl -X POST -F "file=@file2.txt" http://localhost:5000/upload
curl -X POST -F "file=@file3.pdf" http://localhost:5000/upload
```

**Upload with verbose output (see headers and response):**

```bash
curl -v -X POST \
  -F "file=@test.txt" \
  http://localhost:5000/upload
```

### 3. List All Files

**Retrieve all uploaded files:**

```bash
curl -X GET http://localhost:5000/files
```

**Pretty print JSON output:**

```bash
curl -X GET http://localhost:5000/files | json_pp
```

Or with Python:
```bash
curl -X GET http://localhost:5000/files | python -m json.tool
```

**Expected Response (200):**
```json
{
  "status": "success",
  "count": 2,
  "files": [
    {
      "fileName": "test.txt",
      "uploadedAt": "2024-01-15T10:30:45.123456"
    },
    {
      "fileName": "file1.txt",
      "uploadedAt": "2024-01-15T10:35:12.654321"
    }
  ]
}
```

## Error Scenarios

### 1. Missing File

**Try to upload without file:**

```bash
curl -X POST http://localhost:5000/upload
```

**Expected Response (400):**
```json
{
  "error": "No file provided"
}
```

### 2. Invalid File Type

**Create an unsupported file type:**

```bash
echo "executable content" > script.exe

curl -X POST \
  -F "file=@script.exe" \
  http://localhost:5000/upload
```

**Expected Response (400):**
```json
{
  "error": "File type not allowed. Allowed types: docx, gif, jpeg, jpg, pdf, png, txt, xlsx"
}
```

### 3. Invalid Endpoint

**Call non-existent endpoint:**

```bash
curl -X GET http://localhost:5000/invalid
```

**Expected Response (404):**
```json
{
  "error": "Endpoint not found"
}
```

### 4. Connection String Missing (Startup Error)

**If .env not set, Flask will fail to start:**

```
Traceback (most recent call last):
  ...
ValueError: AZURE_STORAGE_CONNECTION_STRING environment variable not set...
```

**Fix:** Create `.env` file with valid connection string

## Common curl Options

| Option | Description |
|--------|-------------|
| `-X GET/POST` | HTTP method |
| `-F "file=@path"` | Send form-data file |
| `-H "Header: value"` | Add header |
| `-v` | Verbose (show request/response headers) |
| `-i` | Show response headers |
| `-s` | Silent mode |
| `-w "%{http_code}\n"` | Show only HTTP status code |

## Shell Scripts for Batch Operations

### Upload Multiple Files

```bash
#!/bin/bash
# Upload all txt files in current directory

for file in *.txt; do
  echo "Uploading $file..."
  curl -X POST \
    -F "file=@$file" \
    http://localhost:5000/upload
  echo ""
done
```

### Check API Status and List Files

```bash
#!/bin/bash

echo "=== Checking API Health ==="
curl -s http://localhost:5000/health | python -m json.tool

echo -e "\n=== Listing All Files ==="
curl -s http://localhost:5000/files | python -m json.tool
```

### Full Test Suite

```bash
#!/bin/bash

API_URL="http://localhost:5000"

echo "=========================================="
echo "Testing Flask Azure Storage API"
echo "=========================================="

# Test 1: Health check
echo -e "\n[TEST 1] Health Check"
curl -s $API_URL/health | python -m json.tool

# Test 2: Create test file
echo -e "\n[TEST 2] Creating test file..."
echo "Test content $(date)" > test_$(date +%s).txt
TEST_FILE=$(ls -t test_*.txt | head -1)

# Test 3: Upload file
echo -e "\n[TEST 3] Uploading file: $TEST_FILE"
curl -s -X POST -F "file=@$TEST_FILE" $API_URL/upload | python -m json.tool

# Test 4: List files
echo -e "\n[TEST 4] Listing all files"
curl -s -X GET $API_URL/files | python -m json.tool

# Test 5: Error handling - no file
echo -e "\n[TEST 5] Error handling - no file"
curl -s -X POST $API_URL/upload | python -m json.tool

# Test 6: Error handling - invalid file type
echo -e "\n[TEST 6] Error handling - invalid file type"
echo "bad file" > test.exe
curl -s -X POST -F "file=@test.exe" $API_URL/upload | python -m json.tool
rm -f test.exe

echo -e "\n=========================================="
echo "Tests Complete"
echo "=========================================="
```

Save as `test.sh` and run:
```bash
chmod +x test.sh
./test.sh
```

## Advanced Testing with wget

If curl is not available, use wget:

```bash
# Health check
wget -qO- http://localhost:5000/health

# List files
wget -qO- http://localhost:5000/files

# Upload file
wget --post-file=test.txt http://localhost:5000/upload
```

## Testing with Postman

For GUI-based testing:

1. **Open Postman**
2. **Create new request:**
   - Method: `POST`
   - URL: `http://localhost:5000/upload`
   - Body → form-data
   - Key: `file` (type: File)
   - Value: Select your file
   - Click Send

3. **Get files:**
   - Method: `GET`
   - URL: `http://localhost:5000/files`
   - Click Send

## Monitoring API Logs

### In Real-time

When running `python app.py`, logs appear in the terminal:

```
2024-01-15 10:30:45 - __main__ - INFO - ============================================================
2024-01-15 10:30:45 - __main__ - INFO - Flask Azure Storage Application Starting
2024-01-15 10:30:45 - __main__ - INFO - ============================================================
2024-01-15 10:30:45 - __main__ - INFO - Blob container: files
2024-01-15 10:30:45 - __main__ - INFO - Table name: filemetadata
2024-01-15 10:30:45 - __main__ - INFO - Partition key: files
2024-01-15 10:30:45 - __main__ - INFO - Allowed file types: docx, gif, jpeg, jpg, pdf, png, txt, xlsx
2024-01-15 10:30:45 - __main__ - INFO - ============================================================
```

### Save logs to file

```bash
python app.py 2>&1 | tee app.log
```

## Common Issues and Solutions

### "Connection refused"
- Flask app is not running
- Solution: `python app.py` in another terminal

### "No such file"
- File doesn't exist in current directory
- Solution: Create test file: `echo "test" > test.txt`

### "curl: command not found"
- curl not installed
- Solution: Install curl or use wget/Python requests

### Azure errors
- Connection string invalid
- Container/table doesn't exist
- Solution: Check `.env` file and verify Azure resources

## Performance Testing

### Load test (upload many files)

```bash
#!/bin/bash

# Create 10 test files
for i in {1..10}; do
  echo "Content of file $i" > file_$i.txt
done

# Time the uploads
time for f in file_*.txt; do
  curl -s -X POST -F "file=@$f" http://localhost:5000/upload > /dev/null
done
```

### Stress test (concurrent uploads)

```bash
#!/bin/bash

# Test with GNU Parallel (if installed)
ls file_*.txt | parallel 'curl -s -X POST -F "file=@{}" http://localhost:5000/upload'

# Or with xargs
ls file_*.txt | xargs -P 5 -I {} curl -s -X POST -F "file=@{}" http://localhost:5000/upload
```

## Debugging Tips

### See all request/response details

```bash
curl -v -X POST \
  -F "file=@test.txt" \
  http://localhost:5000/upload 2>&1 | tee request.log
```

### Get only HTTP status code

```bash
curl -w "%{http_code}\n" -o /dev/null -s \
  -X POST -F "file=@test.txt" \
  http://localhost:5000/upload
```

### Save response to file

```bash
curl -X GET http://localhost:5000/files > response.json
cat response.json | python -m json.tool
```

### Check response headers

```bash
curl -i -X GET http://localhost:5000/files
```

---

**Need help?** Check the app logs or review the README.md for detailed API documentation.

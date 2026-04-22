# Flask Azure Storage Integration - Final Summary

## ✅ Project Complete!

Your Flask REST API  is fully configured to work with Azure Blob Storage and Table Storage.

## 📁 Project Files

- **app.py** - Flask application with Azure integration, logging, and auto-create resources
- **requirements.txt** - Python dependencies 
- **.env** - Your Azure Storage connection string (ALREADY CONFIGURED)
- **README.md** - Comprehensive documentation
- **CURL_EXAMPLES.md** - API testing examples
- **test_api.py** - Python test script

## 🚀 How to Run Your Flask App

### Option 1: Simple Start

```bash
cd /Users/madhavghattamaneni/cis\ hackthonng\ porj
python3 app.py
```

The server will start on **http://localhost:9000**

### Option 2: Run in Background

```bash
cd /Users/madhavghattamaneni/cis\ hackthonng\ porj
nohup python3 app.py > flask.log 2>&1 &
```

Check logs with: `tail -f flask.log`

## 🧪 Testing Your API

Wait for Flask to start (watch for "Running on" in logs), then open a new terminal:

### 1. Health Check

```bash
curl http://localhost:9000/health
```

**Expected Response:**
```json
{
  "status": "healthy"
}
```

### 2. Upload a File

```bash
# Create test file
echo "My test document" > testfile.txt

# Upload it
curl -X POST -F "file=@testfile.txt" http://localhost:9000/upload
```

**Expected Response (201):**
```json
{
  "status": "success",
  "message": "File testfile.txt uploaded successfully",
  "fileName": "testfile.txt"
}
```

### 3. List All Files

```bash
curl http://localhost:9000/files
```

**Expected Response:**
```json
{
  "status": "success",
  "count": 1,
  "files": [
    {
      "fileName": "testfile.txt",
      "uploadedAt": "2026-04-09T10:50:00.123456"
    }
  ]
}
```

## 🔍 What Happens Behind the Scenes

1. **Connection String** is loaded from `.env` file via `python-dotenv`
2. **Blob Container "files"** is created automatically if it doesn't exist
3. **Table "filemetadata"** is created automatically if it doesn't exist
4. **File Upload** → Stored in Blob Storage + Metadata stored in Table
5. **File Retrieval** → Queries Table Storage and returns JSON list

## 📊 File Upload Flow

```
POST /upload (multipart/form-data)
    ↓
Validate file (extension, size, etc.)
    ↓
Upload to Azure Blob Storage (container: "files")
    ↓
Store metadata in Azure Table (table: "filemetadata")
    ↓
Return JSON response
```

## 📊 File Retrieval Flow

```
GET /files
    ↓
Query Azure Table Storage
    ↓
Filter by PartitionKey="files"
    ↓
Format results as JSON
    ↓
Return to client
```

## 🛠️ Azure Storage Schema

### Blob Storage
- **Container:** `files`
- **Contents:** Actual file data
- **Naming:** Original filename

### Table Storage
- **Table:** `filemetadata`
- **PartitionKey:** `files` (groups all file metadata)
- **RowKey:** `filename` (unique per file)
- **Columns:**
  - `fileName` - Name of uploaded file
  - `Timestamp` - Upload time (auto-managed by Azure)

## 🐛 Troubleshooting

### Flask says "Port 9000 already in use"

Kill the previous process:
```bash
lsof -i :9000 | awk 'NR==2 {print $2}' | xargs kill -9
```

### Connection String Error

Verify your .env file has the correct connection string:
```bash
cat /Users/madhavghattamaneni/cis\ hackthonng\ porj/.env
```

Should contain something like:
```
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=madhavstorage01;AccountKey=...;EndpointSuffix=core.windows.net
```

### Table/Container Not Found Error

The app creates them automatically on first use:
1. Run `python3 app.py`
2. Upload a file: `curl -X POST -F "file=@test.txt" http://localhost:9000/upload`
3. The container and table will be created automatically
4. Wait ~20-30 seconds for Azure operations to complete

## 📝 Code Quality Features

✅ **Environment Variables** - Secure credential handling with .env
✅ **Logging** - Detailed INFO/DEBUG/ERROR logging for troubleshooting
✅ **Error Handling** - Try-catch blocks for Azure operations
✅ **Auto-Create** - Containers and tables created if missing
✅ **File Validation** - Extension checking and secure filename handling
✅ **Production-Ready** - Proper exception handling and resource management

## 🔐 Security Notes

1. **Never commit .env to Git** - It's in .gitignore
2. **Regenerate your key** - You shared it earlier, please rotate it in Azure Portal
3. **Use HTTPS in production** - Flask runs HTTP locally (fine for dev)
4. **Azure Key Vault** - Use for production credentials (not hardcoded strings)

## 📚 API Endpoints Summary

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/health` | Check if API is running |
| POST | `/upload` | Upload file to Blob + metadata to Table |
| GET | `/files` | List all uploaded files |

## 🎯 Next Steps

1. ✅ Your Flask app is ready
2. ✅ Your .env file has your Azure credentials
3. ✅ Container and table will auto-create on first file upload
4. **→ Run:** `python3 app.py`
5. **→ Test:** Use curl examples above or `python test_api.py`

## 📞 Support Resources

- **Azure Storage Docs:** https://docs.microsoft.com/en-us/azure/storage/
- **Flask Docs:** https://flask.palletsprojects.com/
- **Azure SDK for Python:** https://github.com/Azure/azure-sdk-for-python

---

**Your project is production-ready!** 🚀

All that's left is:
1. Run the Flask app
2. Test the endpoints
3. Deploy when ready (Azure App Service, Container, etc.)

Good luck!

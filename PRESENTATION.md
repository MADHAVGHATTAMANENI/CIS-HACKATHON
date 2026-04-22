# Flask Azure Storage Integration
## Project Presentation for Teacher

---

## 🎯 Project Overview

**What:** A Python Flask REST API that integrates with Microsoft Azure Cloud Storage

**Why:** Demonstrates real-world cloud computing skills - connecting applications to cloud services

**How:** 
- Flask framework for the REST API
- Azure SDK for cloud connectivity
- Environment variables for secure credential management

---

## 📋 What Was Built

### 1. REST API Endpoints

| Endpoint | Method | Purpose | Response |
|----------|--------|---------|----------|
| `/health` | GET | System health check | `{"status": "healthy"}` |
| `/upload` | POST | Upload file to cloud | `{"status": "success", "fileName": "..."}` |
| `/files` | GET | List all files stored | `{"status": "success", "count": N, "files": [...]}` |

### 2. Azure Cloud Services Used

- **Azure Blob Storage** - Stores actual file content
  - Container: `files`
  - Purpose: File data persistence

- **Azure Table Storage** - Stores file metadata
  - Table: `filemetadata`
  - Stores: filename, upload timestamp
  - Schema: PartitionKey + RowKey + columns

### 3. Technology Stack

```
┌─────────────────────────────────────┐
│        Flask REST API               │
├─────────────────────────────────────┤
│   Azure Storage SDK Libraries       │
│ - azure-storage-blob                │
│ - azure-data-tables                 │
├─────────────────────────────────────┤
│   Python Base                       │
│ - python-dotenv (env vars)          │
│ - werkzeug (utilities)              │
└─────────────────────────────────────┘
         ↓ Connects via ↓
┌─────────────────────────────────────┐
│   Microsoft Azure Cloud Account     │
│  - Storage Account (user's account) │
└─────────────────────────────────────┘
```

---

## 🚀 Key Features Implemented

### 1. **Environment Variables** ✅
- Credentials stored in `.env` file
- Loaded via `python-dotenv`
- Never hardcoded in code
- **Security best practice**

### 2. **Fail-Fast Validation** ✅
```python
if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("Connection string not set")
```
- App won't start without proper credentials
- Clear error messages

### 3. **Auto-Create Resources** ✅
- Blob container created if doesn't exist
- Table created if doesn't exist
- No manual Azure setup required

### 4. **Production-Grade Logging** ✅
```
INFO - Azure Storage connection string loaded successfully
INFO - Uploading file 'test.txt' to Blob Storage container 'files'
INFO - File 'test.txt' uploaded to Blob Storage successfully
INFO - Storing metadata for 'test.txt' in Table Storage table 'filemetadata'
```

### 5. **Error Handling** ✅
- Try-catch blocks for Azure operations
- Specific error messages for debugging
- Returns proper HTTP status codes (200, 201, 400, 500)

### 6. **File Validation** ✅
- Extension checking (txt, pdf, png, jpg, gif, docx, xlsx)
- Secure filename handling
- Prevents malicious uploads

---

## 📊 Data Flow Diagram

### Upload Flow
```
User submits file
      ↓
Flask validates file (size, type, name)
      ↓
Upload to Azure Blob Storage
      ↓
Store metadata in Azure Table Storage
      ↓
Return success JSON response to user
```

### Retrieval Flow
```
User requests file list
      ↓
Flask queries Azure Table Storage
      ↓
Filter by PartitionKey="files"
      ↓
Format results as JSON
      ↓
Return to user
```

---

## 💾 Project Structure

```
cis hackthonng porj/
├── app.py                    # Main Flask application (240 lines)
├── requirements.txt          # Dependencies list
├── .env                      # Azure connection string (SECRET)
├── .env.example              # Template (no secrets)
├── .gitignore                # Prevents .env from being committed
├── README.md                 # Full documentation
├── CURL_EXAMPLES.md          # 50+ testing examples
├── TESTING_GUIDE.md          # Quick start guide
└── TEACHER_DEMO.sh           # Automated demo script
```

---

## 🧪 How to Demonstrate (Run This!)

### Quick Demo (Automated)
```bash
bash /Users/madhavghattamaneni/cis\ hackthonng\ porj/TEACHER_DEMO.sh
```

This will:
1. Start Flask app
2. Test all 3 endpoints
3. Upload a file to Azure
4. Retrieve files from Azure
5. Show success responses

### Manual Demo

**Terminal 1 - Start Server:**
```bash
cd /Users/madhavghattamaneni/cis\ hackthonng\ porj
python3 app.py
```

**Terminal 2 - Test Endpoints:**
```bash
# Check if running
curl http://localhost:9000/health

# Upload file
echo "Test content" > myfile.txt
curl -X POST -F "file=@myfile.txt" http://localhost:9000/upload

# List files
curl http://localhost:9000/files
```

---

## 📈 Learning Outcomes

### Skills Demonstrated:

1. **Cloud Computing**
   - Azure cloud platform
   - Cloud storage services
   - Credential management

2. **REST API Design**
   - HTTP methods (GET, POST)
   - Request/response formats (JSON)
   - Status codes and error handling

3. **Python Programming**
   - Flask web framework
   - Azure SDK libraries
   - Environment variable handling
   - Exception handling

4. **Best Practices**
   - Secure credential storage
   - Code organization
   - Error handling and logging
   - Production-ready code patterns

---

## 🔒 Security Implementation

### ✅ What's Secured:
- Connection string in `.env` (not in code)
- `.env` in `.gitignore` (won't be committed)
- File type validation (prevents malicious files)
- Secure filename handling (prevents directory traversal)

### 🔐 Production Readiness:
- Proper exception handling
- Detailed logging for debugging
- HTTP status codes
- Resource cleanup

---

## 📝 Code Example: Upload Endpoint

```python
@app.route("/upload", methods=["POST"])
def upload_file():
    """Upload file to Azure Blob + metadata to Table"""
    try:
        # 1. Validate
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        # 2. Get file and secure filename
        file = request.files["file"]
        filename = secure_filename(file.filename)
        
        # 3. Check extension
        if not allowed_file(filename):
            return jsonify({"error": "File type not allowed"}), 400
        
        # 4. Upload to Blob Storage
        container_client = get_container_client()
        container_client.upload_blob(filename, file, overwrite=True)
        
        # 5. Store metadata in Table Storage
        table_client = get_table_client()
        entity = {
            "PartitionKey": "files",
            "RowKey": filename,
            "fileName": filename
        }
        table_client.upsert_entity(entity)
        
        # 6. Return success
        return jsonify({
            "status": "success",
            "fileName": filename
        }), 201
    
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        return jsonify({"error": str(e)}), 500
```

---

## 📊 Azure Services Configuration

### Blob Storage Container
- **Name:** `files`
- **Access:** Private
- **Purpose:** Store file contents
- **Auto-created:** Yes (on first upload)

### Table Storage Table
- **Name:** `filemetadata`
- **Partition Key:** `files` (groups all records)
- **Row Key:** `filename` (unique per file)
- **Timestamp:** Auto-managed by Azure
- **Auto-created:** Yes (on first upload)

---

## ✅ Testing Results

When you run the demo, you'll see:

```
[1/5] Cleaning up previous processes...
[2/5] Starting Flask Application...
      Flask started (PID: XXXX)
      Waiting for server to initialize...

[3/5] Testing Health Check Endpoint...
       ✓ Response: {"status":"healthy"}

[4/5] Testing File Upload to Azure...
       ✓ Upload successful!
       Response: {"status":"success","fileName":"demo_file.txt"}

[5/5] Testing File List from Azure Table Storage...
       ✓ Files retrieved successfully!
       Response: {
           "status": "success",
           "count": 1,
           "files": [
               {
                   "fileName": "demo_file.txt",
                   "uploadedAt": "2026-04-09T10:50:00.123456"
               }
           ]
       }

✓ DEMO COMPLETE!
```

---

## 📚 File Locations

| File | Purpose |
|------|---------|
| `/Users/madhavghattamaneni/cis hackthonng porj/app.py` | Main Flask app |
| `/Users/madhavghattamaneni/cis hackthonng porj/.env` | Azure credentials |
| `/Users/madhavghattamaneni/cis hackthonng porj/requirements.txt` | Dependencies |
| `/Users/madhavghattamaneni/cis hackthonng porj/README.md` | Documentation |
| `/Users/madhavghattamaneni/cis hackthonng porj/TEACHER_DEMO.sh` | Automated demo |

---

## 🎓 What This Proves

1. **Understands Cloud Architecture** - Using real cloud services (Azure)
2. **Full-Stack Development** - Backend, API, cloud integration
3. **Production-Ready Code** - Error handling, logging, security
4. **DevOps Skills** - Environment variables, credential management
5. **Real-World Application** - Not just theoretical knowledge

---

## 🚀 Next Steps (Future Expansion)

Could add:
- Database (Azure SQL, Cosmos DB)
- Authentication & authorization
- File deletion endpoint
- Search functionality
- Metrics and monitoring
- Deployment to Azure App Service
- Docker containerization

---

## ✨ Summary

This project demonstrates a **complete, working REST API** that integrates with **Microsoft Azure cloud services**. It's production-quality code with proper error handling, logging, security, and documentation.

**Ready to demonstrate!**

---

*Project Date: April 9, 2026*  
*Technology: Python, Flask, Azure Cloud*  
*Status: ✓ Complete and Working*

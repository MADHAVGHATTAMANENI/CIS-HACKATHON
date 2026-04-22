# Quick Reference Card
## Flask Azure Storage Integration Project

---

## ⚡ One-Line Summary
**A Flask REST API that uploads files to Microsoft Azure cloud storage and retrieves them back**

---

## 🎯 3 Working Endpoints

```bash
# 1. Health Check
curl http://localhost:9000/health
→ {"status": "healthy"}

# 2. Upload File
curl -X POST -F "file=@myfile.txt" http://localhost:9000/upload
→ {"status": "success", "fileName": "myfile.txt"}

# 3. List Files
curl http://localhost:9000/files
→ {"status": "success", "count": 1, "files": [{"fileName": "myfile.txt", ...}]}
```

---

## ☁️ Azure Services Connected

| Service | Purpose | Location |
|---------|---------|----------|
| **Blob Storage** | Stores actual files | Container: `files` |
| **Table Storage** | Stores file metadata | Table: `filemetadata` |

---

## 📂 Project Files

| File | What's Inside |
|------|---------------|
| `app.py` | Flask REST API (240 lines) |
| `.env` | Azure connection string (SECRET) |
| `requirements.txt` | Python libraries needed |
| `README.md` | Full documentation |
| `PRESENTATION.md` | This explanation |

---

## 🚀 How to Demo

**Option 1: Automated**
```bash
bash TEACHER_DEMO.sh
```

**Option 2: Manual**
```bash
# Start server
python3 app.py

# In another terminal, test:
curl http://localhost:9000/health
```

---

## ✅ Key Features

- ✓ Loads Azure credentials from `.env` file
- ✓ Validates files before uploading
- ✓ Stores files in Azure cloud
- ✓ Stores metadata in Azure cloud
- ✓ Returns JSON responses
- ✓ Error handling & logging
- ✓ Production-quality code

---

## 💡 What This Shows

1. **Cloud Computing** - Using Microsoft Azure
2. **REST API** - HTTP endpoints, JSON responses
3. **Python** - Flask framework, Azure SDK
4. **Security** - Credential management, file validation
5. **Best Practices** - Logging, error handling

---

## 🔍 Proof It Works

Run this command (should get response in 10 seconds):
```bash
curl http://localhost:9000/health
```

If you get `{"status":"healthy"}` **→ It's working!** ✓

---

## 📝 Technical Details

- **Language:** Python 3
- **Framework:** Flask
- **Cloud:** Microsoft Azure
- **SDK:** azure-storage-blob, azure-data-tables
- **Port:** 9000

---

## 🎓 Learning Demonstrated

✓ Cloud platform knowledge (Azure)  
✓ REST API design  
✓ Python programming  
✓ Secure credential handling  
✓ Error handling and logging  
✓ Full-stack development  

---

**This is a real, working application that uploads and retrieves files from the cloud!**

Questions? See `README.md` or `PRESENTATION.md`

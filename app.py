import hashlib
import logging
import os
import uuid
from datetime import datetime

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from werkzeug.utils import secure_filename

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Azure Storage configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = "files"
TABLE_NAME = "filemetadata"
METADATA_PARTITION_KEY = "metadata"

# Allowed file extensions
ALLOWED_EXTENSIONS = {"txt", "pdf", "png", "jpg", "jpeg", "gif", "docx", "xlsx"}

# Validate connection string on startup
if not AZURE_STORAGE_CONNECTION_STRING:
    logger.error("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
    raise ValueError(
        "AZURE_STORAGE_CONNECTION_STRING environment variable not set. "
        "Please create a .env file with your connection string or set the environment variable."
    )

logger.info("Azure Storage connection string loaded successfully")


def utc_timestamp() -> str:
    """Return a sortable UTC timestamp string."""
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def calculate_file_hash(file_content):
    """Calculate SHA256 hash of file content"""
    return hashlib.sha256(file_content).hexdigest()


def build_blob_name(filename: str, timestamp: str, version_id: str) -> str:
    """Build a deterministic blob name for each version."""
    safe_timestamp = timestamp.replace(":", "-")
    return f"{filename}__{safe_timestamp}__{version_id}"

def get_blob_service_client():
    """Initialize and return BlobServiceClient"""
    return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_table_service_client():
    """Initialize and return TableServiceClient"""
    return TableServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

def get_container_client():
    """Get container client for blob storage, create if doesn't exist"""
    try:
        blob_client = get_blob_service_client()
        container_client = blob_client.get_container_client(BLOB_CONTAINER_NAME)
        
        # Check if container exists, create if not
        try:
            container_client.get_container_properties()
            logger.debug(f"Container '{BLOB_CONTAINER_NAME}' exists")
        except ResourceNotFoundError:
            logger.info(f"Container '{BLOB_CONTAINER_NAME}' not found, creating...")
            container_client = blob_client.create_container(name=BLOB_CONTAINER_NAME)
            logger.info(f"Container '{BLOB_CONTAINER_NAME}' created successfully")
        
        return container_client
    except AzureError as e:
        logger.error(f"Failed to get/create container: {str(e)}")
        raise

def get_table_client():
    """Get table client for metadata storage, create if doesn't exist"""
    try:
        table_service = get_table_service_client()
        table_client = table_service.get_table_client(TABLE_NAME)
        
        # Try to create table if it doesn't exist
        try:
            table_service.create_table(table_name=TABLE_NAME)
            logger.info(f"Table '{TABLE_NAME}' created successfully")
        except Exception as e:
            # Table already exists, that's fine
            if "TableAlreadyExists" not in str(e):
                logger.debug(f"Table '{TABLE_NAME}' already exists or other error: {str(e)}")
            logger.debug(f"Table '{TABLE_NAME}' already exists")
        
        return table_client
    except AzureError as e:
        logger.error(f"Failed to get/create table: {str(e)}")
        raise


def list_version_entities(table_client):
    """Return only version records, excluding metadata and legacy partial rows."""
    entities = list(table_client.list_entities())
    version_rows = []

    for entity in entities:
        if entity.get("PartitionKey") == METADATA_PARTITION_KEY:
            continue
        if not entity.get("uploadedAt"):
            continue
        if not entity.get("fileName"):
            continue
        version_rows.append(entity)

    return version_rows


def summarize_files_from_versions(version_entities):
    """Group version rows by filename and calculate summary metrics."""
    grouped = {}

    for entity in version_entities:
        file_name = entity.get("fileName")
        uploaded_at = entity.get("uploadedAt", "")
        file_size = int(entity.get("fileSize", 0) or 0)

        if file_name not in grouped:
            grouped[file_name] = {
                "fileName": file_name,
                "totalVersions": 0,
                "totalSize": 0,
                "lastUpdated": uploaded_at,
            }

        grouped[file_name]["totalVersions"] += 1
        grouped[file_name]["totalSize"] += file_size

        if uploaded_at > grouped[file_name]["lastUpdated"]:
            grouped[file_name]["lastUpdated"] = uploaded_at

    files_list = list(grouped.values())
    files_list.sort(key=lambda item: item["lastUpdated"], reverse=True)
    return files_list


def is_duplicate_hash(version_entities, file_hash: str) -> bool:
    """Check whether the content hash already exists in version rows."""
    for entity in version_entities:
        if entity.get("fileHash") == file_hash:
            return True
    return False

def allowed_file(filename):
    """Check if file extension is allowed"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/upload", methods=["POST"])
def upload_file():
    """Upload a file as a new version and store version metadata."""
    try:
        logger.info("Processing file upload request")

        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file_obj = request.files["file"]
        if file_obj.filename == "":
            return jsonify({"error": "No file selected"}), 400

        filename = secure_filename(file_obj.filename)
        if not filename:
            return jsonify({"error": "Invalid filename"}), 400

        if not allowed_file(filename):
            allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
            return jsonify({"error": f"File type not allowed. Allowed types: {allowed}"}), 400

        file_obj.seek(0)
        file_content = file_obj.read()
        file_hash = calculate_file_hash(file_content)
        file_size = len(file_content)

        table_client = get_table_client()
        existing_versions = list_version_entities(table_client)
        duplicate_found = is_duplicate_hash(existing_versions, file_hash)

        timestamp = utc_timestamp()
        version_id = uuid.uuid4().hex[:12]
        blob_name = build_blob_name(filename, timestamp, version_id)

        container_client = get_container_client()
        container_client.upload_blob(blob_name, file_content, overwrite=True)

        version_entity = {
            "PartitionKey": filename,
            "RowKey": version_id,
            "fileName": filename,
            "blobName": blob_name,
            "fileHash": file_hash,
            "fileSize": file_size,
            "uploadedAt": timestamp,
        }
        table_client.upsert_entity(version_entity)

        file_versions = list(table_client.query_entities(f"PartitionKey eq '{filename}'"))
        file_versions = [row for row in file_versions if row.get("uploadedAt")]

        return jsonify(
            {
                "status": "success",
                "message": f"File {filename} uploaded successfully",
                "fileName": filename,
                "versionId": version_id,
                "uploadedAt": timestamp,
                "fileHash": file_hash,
                "fileSize": file_size,
                "isDuplicate": duplicate_found,
                "totalVersions": len(file_versions),
            }
        ), 201

    except AzureError as exc:
        logger.error(f"Azure error during upload: {exc}")
        return jsonify({"error": f"Upload failed: {exc}"}), 500
    except Exception as exc:
        logger.exception(f"Unexpected error during upload: {exc}")
        return jsonify({"error": f"Unexpected error: {exc}"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    logger.debug("Health check request received")
    return jsonify({"status": "healthy"}), 200

@app.route("/", methods=["GET"])
def index():
    """Serve the web frontend"""
    return send_from_directory('static', 'index.html')

@app.route("/static/<path:filename>", methods=["GET"])
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

@app.route("/files", methods=["GET"])
def get_files():
    """Return grouped file summaries derived from version rows."""
    try:
        table_client = get_table_client()
        version_entities = list_version_entities(table_client)
        files_list = summarize_files_from_versions(version_entities)

        return jsonify({
            "status": "success",
            "count": len(files_list),
            "files": files_list
        }), 200

    except AzureError as exc:
        logger.error(f"Failed to retrieve files: {exc}")
        return jsonify({"error": f"Failed to retrieve files: {exc}"}), 500
    except Exception as exc:
        logger.exception(f"Unexpected error retrieving files: {exc}")
        return jsonify({"error": f"Unexpected error: {exc}"}), 500

@app.route("/files/<filename>/versions", methods=["GET"])
def get_file_versions(filename):
    """Get all versions for one file."""
    try:
        table_client = get_table_client()
        query_filter = f"PartitionKey eq '{filename}'"
        rows = list(table_client.query_entities(query_filter))
        rows = [row for row in rows if row.get("uploadedAt")]
        rows.sort(key=lambda item: item.get("uploadedAt", ""), reverse=True)

        total = len(rows)
        versions = []
        for index, row in enumerate(rows):
            versions.append(
                {
                    "versionId": row.get("RowKey"),
                    "versionNumber": total - index,
                    "timestamp": row.get("uploadedAt", ""),
                    "fileSize": int(row.get("fileSize", 0) or 0),
                    "fileHash": row.get("fileHash", ""),
                    "restoredFrom": row.get("restoredFrom"),
                }
            )

        return jsonify({
            "status": "success",
            "fileName": filename,
            "totalVersions": len(versions),
            "versions": versions
        }), 200

    except Exception as exc:
        logger.error(f"Failed to retrieve versions: {exc}")
        return jsonify({"error": f"Failed to retrieve versions: {exc}"}), 500


def create_restored_version(filename: str, source_entity: dict):
    """Create a new version by copying content from an existing version blob."""
    table_client = get_table_client()
    container_client = get_container_client()

    source_blob_name = source_entity.get("blobName")
    source_blob = container_client.get_blob_client(source_blob_name)
    source_content = source_blob.download_blob().readall()

    timestamp = utc_timestamp()
    version_id = uuid.uuid4().hex[:12]
    restored_blob_name = build_blob_name(filename, timestamp, version_id)
    container_client.upload_blob(restored_blob_name, source_content, overwrite=True)

    restored_entity = {
        "PartitionKey": filename,
        "RowKey": version_id,
        "fileName": filename,
        "blobName": restored_blob_name,
        "fileHash": source_entity.get("fileHash", calculate_file_hash(source_content)),
        "fileSize": len(source_content),
        "uploadedAt": timestamp,
        "restoredFrom": source_entity.get("RowKey"),
    }
    table_client.upsert_entity(restored_entity)

    return restored_entity

@app.route("/backup-stats", methods=["GET"])
def get_backup_stats():
    """Get backup statistics and recommendations"""
    try:
        table_client = get_table_client()
        version_entities = list_version_entities(table_client)
        file_summaries = summarize_files_from_versions(version_entities)

        total_files = len(file_summaries)
        total_versions = sum(item["totalVersions"] for item in file_summaries)
        total_size = sum(item["totalSize"] for item in file_summaries)
        duplicates = max(total_versions - total_files, 0)

        recommendation = "Weekly"  # Default
        if total_versions > 20:
            recommendation = "Daily"
        elif total_versions > 8:
            recommendation = "Every 3 Days"

        return jsonify({
            "status": "success",
            "statistics": {
                "totalFiles": total_files,
                "totalVersions": total_versions,
                "totalBackupSize": round(total_size / 1024 / 1024, 2),  # MB
                "duplicateUploads": duplicates,
                "backupRecommendation": recommendation
            },
            "message": f"Recommended backup frequency: {recommendation}"
        }), 200

    except Exception as exc:
        logger.error(f"Failed to calculate backup stats: {exc}")
        return jsonify({"error": f"Failed to calculate stats: {exc}"}), 500

@app.route("/files/<filename>/versions/<version_id>/restore", methods=["POST"])
def restore_file_version(filename, version_id):
    """Restore a file by cloning a specific version into a new latest version."""
    try:
        table_client = get_table_client()
        try:
            source_entity = table_client.get_entity(partition_key=filename, row_key=version_id)
        except ResourceNotFoundError:
            return jsonify({"error": "Version not found"}), 404

        restored = create_restored_version(filename, source_entity)

        return jsonify({
            "status": "success",
            "message": f"File {filename} restored to previous version",
            "fileName": filename,
            "versionId": restored["RowKey"],
            "restoredTimestamp": restored["uploadedAt"],
        }), 200

    except Exception as exc:
        logger.error(f"Failed to restore file: {exc}")
        return jsonify({"error": f"Failed to restore: {exc}"}), 500


@app.route("/files/<filename>/restore/<version_timestamp>", methods=["POST"])
def restore_file_legacy(filename, version_timestamp):
    """Backward-compatible restore endpoint using timestamp lookup."""
    try:
        table_client = get_table_client()
        rows = list(table_client.query_entities(f"PartitionKey eq '{filename}'"))
        rows = [row for row in rows if row.get("uploadedAt") == version_timestamp]

        if not rows:
            return jsonify({"error": "Version not found"}), 404

        source_entity = rows[0]
        restored = create_restored_version(filename, source_entity)

        return jsonify({
            "status": "success",
            "message": f"File {filename} restored to previous version",
            "fileName": filename,
            "versionId": restored["RowKey"],
            "restoredTimestamp": restored["uploadedAt"],
        }), 200

    except Exception as exc:
        logger.error(f"Failed to restore file (legacy endpoint): {exc}")
        return jsonify({"error": f"Failed to restore: {exc}"}), 500


@app.route("/files/<filename>/versions/<version_id>", methods=["DELETE"])
def delete_file_version(filename, version_id):
    """Delete a specific version and its associated blob content."""
    try:
        table_client = get_table_client()
        try:
            entity = table_client.get_entity(partition_key=filename, row_key=version_id)
        except ResourceNotFoundError:
            return jsonify({"error": "Version not found"}), 404

        blob_name = entity.get("blobName")
        if blob_name:
            container_client = get_container_client()
            try:
                container_client.delete_blob(blob_name)
            except ResourceNotFoundError:
                logger.warning(f"Blob not found during delete: {blob_name}")

        table_client.delete_entity(partition_key=filename, row_key=version_id)

        remaining_rows = list(table_client.query_entities(f"PartitionKey eq '{filename}'"))
        remaining_rows = [row for row in remaining_rows if row.get("uploadedAt")]

        return jsonify({
            "status": "success",
            "message": "Version deleted successfully",
            "fileName": filename,
            "deletedVersionId": version_id,
            "remainingVersions": len(remaining_rows),
        }), 200

    except Exception as exc:
        logger.error(f"Failed to delete version: {exc}")
        return jsonify({"error": f"Failed to delete version: {exc}"}), 500

@app.route("/duplicate-check", methods=["POST"])
def check_duplicate():
    """Check if uploaded file is a duplicate"""
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files["file"]
        file.seek(0)
        content = file.read()
        file_hash = calculate_file_hash(content)
        
        logger.info(f"Checking for duplicates with hash: {file_hash}")
        
        table_client = get_table_client()
        version_entities = list_version_entities(table_client)
        is_duplicate = is_duplicate_hash(version_entities, file_hash)

        return jsonify({
            "status": "success",
            "isDuplicate": is_duplicate,
            "fileHash": file_hash,
            "message": "This file already exists!" if is_duplicate else "This is a new unique file"
        }), 200

    except Exception as exc:
        logger.error(f"Failed to check duplicate: {exc}")
        return jsonify({"error": f"Failed to check: {exc}"}), 500

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    logger.warning(f"404 Not Found: {request.path}")
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"500 Internal Server Error: {str(error)}")
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Flask Azure Storage Application Starting")
    logger.info("=" * 60)
    logger.info(f"Blob container: {BLOB_CONTAINER_NAME}")
    logger.info(f"Table name: {TABLE_NAME}")
    logger.info(f"Metadata partition key: {METADATA_PARTITION_KEY}")
    logger.info(f"Allowed file types: {', '.join(sorted(ALLOWED_EXTENSIONS))}")
    logger.info("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=9000)

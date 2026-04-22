"""
Test script for Flask Azure Storage API
Run this after starting the Flask app with: python app.py
"""

import requests
import json
from pathlib import Path

BASE_URL = "http://localhost:5000"

def test_health_check():
    """Test the health check endpoint"""
    print("\n=== Testing Health Check ===")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_upload_file():
    """Test file upload"""
    print("\n=== Testing File Upload ===")
    
    # Create a test file
    test_file_path = Path("test_file.txt")
    test_file_path.write_text("This is a test file for Azure Storage integration.")
    
    try:
        with open(test_file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(f"{BASE_URL}/upload", files=files)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Clean up
        test_file_path.unlink()
        
        return response.status_code in [200, 201]
    except Exception as e:
        print(f"Error: {e}")
        if test_file_path.exists():
            test_file_path.unlink()
        return False

def test_upload_invalid_file():
    """Test uploading a file with invalid extension"""
    print("\n=== Testing Invalid File Upload ===")
    
    # Create a test file with invalid extension
    test_file_path = Path("test_file.exe")
    test_file_path.write_text("Invalid file")
    
    try:
        with open(test_file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(f"{BASE_URL}/upload", files=files)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        # Clean up
        test_file_path.unlink()
        
        return response.status_code == 400
    except Exception as e:
        print(f"Error: {e}")
        if test_file_path.exists():
            test_file_path.unlink()
        return False

def test_get_files():
    """Test retrieving all files"""
    print("\n=== Testing Get Files ===")
    try:
        response = requests.get(f"{BASE_URL}/files")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 200
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_upload_no_file():
    """Test uploading without file"""
    print("\n=== Testing Upload Without File ===")
    try:
        response = requests.post(f"{BASE_URL}/upload")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 400
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_invalid_endpoint():
    """Test invalid endpoint"""
    print("\n=== Testing Invalid Endpoint ===")
    try:
        response = requests.get(f"{BASE_URL}/invalid")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.status_code == 404
    except Exception as e:
        print(f"Error: {e}")
        return False

def run_all_tests():
    """Run all tests"""
    print("\n" + "="*50)
    print("FLASK AZURE STORAGE API TEST SUITE")
    print("="*50)
    
    tests = [
        ("Health Check", test_health_check),
        ("Get Files", test_get_files),
        ("Upload File", test_upload_file),
        ("Upload No File Error", test_upload_no_file),
        ("Upload Invalid File", test_upload_invalid_file),
        ("Invalid Endpoint", test_invalid_endpoint),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except requests.exceptions.ConnectionError:
            print(f"\nError: Could not connect to Flask app at {BASE_URL}")
            print("Make sure to run 'python app.py' first!")
            return
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n❌ {total - passed} test(s) failed")

if __name__ == "__main__":
    run_all_tests()

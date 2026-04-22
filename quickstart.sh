#!/bin/bash

# Quick start script for Flask Azure Storage API

echo "================================================"
echo "Flask Azure Storage API - Quick Start"
echo "================================================"

# Check Python version
echo -e "\n[1/5] Checking Python version..."
python3 --version || { echo "Python 3 not found. Please install Python 3.8+"; exit 1; }

# Create virtual environment
echo -e "\n[2/5] Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo -e "\n[3/5] Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo -e "\n[4/5] Installing dependencies..."
pip install -r requirements.txt

# Instructions for next steps
echo -e "\n[5/5] Setup complete!"
echo ""
echo "================================================"
echo "NEXT STEPS:"
echo "================================================"
echo ""
echo "1. Create .env file with your Azure Storage connection string:"
echo "   cp .env.example .env"
echo "   # Edit .env with your connection string"
echo ""
echo "2. Create required Azure Storage resources:"
echo "   az storage container create --name files --connection-string \"YOUR_CONNECTION_STRING\""
echo "   az storage table create --name filemetadata --connection-string \"YOUR_CONNECTION_STRING\""
echo ""
echo "3. Start the Flask application:"
echo "   python app.py"
echo ""
echo "4. Test the API (in a new terminal):"
echo "   python test_api.py"
echo ""
echo "5. View the README for detailed API documentation:"
echo "   cat README.md"
echo ""
echo "================================================"

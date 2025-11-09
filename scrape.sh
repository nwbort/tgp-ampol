#!/bin/bash
set -e
set -o pipefail

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Running Python scraper..."
python3 scrape.py

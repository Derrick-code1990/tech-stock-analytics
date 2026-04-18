#!/usr/bin/env python3
"""
Validation script for tech-stock-analytics pipeline setup.
Run this before executing 'bruin run' to catch configuration issues early.
"""

import os
import sys
from pathlib import Path

print("\n" + "="*70)
print("TECH STOCK ANALYTICS - PIPELINE SETUP VALIDATION")
print("="*70 + "\n")

# Check 1: Required environment variables
print("[1/5] Checking environment variables...")
required_env = {
    "TIINGO_API_KEY": "Tiingo API credentials (required for data fetching)",
    "GCP_PROJECT_ID": "GCP project ID (defaults to 'tech-stock-analytics')",
}

env_status = True
for var, desc in required_env.items():
    value = os.getenv(var, "NOT SET")
    status = "[OK]" if value != "NOT SET" else "[X]"
    print(f"  {status} {var:20s} {desc}")
    if var == "TIINGO_API_KEY" and value == "NOT SET":
        env_status = False

if not env_status:
    print("\n  [X] CRITICAL: TIINGO_API_KEY is required!")
    print("     Export it: export TIINGO_API_KEY='your_api_key'")
else:
    print("\n  [OK] All required env vars configured")

# Check 2: Dependencies installed
print("\n[2/5] Checking Python dependencies...")
required_packages = [
    "tiingo",
    "pandas", 
    "pandas",
    "google.cloud.bigquery",
    "google.cloud.storage",
    "dotenv",
    "pyarrow",
]

missing = []
for pkg in required_packages:
    try:
        __import__(pkg)
        print(f"  [OK] {pkg:30s}")
    except ImportError:
        print(f"  [X] {pkg:30s}")
        missing.append(pkg)

if missing:
    print(f"\n  [X] Missing packages: {', '.join(missing)}")
    print("     Run: pip install -r requirements.txt")
else:
    print("\n  [OK] All dependencies installed")

# Check 3: File structure
print("\n[3/5] Checking project structure...")
required_files = [
    "pipeline.yml",
    "assets/ingest/raw_stock_prices.py",
    "assets/ingest/raw_stock_metadata.py",
    "requirements.txt",
]

files_ok = True
for filepath in required_files:
    full_path = Path(filepath)
    status = "[OK]" if full_path.exists() else "[X]"
    print(f"  {status} {filepath}")
    if not full_path.exists():
        files_ok = False

if not files_ok:
    print("\n  [X] Some required files are missing!")
else:
    print("\n  [OK] All required files present")

# Check 4: GCP credentials
print("\n[4/5] Checking GCP authentication...")
try:
    from google.auth import default
    from google.auth.exceptions import DefaultCredentialsError
    
    try:
        creds, project = default()
        print(f"  [OK] GCP credentials found")
        print(f"    Project: {project or os.getenv('GCP_PROJECT_ID', 'tech-stock-analytics')}")
    except DefaultCredentialsError:
        print(f"  [!] No GCP credentials found")
        print(f"    Run: gcloud auth application-default login")
        print(f"    Or set GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json")
except ImportError:
    print(f"  [!] google-auth not available (will be installed with google-cloud-bigquery)")

# Check 5: Network connectivity
print("\n[5/5] Checking network connectivity...")
import urllib.request
import urllib.error

endpoints = [
    ("https://api.tiingo.com", "Tiingo API"),
    ("https://www.googleapis.com", "Google APIs"),
]

for url, name in endpoints:
    try:
        urllib.request.urlopen(url, timeout=3)
        print(f"  [OK] {name:20s} reachable")
    except Exception as e:
        print(f"  [!] {name:20s} {str(e)[:40]}")

# Summary
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)

if env_status and files_ok and not missing:
    print("\n[OK] Setup looks good! Ready to run: bruin run --environment default\n")
    sys.exit(0)
else:
    print("\n[X] Please fix the issues above before running the pipeline.\n")
    sys.exit(1)

#!/usr/bin/env python3
"""
Quick connectivity test for Tiingo and GCP services.
Use this to verify your credentials work before running the full pipeline.
"""

import os
import sys

def test_tiingo():
    """Test Tiingo API connectivity"""
    print("\n[Tiingo API Test]")
    print("-" * 40)
    
    tiingo_key = os.getenv("TIINGO_API_KEY")
    if not tiingo_key:
        print("❌ TIINGO_API_KEY not set")
        return False
    
    print(f"✓ TIINGO_API_KEY found (length: {len(tiingo_key)})")
    
    try:
        from tiingo import TiingoClient
        print("✓ tiingo library imported")
        
        client = TiingoClient({"session": True, "api_key": tiingo_key})
        print("✓ TiingoClient initialized")
        
        # Try to fetch metadata for AAPL
        meta = client.get_ticker_metadata("AAPL")
        print(f"✓ Tiingo API working - Got metadata for {meta.get('ticker')}")
        print(f"  Company: {meta.get('name')}")
        print(f"  Exchange: {meta.get('exchangeCode')}")
        return True
        
    except Exception as e:
        print(f"❌ Tiingo test failed: {e}")
        return False


def test_gcp():
    """Test GCP BigQuery and Storage connectivity"""
    print("\n[GCP Authentication Test]")
    print("-" * 40)
    
    try:
        from google.auth import default
        from google.auth.exceptions import DefaultCredentialsError
        
        try:
            creds, project = default()
            print(f"✓ GCP credentials found")
            print(f"  Project: {project or os.getenv('GCP_PROJECT_ID', 'tech-stock-analytics')}")
            
            # Try to instantiate clients
            from google.cloud import bigquery, storage
            
            bq_project = os.getenv("GCP_PROJECT_ID", "tech-stock-analytics")
            bq_client = bigquery.Client(project=bq_project)
            print(f"✓ BigQuery client initialized for project '{bq_project}'")
            
            gcs_client = storage.Client(project=bq_project)
            print(f"✓ GCS client initialized for project '{bq_project}'")
            
            # Try to list datasets (minimal operation)
            try:
                datasets = list(bq_client.list_datasets(max_results=1))
                print(f"✓ BigQuery is accessible (found {bq_client.project})")
            except Exception as e:
                print(f"⚠ BigQuery reachable but datasets check failed: {e}")
            
            return True
            
        except DefaultCredentialsError:
            print("❌ No GCP credentials found")
            print("\n  To authenticate, choose one:")
            print("  1. gcloud auth application-default login")
            print("  2. export GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json")
            return False
            
    except ImportError as e:
        print(f"❌ google-cloud libraries not installed: {e}")
        return False
    except Exception as e:
        print(f"❌ GCP test failed: {e}")
        return False


def main():
    print("\n" + "="*40)
    print("SERVICE CONNECTIVITY TEST")
    print("="*40)
    
    tiingo_ok = test_tiingo()
    gcp_ok = test_gcp()
    
    print("\n" + "="*40)
    print("SUMMARY")
    print("="*40)
    
    if tiingo_ok and gcp_ok:
        print("\n✓ All services connected successfully!")
        print("  You can now run: bruin run --environment default\n")
        return 0
    else:
        print("\n✗ Some services are not available.")
        print("  Please fix the issues above and try again.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())

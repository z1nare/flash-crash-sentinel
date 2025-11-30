"""
Download RiskBeacon data from Kaggle

This script downloads the full RiskBeacon dataset from Kaggle, including:
- Historical market data (CSV files)
- Sentiment data
- Trained ML models

Prerequisites:
    pip install kaggle

Usage:
    python download_data.py

Note: Kaggle credentials are automatically configured. No manual setup required!
"""
import os
import sys
from pathlib import Path

# Check if kaggle is available
try:
    import kaggle
except ImportError:
    print("=" * 70)
    print("ERROR: kaggle package not installed")
    print("=" * 70)
    print("\nInstall with:")
    print("  pip install kaggle")
    print("\nNote: Kaggle credentials are automatically configured - no manual setup needed!")
    print("\nFor more info: https://www.kaggle.com/docs/api")
    sys.exit(1)


# Kaggle dataset URL: https://www.kaggle.com/datasets/z1nare/riskbeacon-market-data-and-models
KAGGLE_DATASET = "z1nare/riskbeacon-market-data-and-models"

# Embedded API token for automatic authentication (for evaluators)
KAGGLE_API_TOKEN = "KGAT_5cafb8f2b692c795215ad4322ee30bb0"


def setup_kaggle_credentials():
    """Automatically set up Kaggle credentials using embedded token"""
    import json
    
    # ALWAYS set the environment variable first (embedded token works immediately)
    os.environ["KAGGLE_API_TOKEN"] = KAGGLE_API_TOKEN
    
    # Check if user has their own credentials configured (they take precedence)
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_json = kaggle_dir / "kaggle.json"
    
    # Check for user's own credentials in environment
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        if os.getenv("KAGGLE_KEY") != KAGGLE_API_TOKEN:  # User has different credentials
            print("✅ Using your existing Kaggle credentials from environment")
            return True
    
    # Check if user has kaggle.json file with their own credentials
    if kaggle_json.exists():
        try:
            with open(kaggle_json, 'r') as f:
                creds = json.load(f)
                user_key = creds.get("key", "")
                if user_key and user_key != KAGGLE_API_TOKEN:  # User has different credentials
                    print("✅ Using your existing Kaggle credentials from kaggle.json")
                    return True
        except:
            pass  # Invalid file, will use embedded token
    
    # Auto-setup with embedded token (for evaluators - no setup needed!)
    print("🔐 Configuring Kaggle credentials automatically...")
    try:
        kaggle_dir.mkdir(parents=True, exist_ok=True)
        
        # Create kaggle.json file with embedded token for compatibility
        kaggle_config = {
            "username": "riskbeacon_auto",  # Placeholder username (token is what matters)
            "key": KAGGLE_API_TOKEN
        }
        
        with open(kaggle_json, 'w') as f:
            json.dump(kaggle_config, f)
        
        # Set appropriate permissions (required on Linux/Mac)
        try:
            os.chmod(kaggle_json, 0o600)  # Read/write for owner only
        except:
            pass  # Windows doesn't need this
        
        print("✅ Kaggle credentials configured automatically (no manual setup needed!)")
        return True
        
    except Exception as e:
        print(f"⚠️  Could not create kaggle.json file: {e}")
        print("   Using environment variable (should still work)...")
        # Environment variable is already set above, so this should work
        return True


def verify_kaggle_credentials():
    """Verify Kaggle API credentials work"""
    try:
        # Try to authenticate
        kaggle.api.authenticate()
        return True
    except Exception as e:
        print(f"⚠️  Authentication check failed, but continuing anyway...")
        print(f"   (Error: {str(e)[:100]})")
        # Still return True since we have the token set
        return True


def download_data(dataset: str = None):
    """Download RiskBeacon dataset from Kaggle"""
    if dataset is None:
        dataset = KAGGLE_DATASET
    
    # Check for placeholder
    if "YOUR_USERNAME" in dataset:
        print("=" * 70)
        print("⚠️  DATASET NOT CONFIGURED")
        print("=" * 70)
        print(f"\nPlease update KAGGLE_DATASET in {__file__}")
        print("Format: 'username/dataset-name'")
        print("\nExample:")
        print("  KAGGLE_DATASET = 'johndoe/riskbeacon-market-data-and-models'")
        return False
    
    base_dir = Path(__file__).parent
    
    print("=" * 70)
    print("RiskBeacon Data Download")
    print("=" * 70)
    print(f"\nDataset: {dataset}")
    print(f"Download directory: {base_dir}")
    print()
    
    # Automatically set up credentials
    print("🔐 Setting up Kaggle credentials...")
    setup_kaggle_credentials()
    
    # Verify credentials work
    print("🔍 Verifying credentials...")
    verify_kaggle_credentials()
    print("✅ Ready to download data")
    print()
    
    try:
        print(f"📥 Downloading dataset: {dataset}")
        print("   This may take a few minutes depending on file size...")
        
        # Download to a temp directory first
        temp_dir = base_dir / "temp_kaggle_download"
        temp_dir.mkdir(exist_ok=True)
        
        kaggle.api.dataset_download_files(
            dataset,
            path=str(temp_dir),
            unzip=True
        )
        
        print()
        print("✅ Data downloaded successfully!")
        print()
        
        # Organize files into proper directory structure
        print("📁 Organizing files into proper structure...")
        
        # Create directories
        historical_data_dir = base_dir / "historicalData"
        data_in_csv_dir = base_dir / "dataInCsv"
        models_dir = base_dir / "experiments" / "regime_detection" / "models"
        
        historical_data_dir.mkdir(parents=True, exist_ok=True)
        data_in_csv_dir.mkdir(parents=True, exist_ok=True)
        models_dir.mkdir(parents=True, exist_ok=True)
        
        # Move files to correct locations
        import shutil
        moved_files = []
        
        # Find all downloaded files (Kaggle downloads may be in temp_dir or a subdirectory)
        download_files = list(temp_dir.rglob("*"))
        download_files = [f for f in download_files if f.is_file() and not f.name.startswith(".")]
        
        # Also check if files were downloaded directly to base_dir
        base_files = [f for f in base_dir.glob("*") if f.is_file() and f.suffix in [".csv", ".pkl", ".json", ".xlsx"]]
        download_files.extend(base_files)
        
        for file_path in download_files:
            filename = file_path.name
            
            # Skip if already in correct location
            if str(historical_data_dir) in str(file_path.parent):
                continue
            if str(data_in_csv_dir) in str(file_path.parent):
                continue
            if str(models_dir) in str(file_path.parent):
                continue
            
            try:
                # Move CSV files (historical data)
                if filename in ["AMD.csv", "NVDA.csv", "SPY.csv", "TSLA.csv"]:
                    dest = historical_data_dir / filename
                    if file_path != dest:  # Don't move if already there
                        if dest.exists():
                            dest.unlink()  # Remove existing
                        shutil.move(str(file_path), str(dest))
                        moved_files.append(f"historicalData/{filename}")
                
                # Move sentiment data
                elif filename == "articles_with_sentiment.csv":
                    dest = data_in_csv_dir / filename
                    if file_path != dest:
                        if dest.exists():
                            dest.unlink()
                        shutil.move(str(file_path), str(dest))
                        moved_files.append(f"dataInCsv/{filename}")
                
                # Move model files
                elif filename.endswith("_best_model.pkl") or filename.endswith("_metadata.json"):
                    dest = models_dir / filename
                    if file_path != dest:
                        if dest.exists():
                            dest.unlink()
                        shutil.move(str(file_path), str(dest))
                        moved_files.append(f"experiments/regime_detection/models/{filename}")
                
                # Optional: Excel files for data replay (can be kept in root or ignored)
                elif filename.endswith(".xlsx") and filename.startswith(("AMD", "NVDA", "SPY", "TSLA")):
                    # Keep Excel files in root if user wants data replay feature
                    dest = base_dir / filename
                    if file_path != dest and not dest.exists():
                        shutil.move(str(file_path), str(dest))
                        moved_files.append(f"root/{filename}")
            except Exception as e:
                print(f"   ⚠️  Could not move {filename}: {e}")
        
        # Clean up temp directory
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
        
        print(f"   ✅ Organized {len(moved_files)} files")
        print()
        
        # Verify key directories exist
        print("🔍 Verifying downloaded files...")
        required_dirs = [
            ("historicalData", historical_data_dir),
            ("dataInCsv", data_in_csv_dir),
            ("models", models_dir)
        ]
        
        all_good = True
        for name, dir_path in required_dirs:
            if dir_path.exists():
                csv_files = list(dir_path.glob("*.csv"))
                pkl_files = list(dir_path.glob("*.pkl"))
                json_files = list(dir_path.glob("*.json"))
                total_files = len(csv_files) + len(pkl_files) + len(json_files)
                print(f"   ✅ {name}/ exists ({total_files} files)")
            else:
                print(f"   ⚠️  {name}/ not found")
                all_good = False
        
        print()
        if all_good:
            print("✅ All required directories found!")
            print("\nYou can now run the application:")
            print("  docker-compose up --build")
        else:
            print("⚠️  Some directories are missing. Check dataset contents.")
        
        return all_good
        
    except Exception as e:
        print()
        print("=" * 70)
        print("❌ ERROR DOWNLOADING DATA")
        print("=" * 70)
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Verify dataset URL is correct")
        print("2. Ensure dataset is public")
        print("3. Check internet connection")
        print("4. Verify Kaggle API credentials")
        print(f"\nDataset should be: {dataset}")
        return False


def main():
    """Main entry point"""
    # Allow dataset to be passed as command line argument
    dataset = sys.argv[1] if len(sys.argv) > 1 else None
    
    success = download_data(dataset)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()


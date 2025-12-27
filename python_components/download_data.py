"""
Download Flash Crash Sentinel data from Kaggle.

This repository is intended to be safe to publish publicly, so this script does
NOT embed credentials and does NOT write any credential files to disk.

Prerequisites:
  - Install: pip install kaggle
  - Configure Kaggle auth (one of):
      - Set env vars: KAGGLE_USERNAME and KAGGLE_KEY
      - Create local file: ~/.kaggle/kaggle.json  (DO NOT COMMIT IT)

Usage:
  python download_data.py
"""
import sys
from pathlib import Path

# Import kaggle (will use user-provided credentials)
try:
    import kaggle
except ImportError:
    print("=" * 70)
    print("ERROR: kaggle package not installed")
    print("=" * 70)
    print("\nInstall with:")
    print("  pip install kaggle")
    print("\nNote: Configure Kaggle credentials locally (env vars or ~/.kaggle/kaggle.json).")
    print("\nFor more info: https://www.kaggle.com/docs/api")
    sys.exit(1)

# Kaggle dataset name (public): z1nare/riskbeacon-market-data-and-models
KAGGLE_DATASET = "z1nare/riskbeacon-market-data-and-models"


def verify_kaggle_credentials():
    """Verify Kaggle API credentials work"""
    try:
        # Try to authenticate
        kaggle.api.authenticate()
        return True
    except Exception as e:
        print("⚠️  Kaggle authentication failed.")
        print("   Configure credentials via env vars (KAGGLE_USERNAME/KAGGLE_KEY) or ~/.kaggle/kaggle.json.")
        print(f"   (Error: {str(e)[:160]})")
        return False


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
    
    # Verify credentials work
    print("🔍 Verifying credentials...")
    if not verify_kaggle_credentials():
        return False
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


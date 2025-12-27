#!/usr/bin/env python3
"""
Cleanup script to remove unnecessary files before submission.
Keeps only essential files needed for the submission.
"""

import os
import shutil
from pathlib import Path

# Files/directories to DELETE
UNNECESSARY_FILES = [
    # Documentation files (development guides)
    "*.md",
    "!README.md",  # Keep README.md
    
    # Logs
    "logs/",
    
    # Cache and build artifacts
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".pytest_cache/",
    ".mypy_cache/",
    ".coverage",
    "htmlcov/",
    
    # IDE files
    ".vscode/",
    ".idea/",
    "*.swp",
    "*.swo",
    
    # Temporary files
    "*.log",
    "*.tmp",
    ".DS_Store",
    "*.bak",
    
    # Build artifacts (will rebuild from source)
    "build/",
    "dist/",
    "*.egg-info/",
    
    # Test files and data
    "tests/",
    
    # Development scripts
    "run_api.py",
    "run_dashboard.py",
    
    # VM-specific files
    "QUICK_INSTALL.sh",
    "INSTALL_DOCKER.sh",
    "VM_*.md",
    "VM_*.txt",
    "*VM*.md",
    "CLEAN_AND_REBUILD.md",
    "VM_DISK_SPACE_FIX.md",
    "VM_QUICK_FIX.md",
    "VM_FRESH_SETUP.md",
    "VM_SETUP_GUIDE.md",
    "DOCKER_PERMISSIONS_FIX.md",
    "BUILD_TIME_OPTIMIZATION.md",
    "FIRST_BUILD_OPTIMIZATION.md",
    "README_FAST_BUILD.md",
    "QUICK_START_VM.md",
    "ARCHITECTURE_DIAGRAM_GUIDE.md",
    "FINBERT_ML_IMAGES_GUIDE.md",
    "QUICK_IMAGE_REFERENCES.md",
    "CLEAN_AND_REBUILD.md",
    "safe_cleanup.sh",
    "find_large_files.sh",
    "clean_docker.sh",
    "clean_docker.ps1",
    "FREE_DISK_SPACE.sh",
    "build_fast.sh",
    "build_fast.bat",
    
    # Development data (data will be downloaded from Kaggle)
    "historicalData/*.csv",
    "dataInCsv/*.csv",
    
    # Generated plots (will be regenerated)
    "plots/*.html",
    
    # Report development files
    "../report/*.md",
    "../report/main_optimized.tex",
    
    # Root level documentation
    "../FILES_TO_UPDATE_ON_VM.md",
    "../UPDATE_THESE_FILES_ON_VM.txt",
]

# Directories to process
BASE_DIR = Path(__file__).parent
REPORT_DIR = BASE_DIR.parent / "report"

def delete_pattern(pattern, directory):
    """Delete files/directories matching pattern."""
    deleted = []
    
    if pattern.endswith("/"):
        # Directory pattern
        dir_name = pattern.rstrip("/")
        dir_path = directory / dir_name
        if dir_path.exists() and dir_path.is_dir():
            try:
                shutil.rmtree(dir_path)
                deleted.append(f"Directory: {dir_path.relative_to(BASE_DIR)}")
            except Exception as e:
                print(f"  ⚠️  Could not delete {dir_path}: {e}")
    else:
        # File pattern
        if "*" in pattern:
            # Glob pattern
            for path in directory.rglob(pattern):
                if path.exists():
                    try:
                        if path.is_file():
                            path.unlink()
                            deleted.append(f"File: {path.relative_to(BASE_DIR)}")
                        elif path.is_dir():
                            shutil.rmtree(path)
                            deleted.append(f"Directory: {path.relative_to(BASE_DIR)}")
                    except Exception as e:
                        print(f"  ⚠️  Could not delete {path}: {e}")
        else:
            # Specific file
            file_path = directory / pattern
            if file_path.exists():
                try:
                    file_path.unlink()
                    deleted.append(f"File: {file_path.relative_to(BASE_DIR)}")
                except Exception as e:
                    print(f"  ⚠️  Could not delete {file_path}: {e}")
    
    return deleted

def cleanup():
    """Main cleanup function."""
    print("=" * 60)
    print("RiskBeacon Submission Cleanup")
    print("=" * 60)
    print()
    
    all_deleted = []
    
    # Clean python_components directory
    print("🧹 Cleaning python_components/...")
    for pattern in UNNECESSARY_FILES:
        if pattern.startswith("../"):
            continue  # Handle separately
        deleted = delete_pattern(pattern, BASE_DIR)
        all_deleted.extend(deleted)
    
    # Clean report directory (markdown files and optimized version)
    print("\n🧹 Cleaning report/...")
    report_patterns = [
        "*.md",
        "main_optimized.tex",
    ]
    for pattern in report_patterns:
        deleted = delete_pattern(pattern, REPORT_DIR)
        all_deleted.extend(deleted)
    
    # Clean root directory
    print("\n🧹 Cleaning root directory...")
    root_patterns = [
        "FILES_TO_UPDATE_ON_VM.md",
        "UPDATE_THESE_FILES_ON_VM.txt",
    ]
    for pattern in root_patterns:
        deleted = delete_pattern(pattern, BASE_DIR.parent)
        all_deleted.extend(deleted)
    
    # Remove __pycache__ directories recursively
    print("\n🧹 Removing __pycache__ directories...")
    for pycache_dir in BASE_DIR.rglob("__pycache__"):
        try:
            shutil.rmtree(pycache_dir)
            all_deleted.append(f"Directory: {pycache_dir.relative_to(BASE_DIR)}")
        except Exception as e:
            print(f"  ⚠️  Could not delete {pycache_dir}: {e}")
    
    print()
    print("=" * 60)
    print("✅ Cleanup Complete!")
    print("=" * 60)
    print(f"\nDeleted {len(all_deleted)} items")
    print("\nFiles/Directories removed:")
    for item in all_deleted[:20]:  # Show first 20
        print(f"  - {item}")
    if len(all_deleted) > 20:
        print(f"  ... and {len(all_deleted) - 20} more items")
    print()
    print("✅ Ready for submission!")

if __name__ == "__main__":
    cleanup()


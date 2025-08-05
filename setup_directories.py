#!/usr/bin/env python3
"""
Standalone directory setup for MLB betting analysis
Creates all necessary directories for the project
"""
import sys
import os
from pathlib import Path
from datetime import datetime

def create_project_directories():
    """Create all project directories with error handling"""
    
    # Directory structure definition
    directories = {
        # Core data directories (REQUIRED)
        'stage': 'Data staging area for parquet files',
        'logs': 'Application and error logs',
        
        # Code directories (REQUIRED)
        'py': 'Main Python package',
        'loader': 'Data loading utilities',
        'migrations': 'Database schema migrations',
        
        # Optional directories (RECOMMENDED)
        'tests': 'Unit and integration tests',
        'docs': 'Project documentation',
        'backup': 'Data backup storage',
        '.github/workflows': 'CI/CD workflows',
        
        # Additional useful directories
        'scripts': 'Utility and maintenance scripts',
        'config': 'Configuration files and templates'
    }
    
    print("🏗️  MLB Betting Analysis - Directory Setup")
    print("=" * 50)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    created = []
    existed = []
    failed = []
    
    for directory, description in directories.items():
        dir_path = Path(directory)
        
        try:
            if dir_path.exists():
                if dir_path.is_dir():
                    existed.append(directory)
                    print(f"✅ {directory:<20} - {description}")
                else:
                    failed.append(f"{directory} exists but is not a directory")
                    print(f"❌ {directory:<20} - EXISTS BUT NOT A DIRECTORY!")
                    print(f"   💡 Please remove the file '{directory}' and run again")
            else:
                dir_path.mkdir(parents=True, exist_ok=True)
                created.append(directory)
                print(f"📂 {directory:<20} - Created: {description}")
                
        except PermissionError:
            failed.append(f"{directory}: Permission denied")
            print(f"❌ {directory:<20} - PERMISSION DENIED")
            print(f"   💡 Try running with elevated permissions or:")
            print(f"      sudo mkdir -p {directory}")
            print(f"      sudo chown $USER:$USER {directory}")
        except Exception as e:
            failed.append(f"{directory}: {str(e)}")
            print(f"❌ {directory:<20} - ERROR: {e}")
    
    # Create essential files in directories
    essential_files = {
        'py/__init__.py': '"""MLB Betting Analysis Package"""\n__version__ = "1.0.0"\n',
        'logs/.gitkeep': '# Keep this directory in git\n',
        'stage/.gitkeep': '# Keep this directory in git\n',
        'tests/__init__.py': '"""Test package"""\n',
        'docs/README.md': '# MLB Betting Analysis Documentation\n\nThis directory contains project documentation.\n'
    }
    
    print(f"\n📄 Creating essential files...")
    files_created = []
    files_existed = []
    files_failed = []
    
    for file_path, content in essential_files.items():
        file_obj = Path(file_path)
        
        # Only create file if directory exists
        if not file_obj.parent.exists():
            continue
            
        try:
            if file_obj.exists():
                files_existed.append(file_path)
                print(f"   ✅ {file_path}")
            else:
                file_obj.write_text(content)
                files_created.append(file_path)
                print(f"   📝 Created: {file_path}")
        except Exception as e:
            files_failed.append(f"{file_path}: {e}")
            print(f"   ❌ Failed to create {file_path}: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 Directory Setup Summary:")
    print(f"   📂 Created: {len(created)} directories")
    print(f"   ✅ Existed: {len(existed)} directories")
    print(f"   ❌ Failed: {len(failed)} directories")
    print(f"   📝 Files created: {len(files_created)}")
    print(f"   📄 Files existed: {len(files_existed)}")
    
    if created:
        print(f"\n🎉 Successfully created directories:")
        for directory in created:
            print(f"   • {directory}")
    
    if files_created:
        print(f"\n📝 Created essential files:")
        for file_path in files_created:
            print(f"   • {file_path}")
    
    if failed:
        print(f"\n⚠️  Failed to create:")
        for error in failed:
            print(f"   • {error}")
        print(f"\n💡 Solutions:")
        print(f"   • Check file permissions")
        print(f"   • Run with elevated permissions if needed")
        print(f"   • Remove any conflicting files manually")
        print(f"   • Run: sudo chown -R $USER:$USER .")
        return False
    
    if files_failed:
        print(f"\n⚠️  Failed to create files:")
        for error in files_failed:
            print(f"   • {error}")
    
    # Provide next steps
    print(f"\n🚀 Next Steps:")
    if not Path('.env').exists():
        print(f"   1. Set up environment: python setup_env.py")
    else:
        print(f"   1. Environment exists: ✅")
    
    if not Path('py/requirements.txt').exists():
        print(f"   2. Install dependencies: pip install -r py/requirements.txt")
    else:
        print(f"   2. Install dependencies: pip install -r py/requirements.txt")
    
    print(f"   3. Initialize database: python initialize_database.py")
    print(f"   4. Check environment: python check_env.py")
    print(f"   5. Run test backfill: python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
    
    print(f"\n✅ Directory setup complete!")
    return len(failed) == 0

def check_current_directory():
    """Check if we're in the right directory"""
    cwd = Path.cwd()
    
    # Look for indicators that this is the MLB project root
    indicators = [
        'py/config.py',
        'migrations/001_enhanced_simple_schema.sql',
        'loader/enhanced_load_parquet_into_pg.py'
    ]
    
    missing_indicators = []
    for indicator in indicators:
        if not Path(indicator).exists():
            missing_indicators.append(indicator)
    
    if missing_indicators:
        print(f"⚠️  Warning: Some expected files are missing:")
        for indicator in missing_indicators:
            print(f"   • {indicator}")
        print(f"\n💡 Make sure you're running this from the MLB project root directory")
        print(f"   Current directory: {cwd}")
        
        response = input(f"\nContinue anyway? (y/N): ").strip().lower()
        if response != 'y':
            print(f"❌ Setup cancelled")
            return False
    
    return True

def main():
    """Main setup function"""
    print("📁 MLB Betting Analysis - Standalone Directory Setup")
    print("=" * 55)
    
    # Check if we're in the right place
    if not check_current_directory():
        sys.exit(1)
    
    # Create directories
    success = create_project_directories()
    
    if success:
        print(f"\n🎉 All directories created successfully!")
        
        # Optional: Create a .gitignore if it doesn't exist
        gitignore_path = Path('.gitignore')
        if not gitignore_path.exists():
            gitignore_content = """# MLB Betting Analysis .gitignore

# Data files
*.parquet
*.csv
stage/
backup/
logs/

# Environment and secrets
.env
.env.local
.env.production

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Database
*.db
*.sqlite
*.sqlite3

# AWS
.aws/

# Temporary files
*.tmp
*.temp
"""
            try:
                gitignore_path.write_text(gitignore_content)
                print(f"📝 Created .gitignore file")
            except Exception as e:
                print(f"⚠️  Could not create .gitignore: {e}")
        
        sys.exit(0)
    else:
        print(f"\n❌ Some directories failed to create")
        print(f"   Check the errors above and fix them before proceeding")
        sys.exit(1)

if __name__ == "__main__":
    main()
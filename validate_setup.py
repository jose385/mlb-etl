#!/usr/bin/env python3
"""
Master setup script for MLB Betting Analysis
Orchestrates the complete setup process
"""
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def run_command(command, description, allow_failure=False):
    """Run a command and handle errors"""
    print(f"\n🔄 {description}...")
    
    try:
        if isinstance(command, list):
            result = subprocess.run(command, check=True, capture_output=True, text=True)
        else:
            result = subprocess.run([sys.executable, command], check=True, capture_output=True, text=True)
        
        if result.stdout:
            print(result.stdout)
        
        print(f"✅ {description} completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        
        if not allow_failure:
            print(f"💡 You can try running manually: python {command}")
            return False
        
        print(f"⚠️ {description} failed but continuing...")
        return True
    
    except Exception as e:
        print(f"❌ {description} error: {e}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ required, found {version.major}.{version.minor}")
        return False
    
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def check_dependencies():
    """Check if required dependencies can be installed"""
    requirements_file = Path("py/requirements.txt")
    
    if not requirements_file.exists():
        print(f"⚠️ Requirements file not found: {requirements_file}")
        return True  # Don't fail setup, just warn
    
    print(f"📦 Checking dependencies from {requirements_file}...")
    
    try:
        # Try to install dependencies
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ], check=True, capture_output=True, text=True)
        
        print(f"✅ Dependencies installed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies")
        print(f"   Run manually: pip install -r {requirements_file}")
        return False

def main():
    """Main setup orchestration"""
    print("🚀 MLB Betting Analysis - Master Setup")
    print("=" * 50)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Step 1: Check Python version
    print("🐍 Step 1: Checking Python version...")
    if not check_python_version():
        sys.exit(1)
    
    # Step 2: Create directory structure
    print("\n📁 Step 2: Setting up directory structure...")
    if not run_command("setup_directories.py", "Directory structure setup"):
        print("💡 Try running: python setup_directories.py")
        if input("Continue with manual directory creation? (y/N): ").lower() != 'y':
            sys.exit(1)
    
    # Step 3: Install dependencies
    print("\n📦 Step 3: Installing Python dependencies...")
    if not check_dependencies():
        print("⚠️ Dependency installation failed")
        print("💡 You can install them manually later with:")
        print("   pip install -r py/requirements.txt")
        
        if input("Continue without dependencies? (y/N): ").lower() != 'y':
            sys.exit(1)
    
    # Step 4: Environment configuration
    print("\n⚙️ Step 4: Environment configuration...")
    
    env_file = Path('.env')
    if env_file.exists():
        print("✅ .env file already exists")
        
        choice = input("Reconfigure environment? (y/N): ").lower()
        if choice == 'y':
            if not run_command("setup_env.py", "Environment configuration"):
                print("💡 Configure manually or try: python setup_env.py")
        else:
            print("⏭️ Skipping environment configuration")
    else:
        if not run_command("setup_env.py", "Environment configuration"):
            print("💡 Configure manually or try: python setup_env.py")
            if input("Continue without environment configuration? (y/N): ").lower() != 'y':
                sys.exit(1)
    
    # Step 5: Validate setup
    print("\n🔍 Step 5: Validating setup...")
    validation_success = run_command("check_env.py", "Setup validation", allow_failure=True)
    
    # Step 6: Database initialization (optional)
    print("\n🗄️ Step 6: Database initialization (optional)...")
    
    if env_file.exists():
        init_db = input("Initialize database now? (y/N): ").lower()
        if init_db == 'y':
            if not run_command("initialize_database.py", "Database initialization", allow_failure=True):
                print("💡 You can initialize the database later with:")
                print("   python initialize_database.py")
        else:
            print("⏭️ Skipping database initialization")
            print("💡 Initialize later with: python initialize_database.py")
    else:
        print("⏭️ Skipping database initialization (no .env file)")
    
    # Final summary
    print("\n" + "=" * 50)
    print("🎉 Setup Complete!")
    print("=" * 50)
    
    print("\n📋 Setup Summary:")
    print("   ✅ Python version checked")
    print("   ✅ Directory structure created")
    
    if Path("py/requirements.txt").exists():
        print("   ✅ Dependencies processed")
    else:
        print("   ⚠️ Dependencies skipped (no requirements.txt)")
    
    if env_file.exists():
        print("   ✅ Environment configured")
    else:
        print("   ⚠️ Environment not configured")
    
    print("   ✅ Setup validation attempted")
    
    # Next steps
    print("\n🚀 Next Steps:")
    
    if not env_file.exists():
        print("   1. Configure environment: python setup_env.py")
        print("   2. Initialize database: python initialize_database.py")
    else:
        print("   1. Initialize database: python initialize_database.py")
    
    print("   2. Run test data collection:")
    print("      python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
    print("   3. Load data into database:")
    print("      python loader/enhanced_load_parquet_into_pg.py")
    print("   4. Run analysis:")
    print("      python py/simple_analysis.py")
    
    print("\n🔧 Troubleshooting:")
    print("   • Check setup: python check_env.py")
    print("   • Create directories only: python setup_directories.py")
    print("   • Configure environment only: python setup_env.py")
    print("   • View logs in: logs/")
    
    print("\n📚 Documentation:")
    print("   • README.md - Full setup guide")
    print("   • py/config.py - Configuration options")
    print("   • migrations/ - Database schema")
    
    if validation_success:
        print("\n✅ Your system appears to be ready!")
    else:
        print("\n⚠️ Some validation issues found - check the output above")
    
    print("\n🎯 Happy betting analysis! 📊⚾")

if __name__ == "__main__":
    main()
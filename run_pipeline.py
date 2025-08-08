#!/usr/bin/env python3
"""
run_pipeline.py - One-command MLB pipeline runner
Runs the complete pipeline: backfill → load → analyze in one command
Perfect for daily automated runs or quick testing
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
from datetime import datetime, timedelta

def run_step(cmd, description, show_output=False):
    """Run a pipeline step with proper error handling"""
    print(f"\n🔄 {description}")
    print(f"   $ {' '.join(cmd)}")
    
    start_time = time.time()
    
    try:
        if show_output:
            # Stream output in real-time
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                     universal_newlines=True, bufsize=1)
            
            for line in process.stdout:
                print(f"   {line.rstrip()}")
            
            process.wait()
            result_code = process.returncode
            elapsed = time.time() - start_time
            
        else:
            # Capture output and show summary
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            result_code = result.returncode
            elapsed = time.time() - start_time
            
            if result_code == 0:
                # Show last few lines of successful output
                if result.stdout:
                    lines = result.stdout.strip().split('\n')
                    for line in lines[-2:]:
                        if line.strip() and ('✅' in line or '🎉' in line or 'Success' in line):
                            print(f"   {line.strip()}")
            else:
                # Show error output
                if result.stderr:
                    print(f"   ❌ Error: {result.stderr.strip()}")
                if result.stdout:
                    print(f"   Output: {result.stdout.strip()}")
        
        if result_code == 0:
            print(f"   ✅ Completed in {elapsed:.1f}s")
            return True
        else:
            print(f"   ❌ Failed with exit code {result_code}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ Timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"   💥 Exception: {e}")
        return False

def check_prerequisites():
    """Check if system is ready to run pipeline"""
    print("🔍 Checking prerequisites...")
    
    issues = []
    
    # Check Python version
    if sys.version_info < (3, 8):
        issues.append("Python 3.8+ required")
    
    # Check critical files exist
    critical_files = [
        "py/enhanced_simple_backfill.py",
        "loader/enhanced_load_parquet_into_pg.py", 
        "py/simple_analysis.py",
        "py/config.py",
        ".env"
    ]
    
    for file in critical_files:
        if not Path(file).exists():
            issues.append(f"Missing file: {file}")
    
    # Check configuration
    try:
        from py.config import get_config
        config = get_config()
        
        if not config.PG_DSN:
            issues.append("PG_DSN not configured in .env")
        
        # Test database connection
        success, message = config.test_database_connection()
        if not success:
            issues.append(f"Database connection failed: {message}")
        
    except Exception as e:
        issues.append(f"Configuration error: {e}")
    
    if issues:
        print("❌ Prerequisites check failed:")
        for issue in issues:
            print(f"   • {issue}")
        print("\n💡 Fixes:")
        print("   1. Make sure all files are updated")
        print("   2. Run: pip install -r py/requirements.txt")
        print("   3. Configure .env file with valid PG_DSN")
        print("   4. Run: python initialize_database.py")
        return False
    else:
        print("✅ Prerequisites check passed")
        return True

def show_configuration():
    """Show current configuration"""
    try:
        from py.config import get_config
        config = get_config()
        
        placeholder_mode = getattr(config, 'USE_PLACEHOLDER_DATA', True)
        mode = "PLACEHOLDER" if placeholder_mode else "REAL DATA"
        
        print(f"\n⚙️ Current Configuration:")
        print(f"   🔧 Mode: {mode}")
        print(f"   🗄️ Database: {'✅' if config.PG_DSN else '❌'}")
        print(f"   🌤️ Weather API: {'✅' if config.OPENWEATHER_API_KEY else '❌'}")
        print(f"   📁 Output: {config.OUTPUT_DIR}")
        
        if placeholder_mode:
            print(f"   💡 Using fast placeholder data - perfect for testing!")
        else:
            print(f"   🌐 Using real MLB APIs - slower but real data")
            
    except Exception as e:
        print(f"⚠️ Could not show configuration: {e}")

def main():
    parser = argparse.ArgumentParser(description="Run complete MLB betting analysis pipeline")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD, defaults to start date)")
    parser.add_argument("--output-dir", default="stage", help="Output directory")
    parser.add_argument("--skip-init", action="store_true", help="Skip database initialization")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip final analysis")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output")
    parser.add_argument("--quick-test", action="store_true", help="Run quick test with today's date")
    
    args = parser.parse_args()
    
    print("🚀 MLB Betting Analysis - Pipeline Runner")
    print("=" * 50)
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Quick test mode
    if args.quick_test:
        args.start = "2025-01-15"  # Good date for placeholder data
        args.end = "2025-01-15"
        print(f"🧪 Quick test mode: using {args.start}")
    
    # Set end date if not provided
    if not args.end:
        args.end = args.start
    
    # Check prerequisites
    if not check_prerequisites():
        return False
    
    # Show configuration
    show_configuration()
    
    print(f"\n🎯 Pipeline Plan:")
    print(f"   📅 Date range: {args.start} to {args.end}")
    print(f"   📁 Output: {args.output_dir}")
    
    if not args.skip_init:
        print(f"   1. Initialize database schema")
    print(f"   2. Run data backfill")
    print(f"   3. Load data into database")
    if not args.skip_analysis:
        print(f"   4. Run betting analysis")
    
    # Confirm for real data mode
    try:
        from py.config import get_config
        config = get_config()
        if not getattr(config, 'USE_PLACEHOLDER_DATA', True):
            print(f"\n⚠️ REAL DATA MODE - This will make actual API calls")
            response = input("Continue? (y/N): ").strip().lower()
            if response not in ['y', 'yes']:
                print("❌ Cancelled by user")
                return False
    except:
        pass
    
    print(f"\n🚀 Starting pipeline...")
    
    # Track results
    results = {}
    start_time = time.time()
    
    # Step 1: Initialize database (optional)
    if not args.skip_init:
        init_cmd = [sys.executable, "initialize_database.py"]
        results['init'] = run_step(init_cmd, "Initialize database schema", show_output=args.verbose)
        
        if not results['init']:
            print(f"\n❌ Database initialization failed")
            print(f"💡 Try running manually: python initialize_database.py")
            return False
    
    # Step 2: Run backfill
    backfill_cmd = [
        sys.executable, "py/enhanced_simple_backfill.py",
        "--start", args.start,
        "--end", args.end,
        "--output", args.output_dir
    ]
    
    results['backfill'] = run_step(backfill_cmd, f"Backfill data ({args.start} to {args.end})", 
                                  show_output=args.verbose)
    
    if not results['backfill']:
        print(f"\n❌ Data backfill failed")
        print(f"💡 Check your configuration and try again")
        return False
    
    # Step 3: Load data
    load_cmd = [
        sys.executable, "loader/enhanced_load_parquet_into_pg.py",
        "--input-dir", args.output_dir,
        "--validate-schema"
    ]
    
    results['load'] = run_step(load_cmd, "Load data into database", show_output=args.verbose)
    
    if not results['load']:
        print(f"\n❌ Data loading failed")
        print(f"💡 Check database connection and schema")
        return False
    
    # Step 4: Run analysis (optional)
    if not args.skip_analysis:
        analysis_cmd = [sys.executable, "py/simple_analysis.py"]
        results['analysis'] = run_step(analysis_cmd, "Run betting analysis", show_output=True)
        
        if not results['analysis']:
            print(f"\n⚠️ Analysis failed, but data is loaded successfully")
    
    # Summary
    total_time = time.time() - start_time
    print(f"\n🎉 Pipeline completed in {total_time:.1f} seconds!")
    
    successful_steps = sum(1 for success in results.values() if success)
    total_steps = len(results)
    
    print(f"📊 Results: {successful_steps}/{total_steps} steps successful")
    
    for step, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {step.capitalize()}")
    
    if successful_steps == total_steps:
        print(f"\n🚀 SUCCESS! Your MLB betting analysis is ready!")
        
        print(f"\n💡 Next steps:")
        print(f"   • Run with different dates")
        print(f"   • Schedule daily runs")
        print(f"   • Build betting strategies")
        print(f"   • Switch to real data when ready")
        
        return True
    else:
        print(f"\n⚠️ Pipeline partially completed. Check errors above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
    
#!/bin/bash
# create_dirs.sh - Manual directory creation for MLB betting analysis
# Use this if Python scripts fail due to dependency issues

set -e  # Exit on any error

echo "🏗️  MLB Betting Analysis - Manual Directory Creation"
echo "=================================================="
echo "📅 $(date)"
echo ""

# Define directories to create
declare -A REQUIRED_DIRS=(
    ["stage"]="Data staging area for parquet files"
    ["logs"]="Application and error logs"
    ["migrations"]="Database schema migration files"
    ["loader"]="Data loading utilities"
    ["py"]="Main Python package"
)

declare -A OPTIONAL_DIRS=(
    ["tests"]="Unit and integration tests"
    ["docs"]="Project documentation"
    ["backup"]="Data backup storage"
    [".github/workflows"]="CI/CD workflows"
    ["scripts"]="Utility scripts"
    ["config"]="Configuration files"
)

# Function to create directory with error handling
create_directory() {
    local dir_name="$1"
    local description="$2"
    local is_required="$3"
    
    if [[ -d "$dir_name" ]]; then
        echo "   ✅ $dir_name (already exists) - $description"
        return 0
    elif [[ -e "$dir_name" ]]; then
        echo "   ❌ $dir_name exists but is not a directory!"
        if [[ "$is_required" == "true" ]]; then
            echo "      CRITICAL: Please remove the file '$dir_name' and run again"
            return 1
        else
            echo "      WARNING: Skipping optional directory"
            return 0
        fi
    else
        if mkdir -p "$dir_name" 2>/dev/null; then
            echo "   📂 Created: $dir_name - $description"
            return 0
        else
            echo "   ❌ Failed to create: $dir_name"
            if [[ "$is_required" == "true" ]]; then
                echo "      CRITICAL: Cannot create required directory"
                return 1
            else
                echo "      WARNING: Cannot create optional directory"
                return 0
            fi
        fi
    fi
}

# Function to create essential files
create_essential_files() {
    echo ""
    echo "📄 Creating essential files..."
    
    # Create Python package marker
    if [[ -d "py" ]]; then
        if [[ ! -f "py/__init__.py" ]]; then
            cat > "py/__init__.py" << 'EOF'
"""
MLB Betting Analysis Package

A comprehensive data collection and analysis system for MLB sports betting.
"""

__version__ = "1.0.0"
__author__ = "MLB Analytics Team"

# Optional: Import commonly used functions/classes
try:
    from .enhanced_simple_backfill import main as run_backfill
    from .simple_analysis import analyze_game
except ImportError:
    # Handle cases where dependencies aren't available
    pass
EOF
            echo "   📝 Created: py/__init__.py"
        else
            echo "   ✅ py/__init__.py (already exists)"
        fi
    fi
    
    # Create .gitkeep files for empty directories
    for dir in "logs" "stage" "backup"; do
        if [[ -d "$dir" && ! -f "$dir/.gitkeep" ]]; then
            echo "# Keep this directory in git" > "$dir/.gitkeep"
            echo "   📝 Created: $dir/.gitkeep"
        fi
    done
    
    # Create basic documentation
    if [[ -d "docs" && ! -f "docs/README.md" ]]; then
        cat > "docs/README.md" << 'EOF'
# MLB Betting Analysis Documentation

This directory contains project documentation.

## Getting Started
1. [Setup Guide](../README.md#setup)
2. [Configuration](../README.md#configuration)
3. [Usage Examples](../README.md#usage)

## API Documentation
- Configuration: `py/config.py`
- Data Collection: `py/enhanced_simple_backfill.py`
- Analysis: `py/simple_analysis.py`
- Database: `migrations/`

## Troubleshooting
- Check environment: `python check_env.py`
- Validate setup: `python validate_setup.py`
- View logs: `logs/`
EOF
        echo "   📝 Created: docs/README.md"
    fi
}

# Function to set permissions
set_permissions() {
    echo ""
    echo "🔒 Setting directory permissions..."
    
    # Ensure directories are readable and writable
    for dir in "stage" "logs" "backup"; do
        if [[ -d "$dir" ]]; then
            if chmod 755 "$dir" 2>/dev/null; then
                echo "   ✅ $dir - permissions set"
            else
                echo "   ⚠️  $dir - could not set permissions"
            fi
        fi
    done
}

# Function to create .gitignore if it doesn't exist
create_gitignore() {
    if [[ ! -f ".gitignore" ]]; then
        echo ""
        echo "📝 Creating .gitignore file..."
        
        cat > ".gitignore" << 'EOF'
# MLB Betting Analysis .gitignore

# Data files
*.parquet
*.csv
stage/*.parquet
stage/*.csv
backup/*.parquet
backup/*.csv

# Environment and secrets
.env
.env.local
.env.production
.env.staging

# API keys and credentials
*api_key*
*secret*
*password*

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
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual environments
venv/
env/
ENV/
.env/
.venv/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Database
*.db
*.sqlite
*.sqlite3

# Logs
logs/*.log
logs/*.txt
*.log

# AWS
.aws/
.boto

# Temporary files
*.tmp
*.temp
.cache/

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Documentation builds
docs/_build/
site/

# Jupyter Notebooks
.ipynb_checkpoints

# Archive files
*.zip
*.tar.gz
*.rar
EOF
        echo "   📝 Created: .gitignore"
    else
        echo ""
        echo "   ✅ .gitignore already exists"
    fi
}

# Main execution
echo "📁 Creating required directories..."
failed_required=0

for dir in "${!REQUIRED_DIRS[@]}"; do
    if ! create_directory "$dir" "${REQUIRED_DIRS[$dir]}" "true"; then
        ((failed_required++))
    fi
done

echo ""
echo "📁 Creating optional directories..."
failed_optional=0

for dir in "${!OPTIONAL_DIRS[@]}"; do
    if ! create_directory "$dir" "${OPTIONAL_DIRS[$dir]}" "false"; then
        ((failed_optional++))
    fi
done

# Create essential files
create_essential_files

# Set permissions
set_permissions

# Create .gitignore
create_gitignore

# Summary
echo ""
echo "=================================================="
echo "📊 Directory Creation Summary"
echo "=================================================="

total_required=${#REQUIRED_DIRS[@]}
total_optional=${#OPTIONAL_DIRS[@]}
success_required=$((total_required - failed_required))
success_optional=$((total_optional - failed_optional))

echo "Required directories: $success_required/$total_required created"
echo "Optional directories: $success_optional/$total_optional created"

if [[ $failed_required -eq 0 ]]; then
    echo ""
    echo "🎉 All required directories created successfully!"
    
    if [[ $failed_optional -gt 0 ]]; then
        echo "⚠️  Some optional directories failed (this is not critical)"
    fi
    
    echo ""
    echo "🚀 Next steps:"
    echo "   1. Install Python dependencies:"
    echo "      pip install -r py/requirements.txt"
    echo "   2. Configure environment:"
    echo "      python setup_env.py"
    echo "   3. Initialize database:"
    echo "      python initialize_database.py"
    echo "   4. Validate setup:"
    echo "      python check_env.py"
    
    exit 0
else
    echo ""
    echo "❌ Failed to create $failed_required required directories"
    echo ""
    echo "💡 Troubleshooting:"
    echo "   • Check file permissions: ls -la"
    echo "   • Remove conflicting files manually"
    echo "   • Run with sudo if necessary: sudo $0"
    echo "   • Check disk space: df -h"
    
    exit 1
fi
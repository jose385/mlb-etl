# Create-Directories.ps1
# PowerShell script for Windows users to set up MLB Betting Analysis directories
# Run with: PowerShell -ExecutionPolicy Bypass -File Create-Directories.ps1

param(
    [switch]$Force,
    [switch]$Verbose
)

# Set error action preference
$ErrorActionPreference = "Stop"

# Function to write colored output
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$ForegroundColor = "White"
    )
    Write-Host $Message -ForegroundColor $ForegroundColor
}

# Function to create directory with error handling
function New-ProjectDirectory {
    param(
        [string]$Path,
        [string]$Description,
        [bool]$IsRequired = $true
    )
    
    try {
        if (Test-Path $Path) {
            if ((Get-Item $Path).PSIsContainer) {
                Write-ColorOutput "   ✅ $($Path.PadRight(20)) - $Description" Green
                return $true
            } else {
                Write-ColorOutput "   ❌ $($Path.PadRight(20)) - EXISTS BUT IS NOT A DIRECTORY!" Red
                if ($IsRequired) {
                    Write-ColorOutput "      CRITICAL: Please remove the file '$Path' and run again" Red
                    return $false
                } else {
                    Write-ColorOutput "      WARNING: Skipping optional directory" Yellow
                    return $true
                }
            }
        } else {
            New-Item -ItemType Directory -Path $Path -Force | Out-Null
            Write-ColorOutput "   📂 Created: $($Path.PadRight(15)) - $Description" Cyan
            return $true
        }
    } catch {
        Write-ColorOutput "   ❌ Failed to create: $Path" Red
        Write-ColorOutput "      Error: $($_.Exception.Message)" Red
        
        if ($IsRequired) {
            Write-ColorOutput "      CRITICAL: Cannot create required directory" Red
            return $false
        } else {
            Write-ColorOutput "      WARNING: Cannot create optional directory" Yellow
            return $true
        }
    }
}

# Function to create essential files
function New-EssentialFiles {
    Write-ColorOutput "`n📄 Creating essential files..." White
    
    # Create Python package marker
    $pyInitPath = "py\__init__.py"
    if ((Test-Path "py") -and (-not (Test-Path $pyInitPath))) {
        $initContent = @'
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
'@
        Set-Content -Path $pyInitPath -Value $initContent -Encoding UTF8
        Write-ColorOutput "   📝 Created: $pyInitPath" Cyan
    } elseif (Test-Path $pyInitPath) {
        Write-ColorOutput "   ✅ $pyInitPath (already exists)" Green
    }
    
    # Create .gitkeep files for empty directories
    $gitkeepDirs = @("logs", "stage", "backup")
    foreach ($dir in $gitkeepDirs) {
        $gitkeepPath = "$dir\.gitkeep"
        if ((Test-Path $dir) -and (-not (Test-Path $gitkeepPath))) {
            Set-Content -Path $gitkeepPath -Value "# Keep this directory in git" -Encoding UTF8
            Write-ColorOutput "   📝 Created: $gitkeepPath" Cyan
        }
    }
    
    # Create basic documentation
    $docsReadmePath = "docs\README.md"
    if ((Test-Path "docs") -and (-not (Test-Path $docsReadmePath))) {
        $docsContent = @'
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
'@
        Set-Content -Path $docsReadmePath -Value $docsContent -Encoding UTF8
        Write-ColorOutput "   📝 Created: $docsReadmePath" Cyan
    }
}

# Function to create .gitignore if it doesn't exist
function New-GitIgnore {
    if (-not (Test-Path ".gitignore")) {
        Write-ColorOutput "`n📝 Creating .gitignore file..." White
        
        $gitignoreContent = @'
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
*.egg-info/
.installed.cfg
*.egg

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

# OS
.DS_Store
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

# Keep important directories
!stage/.gitkeep
!logs/.gitkeep
!backup/.gitkeep
'@
        Set-Content -Path ".gitignore" -Value $gitignoreContent -Encoding UTF8
        Write-ColorOutput "   📝 Created: .gitignore" Cyan
    } else {
        Write-ColorOutput "`n   ✅ .gitignore already exists" Green
    }
}

# Main script execution
Write-ColorOutput "🏗️  MLB Betting Analysis - Windows Directory Setup" White
Write-ColorOutput "=================================================" White
Write-ColorOutput "📅 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" White
Write-ColorOutput ""

# Check if we're in the right directory
$currentDir = Get-Location
Write-ColorOutput "📂 Current directory: $currentDir" White

# Define directories to create
$requiredDirs = @{
    "stage" = "Data staging area for parquet files"
    "logs" = "Application and error logs"
    "migrations" = "Database schema migration files"
    "loader" = "Data loading utilities"
    "py" = "Main Python package"
}

$optionalDirs = @{
    "tests" = "Unit and integration tests"
    "docs" = "Project documentation"
    "backup" = "Data backup storage"
    ".github\workflows" = "CI/CD workflows"
    "scripts" = "Utility scripts"
    "config" = "Configuration files"
}

Write-ColorOutput "📁 Creating required directories..." White
$failedRequired = 0

foreach ($dir in $requiredDirs.Keys) {
    if (-not (New-ProjectDirectory -Path $dir -Description $requiredDirs[$dir] -IsRequired $true)) {
        $failedRequired++
    }
}

Write-ColorOutput "`n📁 Creating optional directories..." White
$failedOptional = 0

foreach ($dir in $optionalDirs.Keys) {
    if (-not (New-ProjectDirectory -Path $dir -Description $optionalDirs[$dir] -IsRequired $false)) {
        $failedOptional++
    }
}

# Create essential files
New-EssentialFiles

# Create .gitignore
New-GitIgnore

# Summary
Write-ColorOutput "`n=================================================" White
Write-ColorOutput "📊 Directory Creation Summary" White
Write-ColorOutput "=================================================" White

$totalRequired = $requiredDirs.Count
$totalOptional = $optionalDirs.Count
$successRequired = $totalRequired - $failedRequired
$successOptional = $totalOptional - $failedOptional

Write-ColorOutput "Required directories: $successRequired/$totalRequired created" White
Write-ColorOutput "Optional directories: $successOptional/$totalOptional created" White

if ($failedRequired -eq 0) {
    Write-ColorOutput "`n🎉 All required directories created successfully!" Green
    
    if ($failedOptional -gt 0) {
        Write-ColorOutput "⚠️  Some optional directories failed (this is not critical)" Yellow
    }
    
    Write-ColorOutput "`n🚀 Next steps:" White
    Write-ColorOutput "   1. Install Python dependencies:" White
    Write-ColorOutput "      pip install -r py\requirements.txt" Cyan
    Write-ColorOutput "   2. Configure environment:" White
    Write-ColorOutput "      python setup_env.py" Cyan
    Write-ColorOutput "   3. Initialize database:" White
    Write-ColorOutput "      python initialize_database.py" Cyan
    Write-ColorOutput "   4. Validate setup:" White
    Write-ColorOutput "      python check_env.py" Cyan
    
    Write-ColorOutput "`n✅ Windows setup complete!" Green
    exit 0
} else {
    Write-ColorOutput "`n❌ Failed to create $failedRequired required directories" Red
    
    Write-ColorOutput "`n💡 Troubleshooting:" White
    Write-ColorOutput "   • Check file permissions" White
    Write-ColorOutput "   • Remove conflicting files manually" White
    Write-ColorOutput "   • Run as Administrator if necessary" White
    Write-ColorOutput "   • Check disk space: Get-PSDrive" White
    Write-ColorOutput "   • Try running Python setup instead: python setup_directories.py" White
    
    exit 1
}

# Check PowerShell execution policy at the end
Write-ColorOutput "`n💡 PowerShell Tip:" White
Write-ColorOutput "   If you get execution policy errors in the future, run:" White
Write-ColorOutput "   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser" Cyan
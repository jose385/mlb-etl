#!/bin/bash
# Quick setup script for MLB betting analysis

echo "🚀 MLB Betting Analysis - Quick Setup"
echo "======================================"

# Check if .env exists
if [ -f ".env" ]; then
    echo "✅ Found existing .env file"
    source .env
else
    echo "📝 No .env file found. Running interactive setup..."
    python setup_env.py
    
    if [ $? -eq 0 ]; then
        echo "✅ Environment setup complete"
        source .env
    else
        echo "❌ Environment setup failed"
        exit 1
    fi
fi

# Validate environment
echo "🔍 Validating environment..."
python check_env.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Setup complete! You can now run:"
    echo "   python run_backfill.py --help"
    echo "   python run_daily_analysis.py --help"
    echo "   python run_loader.py --help"
else
    echo "❌ Environment validation failed"
    exit 1
fi
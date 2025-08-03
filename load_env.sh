#!/bin/bash

# Load environment variables from .env file, ignoring comments and invalid lines
if [ -f .env ]; then
    echo "Loading environment variables from .env..."
    
    # Export only valid environment variable lines
    while IFS= read -r line; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        
        # Only export lines that look like valid env vars (KEY=value)
        if [[ "$line" =~ ^[A-Z_][A-Z0-9_]*= ]]; then
            export "$line"
            # Show what we loaded (hide passwords)
            key=$(echo "$line" | cut -d'=' -f1)
            if [[ "$key" == *"PASSWORD"* || "$key" == *"KEY"* ]]; then
                echo "  $key=***"
            else
                echo "  $line"
            fi
        fi
    done < .env
    
    echo "Environment loading complete."
else
    echo "No .env file found"
fi

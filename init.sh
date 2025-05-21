#!/bin/sh

# Get the directory of the script using a more portable method
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
echo "Script directory: $SCRIPT_DIR"

# Define project directory as the script directory itself
# (assuming prefect_flow is the main project directory)
PROJECT_DIR="$SCRIPT_DIR"
echo "Project directory: $PROJECT_DIR"

cd "$PROJECT_DIR"

# Create virtual environment if it doesn't exist
if [ ! -d .venv_test ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv_test
fi

# Use . instead of source for better compatibility
. .venv_test/bin/activate || {
    echo "Failed to activate virtual environment."
    exit 1
}

# Check if setup files exist before trying to install
if [ -f "setup.py" ] || [ -f "pyproject.toml" ]; then
    echo "Installing package..."
    pip install .
else
    echo "No setup.py or pyproject.toml found. Skipping package installation."
    # Install required packages from requirements.txt if it exists
    if [ -f "requirements.txt" ]; then
        echo "Installing from requirements.txt instead..."
        pip install -r requirements.txt
    fi
fi

echo "Environment setup complete!"
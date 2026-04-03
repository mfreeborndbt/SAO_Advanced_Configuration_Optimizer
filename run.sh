#!/usr/bin/env bash
set -e

# SAO Advanced Configuration Optimizer — one-command launcher
# Usage: ./run.sh

PYTHON=${PYTHON:-python3}
PORT=${PORT:-5555}

# Check Python 3.8+
if ! $PYTHON -c 'import sys; assert sys.version_info >= (3, 8)' 2>/dev/null; then
  echo "Error: Python 3.8 or higher is required."
  echo "Install from https://python.org or via your package manager."
  exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  $PYTHON -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install / upgrade dependencies quietly
echo "Installing dependencies..."
pip install -q -r requirements.txt

# Open browser after a short delay (non-blocking)
URL="http://localhost:$PORT"
if command -v open &>/dev/null; then
  (sleep 1.5 && open "$URL") &       # macOS
elif command -v xdg-open &>/dev/null; then
  (sleep 1.5 && xdg-open "$URL") &   # Linux
fi

echo ""
echo "  SAO Advanced Configuration Optimizer"
echo "  ------------------"
echo "  Running at: $URL"
echo "  Stop with:  Ctrl+C"
echo ""

# Launch the app
python app.py --port "$PORT"

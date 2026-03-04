#!/usr/bin/env bash
set -e

echo "============================================"
echo "  MongoDB Atlas PoC — Setup"
echo "============================================"
echo ""

# Check Python version
PYTHON=""
for cmd in python3.12 python3 python; do
  if command -v "$cmd" &>/dev/null; then
    ver=$("$cmd" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
    major=$(echo "$ver" | cut -d. -f1)
    minor=$(echo "$ver" | cut -d. -f2)
    if [ "$major" -ge 3 ] && [ "$minor" -ge 10 ]; then
      PYTHON="$cmd"
      break
    fi
  fi
done

if [ -z "$PYTHON" ]; then
  echo "ERROR: Python 3.10+ is required. Install it and re-run."
  exit 1
fi
echo "Using: $PYTHON ($($PYTHON --version))"

# Create virtual environment
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  $PYTHON -m venv .venv
fi

source .venv/bin/activate
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
echo ""

# Configure .env
if [ -f ".env" ]; then
  echo "Found existing .env file."
  # shellcheck disable=SC1091
  source .env 2>/dev/null || true
  echo "  MONGODB_URI=${MONGODB_URI:+(set)}"
  echo "  MONGODB_DATABASE=${MONGODB_DATABASE:-(not set)}"
  echo ""
  read -rp "Overwrite .env? [y/N] " overwrite
  if [[ ! "$overwrite" =~ ^[Yy] ]]; then
    echo ""
    echo "Setup complete. Run:  source .venv/bin/activate && python demo_hub.py"
    exit 0
  fi
fi

echo ""
echo "Enter your MongoDB Atlas connection string."
echo "  Example: mongodb+srv://user:pass@cluster.mongodb.net/?appName=MyApp"
echo ""
read -rp "MONGODB_URI: " uri

echo ""
echo "Enter the database name to use (press Enter for 'poc_demo'):"
read -rp "MONGODB_DATABASE [poc_demo]: " dbname
dbname="${dbname:-poc_demo}"

cat > .env <<EOF
MONGODB_URI=${uri}
MONGODB_DATABASE=${dbname}
MONGODB_COLLECTION=duns
EOF

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "  1. Activate:   source .venv/bin/activate"
echo "  2. Launch hub:  python demo_hub.py"
echo "  3. Open:        http://localhost:5050"
echo ""
echo "  The hub includes:"
echo "    - Change Stream Dashboard"
echo "    - Atlas Search Explorer"
echo "    - Relationship Graph"
echo "    - Locust Load Testing (4 scripts)"
echo ""

#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building WASM..."
wasm-pack build --target web --out-dir www/pkg

echo ""
echo "Done! To serve locally:"
echo "  python3 -m http.server 8080 -d www/"
echo "  open http://localhost:8080"

#!/usr/bin/env bash
#
# Run all rotortree divan benchmarks and generate an HTML report.
#
# Usage:
#   ./scripts/run_benchmarks.sh [--output-dir DIR]
#
# Environment:
#   RUSTFLAGS  Override compiler flags (default: -C target-cpu=native)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${PROJECT_DIR}/bench_results"
EXTRA_RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${OUTPUT_DIR}/${TIMESTAMP}"
mkdir -p "$RESULT_DIR"

# bench_name|features
BENCHES=(
    "tree_bench|"
    "tree_bench_concurrent|concurrent"
    "tree_bench_parallel|parallel"
    "tree_bench_all|concurrent,parallel"
    "tree_bench_storage|storage,parallel"
)

# Capture system info
{
    echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "machine: $(uname -a)"
    echo "rustc: $(rustc --version)"
    echo "cargo: $(cargo --version)"
} > "$RESULT_DIR/meta.txt"

echo "Output: $RESULT_DIR"
echo ""

OVERALL_START=$(date +%s)
FAILED=0

for spec in "${BENCHES[@]}"; do
    IFS='|' read -r bench_name features <<< "$spec"

    echo "=== $bench_name (features: ${features:-default}) ==="
    START=$(date +%s)

    FEAT_ARG=""
    if [[ -n "$features" ]]; then
        FEAT_ARG="--features $features"
    fi

    if RUSTFLAGS="$EXTRA_RUSTFLAGS" cargo bench \
        --manifest-path "$PROJECT_DIR/Cargo.toml" \
        --bench "$bench_name" $FEAT_ARG \
        > "$RESULT_DIR/${bench_name}.txt" 2>&1; then
        END=$(date +%s)
        echo "    done ($((END - START))s)"
    else
        END=$(date +%s)
        echo "    FAILED ($((END - START))s) — output saved"
        FAILED=$((FAILED + 1))
    fi
done

OVERALL_END=$(date +%s)
echo ""
echo "Completed in $((OVERALL_END - OVERALL_START))s ($FAILED failures)"
echo ""

echo "Generating HTML report..."
python3 "$SCRIPT_DIR/generate_report.py" \
    "$RESULT_DIR" \
    --meta "$RESULT_DIR/meta.txt" \
    -o "$RESULT_DIR/report.html"

ln -sfn "$RESULT_DIR" "$OUTPUT_DIR/latest"
echo "Report: $OUTPUT_DIR/latest/report.html"

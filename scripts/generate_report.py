#!/usr/bin/env python3
"""Parse criterion JSON benchmark output and generate an HTML report."""

import argparse
import html
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ── Data types ──────────────────────────────────────────────────────────────


@dataclass
class BenchEntry:
    name: str
    fastest_ns: float
    slowest_ns: float
    median_ns: float
    mean_ns: float
    samples: int
    iters: int
    throughput_elements: int | None = None


@dataclass
class BenchId:
    operation: str
    n: int
    count: int
    extra: dict = field(default_factory=dict)


@dataclass
class BenchRow:
    label: str
    fastest: str
    slowest: str
    median: str
    mean: str
    tp_fastest: str
    tp_slowest: str
    tp_median: str
    tp_mean: str
    samples: int
    iters: int
    diff_fastest: float | None = None
    diff_slowest: float | None = None
    diff_median: float | None = None
    diff_mean: float | None = None


# ── Constants ───────────────────────────────────────────────────────────────

BINARY_ORDER = [
    "tree_bench",
    "tree_bench_parallel",
    "tree_bench_concurrent",
    "tree_bench_all",
    "tree_bench_storage",
]

BINARY_DISPLAY = {
    "tree_bench": "tree_bench (sequential baseline)",
    "tree_bench_parallel": "tree_bench_parallel (rayon)",
    "tree_bench_concurrent": "tree_bench_concurrent",
    "tree_bench_all": "tree_bench_all (concurrent + parallel)",
    "tree_bench_storage": "tree_bench_storage (WAL + checkpoint)",
}

OP_ORDER = [
    "insert_single",
    "insert_many",
    "insert_many_chunked_100",
    "insert_many_chunked_1000",
    "insert_incremental",
    "generate_proof",
    "verify_proof",
    "snapshot",
    "flush",
    "open_recover",
    "mixed_workload",
    "sustained_checkpoint",
    "concurrent_contention",
]

DISPLAY_NAMES = {
    "insert_single": "insert (single)",
    "insert_many": "insert_many (batch)",
    "insert_many_chunked_100": "insert_many (chunked, 100/batch)",
    "insert_many_chunked_1000": "insert_many (chunked, 1000/batch)",
    "insert_incremental": "insert_many (incremental, half pre-filled)",
    "generate_proof": "generate_proof",
    "verify_proof": "verify_proof",
    "snapshot": "snapshot",
    "concurrent_contention": "concurrent contention (4 readers + 1 writer)",
    "flush": "flush (WAL fsync)",
    "open_recover": "open + recover (WAL replay)",
    "mixed_workload": "mixed workload (insert + proof + verify)",
    "sustained_checkpoint": "sustained checkpoint",
}


# ── Time formatting ───────────────────────────────────────────────────────


def format_time_ns(ns: float) -> str:
    """Format nanoseconds to human-readable string."""
    if ns < 1e3:
        return f"{ns:.1f} ns"
    elif ns < 1e6:
        return f"{ns / 1e3:.1f} µs"
    elif ns < 1e9:
        return f"{ns / 1e6:.3f} ms"
    else:
        return f"{ns / 1e9:.3f} s"


def format_throughput(elements: int, time_ns: float) -> str:
    """Format throughput as elements/second."""
    if time_ns <= 0:
        return ""
    eps = elements / (time_ns / 1e9)
    if eps >= 1e6:
        return f"{eps / 1e6:.2f} Melem/s"
    elif eps >= 1e3:
        return f"{eps / 1e3:.2f} Kelem/s"
    else:
        return f"{eps:.0f} elem/s"


# ── Criterion JSON parsing ────────────────────────────────────────────────


def parse_criterion_dir(binary_dir: Path) -> dict:
    """Walk a binary's criterion output directory and extract benchmark entries.

    Structure: binary_dir/{group_dir}/{param_dir}/new/{estimates,benchmark,sample}.json
    For sustained_checkpoint: binary_dir/{group_dir}/{function_dir}/{param_dir}/new/...
    """
    binary_name = binary_dir.name
    entries: list[BenchEntry] = []

    # Find all benchmark.json files — they mark individual benchmark points
    # Prefer new/ over base/ (both may exist when criterion has a previous baseline)
    for bm_json in sorted(binary_dir.rglob("benchmark.json")):
        data_dir = bm_json.parent  # e.g., .../new/ or .../base/
        param_dir = data_dir.parent  # e.g., .../insert_many_n8/1000/

        # Skip base/ if new/ exists for the same benchmark
        if data_dir.name == "base" and (param_dir / "new" / "benchmark.json").exists():
            continue
        # Skip change/ directory (contains diff estimates, not raw data)
        if data_dir.name == "change":
            continue

        try:
            benchmark = json.loads(bm_json.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        group_id = benchmark.get("group_id", "")
        value_str = benchmark.get("value_str", "")
        function_id = benchmark.get("function_id")
        throughput = benchmark.get("throughput")

        # Read estimates.json for mean/median
        estimates_path = data_dir / "estimates.json"
        if not estimates_path.exists():
            continue
        try:
            estimates = json.loads(estimates_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        mean_ns = estimates["mean"]["point_estimate"]
        median_ns = estimates["median"]["point_estimate"]

        # Read sample.json for fastest/slowest
        sample_path = data_dir / "sample.json"
        if not sample_path.exists():
            continue
        try:
            sample = json.loads(sample_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        iters_list = sample["iters"]
        times_list = sample["times"]
        per_iter = [t / i for t, i in zip(times_list, iters_list) if i > 0]
        if not per_iter:
            continue

        fastest_ns = min(per_iter)
        slowest_ns = max(per_iter)
        samples = len(iters_list)
        total_iters = int(sum(iters_list))

        throughput_elements = None
        if throughput and "Elements" in throughput:
            throughput_elements = int(throughput["Elements"])

        # Build entry name to match divan convention: {op}_n{N}_{count}[_every{freq}]
        # group_id is like "insert_many/n8", value_str is like "1000"
        # function_id is like "every5" for sustained_checkpoint
        op_n = group_id.replace("/", "_")  # "insert_many_n8"
        if function_id:
            entry_name = f"{op_n}_{function_id}_{value_str}"
        else:
            entry_name = f"{op_n}_{value_str}"

        entries.append(
            BenchEntry(
                name=entry_name,
                fastest_ns=fastest_ns,
                slowest_ns=slowest_ns,
                median_ns=median_ns,
                mean_ns=mean_ns,
                samples=samples,
                iters=total_iters,
                throughput_elements=throughput_elements,
            )
        )

    return {"binary": binary_name, "entries": entries}


# ── Name decomposition ────────────────────────────────────────────────────

RE_BENCH_NAME = re.compile(r"^(.+?)_n(\d+)(?:_every(\d+))?_(\d+)$")


def decompose_name(entry: BenchEntry) -> BenchId:
    """Decompose a benchmark name into structured components."""
    m = RE_BENCH_NAME.match(entry.name)
    if not m:
        return BenchId(operation=entry.name, n=0, count=0)

    op, n, every, count = m.groups()
    extra = {}
    if every:
        extra["every"] = int(every)
    return BenchId(operation=op, n=int(n), count=int(count), extra=extra)


def format_count(n: int) -> str:
    """Format a count with thousand separators."""
    return f"{n:,}"


# ── Diff computation ─────────────────────────────────────────────────────


def compute_diff_pct(current_ns: float, previous_ns: float | None) -> float | None:
    """Compute % change from previous to current. Positive = regression (slower)."""
    if previous_ns is None or previous_ns == 0:
        return None
    return ((current_ns - previous_ns) / previous_ns) * 100.0


def find_previous_run(input_dir: Path) -> Path | None:
    """Find the previous benchmark run directory (sibling sorted by name)."""
    parent = input_dir.parent
    if not parent.is_dir():
        return None
    current_name = input_dir.resolve().name
    siblings = sorted(
        d.name for d in parent.iterdir()
        if d.is_dir() and not d.is_symlink() and d.name != "latest"
    )
    try:
        idx = siblings.index(current_name)
    except ValueError:
        return None
    if idx == 0:
        return None
    prev = parent / siblings[idx - 1]
    # Check for criterion JSON subdirs (binary directories)
    has_json = any(
        d.is_dir() and d.name.startswith("tree_bench")
        for d in prev.iterdir()
    )
    if not has_json:
        # Fallback: check for old divan .txt files for backwards compat
        if not list(prev.glob("tree_bench*.txt")):
            return None
    return prev


@dataclass
class PrevEntry:
    fastest_ns: float | None
    slowest_ns: float | None
    median_ns: float | None
    mean_ns: float | None


def build_previous_lookup(
    all_results: list[dict],
) -> dict[tuple[str, str], PrevEntry]:
    """Build a flat lookup: (binary, entry_name) -> PrevEntry from parsed results."""
    lookup: dict[tuple[str, str], PrevEntry] = {}
    for result in all_results:
        binary = result["binary"]
        for entry in result["entries"]:
            lookup[(binary, entry.name)] = PrevEntry(
                fastest_ns=entry.fastest_ns,
                slowest_ns=entry.slowest_ns,
                median_ns=entry.median_ns,
                mean_ns=entry.mean_ns,
            )
    return lookup


def load_previous_results(prev_dir: Path) -> list[dict]:
    """Load benchmark results from a previous run directory (criterion JSON)."""
    results = []
    for sub in sorted(prev_dir.iterdir()):
        if sub.is_dir() and sub.name.startswith("tree_bench"):
            result = parse_criterion_dir(sub)
            if result and result["entries"]:
                results.append(result)
    return results


# ── Hierarchy construction ──────────────────────────────────────────────────


def build_hierarchy(
    all_results: list[dict],
    prev_lookup: dict[tuple[str, str], PrevEntry] | None = None,
) -> dict[str, dict[str, dict[str, list[BenchRow]]]]:
    """Build a nested dict: binary -> operation -> n_key -> [rows]."""
    hierarchy: dict[str, dict[str, dict[str, list[BenchRow]]]] = {}

    for result in all_results:
        binary = result["binary"]
        hierarchy[binary] = {}

        for entry in result["entries"]:
            bid = decompose_name(entry)
            op = bid.operation
            n_key = f"n={bid.n}" if bid.n > 0 else "default"

            if op not in hierarchy[binary]:
                hierarchy[binary][op] = {}
            if n_key not in hierarchy[binary][op]:
                hierarchy[binary][op][n_key] = []

            label = format_count(bid.count)
            if "every" in bid.extra:
                label += f" (every {bid.extra['every']})"

            # Compute throughput strings
            tp_fastest = tp_slowest = tp_median = tp_mean = ""
            if entry.throughput_elements:
                elems = entry.throughput_elements
                tp_fastest = format_throughput(elems, entry.fastest_ns)
                tp_slowest = format_throughput(elems, entry.slowest_ns)
                tp_median = format_throughput(elems, entry.median_ns)
                tp_mean = format_throughput(elems, entry.mean_ns)

            # Compute diffs against previous run
            prev = prev_lookup.get((binary, entry.name)) if prev_lookup else None

            row = BenchRow(
                label=label,
                fastest=format_time_ns(entry.fastest_ns),
                slowest=format_time_ns(entry.slowest_ns),
                median=format_time_ns(entry.median_ns),
                mean=format_time_ns(entry.mean_ns),
                tp_fastest=tp_fastest,
                tp_slowest=tp_slowest,
                tp_median=tp_median,
                tp_mean=tp_mean,
                samples=entry.samples,
                iters=entry.iters,
                diff_fastest=compute_diff_pct(entry.fastest_ns, prev.fastest_ns) if prev else None,
                diff_slowest=compute_diff_pct(entry.slowest_ns, prev.slowest_ns) if prev else None,
                diff_median=compute_diff_pct(entry.median_ns, prev.median_ns) if prev else None,
                diff_mean=compute_diff_pct(entry.mean_ns, prev.mean_ns) if prev else None,
            )
            hierarchy[binary][op][n_key].append(row)

    # Sort rows within each N group
    for binary in hierarchy.values():
        for op in binary.values():
            for rows in op.values():
                rows.sort(key=_row_sort_key)

    return hierarchy


def _row_sort_key(row: BenchRow) -> tuple:
    """Extract a numeric sort key from a row label."""
    nums = [int(x) for x in re.findall(r"\d+", row.label.replace(",", ""))]
    return tuple(nums) if nums else (0,)


def _op_sort_key(op: str) -> int:
    try:
        return OP_ORDER.index(op)
    except ValueError:
        return len(OP_ORDER)


def _n_sort_key(n_key: str) -> int:
    m = re.search(r"\d+", n_key)
    return int(m.group()) if m else 0


# ── HTML generation ─────────────────────────────────────────────────────────

CSS = """\
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    line-height: 1.6;
    color: #1a1a1a;
    background: #F5EEDD;
    padding: 32px 40px;
    max-width: 1400px;
    margin: 0 auto;
}
h1 {
    font-size: 22px;
    font-weight: 700;
    color: #096B68;
    margin-bottom: 4px;
}
.meta {
    color: #096B68;
    font-size: 11px;
    margin-bottom: 24px;
    opacity: 0.7;
}
details { margin-bottom: 2px; }
summary {
    cursor: pointer;
    padding: 8px 12px;
    border-radius: 6px;
    font-weight: 600;
    user-select: none;
    list-style: none;
}
summary::-webkit-details-marker { display: none; }
summary::before {
    content: '\\25B6';
    display: inline-block;
    margin-right: 10px;
    font-size: 10px;
    transition: transform 0.15s;
}
details[open] > summary::before {
    transform: rotate(90deg);
}
.level-1 > summary {
    font-size: 15px;
    background: #90D1CA;
    color: #096B68;
    margin-bottom: 4px;
}
.level-2 > summary {
    font-size: 13px;
    color: #129990;
    padding-left: 28px;
}
.level-2 > summary:hover { background: rgba(144, 209, 202, 0.3); }
.level-3 > summary {
    font-size: 12px;
    color: #1a1a1a;
    padding-left: 52px;
    font-weight: 500;
}
.level-3 > summary:hover { background: rgba(144, 209, 202, 0.2); }
.level-1 > .content { padding-left: 0; }
.level-2 > .content { padding-left: 8px; }
.level-3 > .content { padding-left: 16px; }
.bench-count {
    font-weight: 400;
    font-size: 11px;
    opacity: 0.6;
    margin-left: 6px;
}
.table-wrap {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    margin: 8px 0 12px 64px;
}
table {
    border-collapse: collapse;
    font-size: 12px;
}
th, td {
    padding: 5px 14px;
    text-align: right;
    white-space: nowrap;
}
th {
    font-weight: 600;
    color: #096B68;
    background: #90D1CA;
    border-bottom: 2px solid #129990;
}
th:first-child, td:first-child { text-align: left; }
td { border-bottom: 1px solid rgba(18, 153, 144, 0.15); }
tr:hover td { background: rgba(144, 209, 202, 0.15); }
.tp {
    color: #129990;
    font-size: 10px;
    display: block;
    margin-top: 1px;
}
.count-label { font-weight: 600; }
.diff-better {
    color: #1a7a3a;
    font-size: 10px;
    font-weight: 600;
}
.diff-worse {
    color: #c0392b;
    font-size: 10px;
    font-weight: 600;
}
@media (max-width: 768px) {
    body { padding: 16px 10px; font-size: 12px; }
    h1 { font-size: 18px; }
    .level-2 > summary { padding-left: 16px; }
    .level-3 > summary { padding-left: 28px; }
    .level-2 > .content { padding-left: 4px; }
    .level-3 > .content { padding-left: 8px; }
    .table-wrap { margin: 8px 0 12px 0; }
    table { font-size: 11px; }
    th, td { padding: 4px 8px; }
}
"""


def _esc(s: str) -> str:
    return html.escape(s, quote=True)


def render_diff(pct: float | None) -> str:
    """Render a small colored diff badge, or empty string if no diff."""
    if pct is None:
        return ""
    abs_pct = abs(pct)
    if abs_pct < 0.5:
        return ""
    if pct > 0:
        # Slower = regression (red)
        sign = "+"
        cls = "diff-worse"
    else:
        # Faster = improvement (green)
        sign = ""
        cls = "diff-better"
    if abs_pct >= 10:
        text = f"{sign}{pct:.0f}%"
    else:
        text = f"{sign}{pct:.1f}%"
    return f' <span class="{cls}">{text}</span>'


def render_cell(time_val: str, tp_val: str, diff_pct: float | None = None) -> str:
    """Render a table cell with timing, optional throughput, and optional diff."""
    out = _esc(time_val) + render_diff(diff_pct)
    if tp_val:
        out += f'<span class="tp">{_esc(tp_val)}</span>'
    return out


def render_table(rows: list[BenchRow]) -> str:
    """Render the innermost data table."""
    lines = ['<div class="table-wrap">', "<table>", "<thead><tr>"]
    lines.append(
        "<th>count</th><th>fastest</th><th>slowest</th><th>median</th><th>mean</th>"
    )
    lines.append("</tr></thead>")
    lines.append("<tbody>")

    for row in rows:
        lines.append("<tr>")
        lines.append(f'<td class="count-label">{_esc(row.label)}</td>')
        lines.append(f"<td>{render_cell(row.fastest, row.tp_fastest, row.diff_fastest)}</td>")
        lines.append(f"<td>{render_cell(row.slowest, row.tp_slowest, row.diff_slowest)}</td>")
        lines.append(f"<td>{render_cell(row.median, row.tp_median, row.diff_median)}</td>")
        lines.append(f"<td>{render_cell(row.mean, row.tp_mean, row.diff_mean)}</td>")
        lines.append("</tr>")

    lines.append("</tbody></table></div>")
    return "\n".join(lines)


def count_benchmarks(ops: dict[str, dict[str, list[BenchRow]]]) -> int:
    return sum(len(rows) for nmap in ops.values() for rows in nmap.values())


def generate_html(
    hierarchy: dict[str, dict[str, dict[str, list[BenchRow]]]],
    meta: dict[str, str],
) -> str:
    """Generate the full HTML report."""
    parts: list[str] = []

    parts.append("<!DOCTYPE html>")
    parts.append('<html lang="en">')
    parts.append("<head>")
    parts.append('<meta charset="utf-8">')
    parts.append('<meta name="viewport" content="width=device-width, initial-scale=1">')
    parts.append("<title>rotortree benchmarks</title>")
    parts.append('<link rel="preconnect" href="https://fonts.googleapis.com">')
    parts.append('<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>')
    parts.append(
        '<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:'
        'ital,wght@0,100..800;1,100..800&display=swap" rel="stylesheet">'
    )
    parts.append(f"<style>{CSS}</style>")
    parts.append("</head>")
    parts.append("<body>")

    parts.append(
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">'
        "<h1>rotortree benchmarks</h1>"
        '<div style="display:flex;gap:8px">'
        '<a href="../differ/" style="font-size:12px;color:#096B68;text-decoration:none;'
        'border:1px solid #096B68;padding:4px 10px;border-radius:4px">differ &rarr;</a>'
        '<a href="../viz/" style="font-size:12px;color:#096B68;text-decoration:none;'
        'border:1px solid #096B68;padding:4px 10px;border-radius:4px">viz &rarr;</a>'
        "</div></div>"
    )
    meta_items = []
    if "timestamp" in meta:
        meta_items.append(meta["timestamp"])
    if "rustc" in meta:
        meta_items.append(meta["rustc"])
    if "machine" in meta:
        # Shorten machine info to just OS + arch
        machine = meta["machine"]
        uname_parts = machine.split()
        if len(uname_parts) >= 3:
            meta_items.append(f"{uname_parts[0]} {uname_parts[2]}")
        else:
            meta_items.append(machine)
    if meta_items:
        parts.append(f'<div class="meta">{_esc(" | ".join(meta_items))}</div>')

    # Sort binaries
    sorted_binaries = sorted(
        hierarchy.keys(),
        key=lambda b: BINARY_ORDER.index(b) if b in BINARY_ORDER else len(BINARY_ORDER),
    )

    for binary in sorted_binaries:
        ops = hierarchy[binary]
        total = count_benchmarks(ops)
        display = BINARY_DISPLAY.get(binary, binary)

        parts.append('<details class="level-1" open>')
        parts.append(
            f"<summary>{_esc(display)}"
            f'<span class="bench-count">({total})</span></summary>'
        )
        parts.append('<div class="content">')

        sorted_ops = sorted(ops.keys(), key=_op_sort_key)

        for op in sorted_ops:
            n_map = ops[op]
            op_total = sum(len(rows) for rows in n_map.values())
            op_display = DISPLAY_NAMES.get(op, op)

            parts.append('<details class="level-2" open>')
            parts.append(
                f"<summary>{_esc(op_display)}"
                f'<span class="bench-count">({op_total})</span></summary>'
            )
            parts.append('<div class="content">')

            sorted_ns = sorted(n_map.keys(), key=_n_sort_key)

            for n_key in sorted_ns:
                rows = n_map[n_key]

                parts.append('<details class="level-3" open>')
                parts.append(
                    f"<summary>{_esc(n_key)}"
                    f'<span class="bench-count">'
                    f"({len(rows)})</span></summary>"
                )
                parts.append('<div class="content">')
                parts.append(render_table(rows))
                parts.append("</div>")
                parts.append("</details>")

            parts.append("</div>")
            parts.append("</details>")

        parts.append("</div>")
        parts.append("</details>")

    parts.append("</body>")
    parts.append("</html>")

    return "\n".join(parts)


# ── Meta file reader ────────────────────────────────────────────────────────


def read_meta(path: Path) -> dict[str, str]:
    """Read key: value pairs from meta.txt."""
    meta = {}
    for line in path.read_text().splitlines():
        if ":" in line:
            key, _, val = line.partition(":")
            meta[key.strip()] = val.strip()
    return meta


# ── CLI ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_dir", help="Directory containing criterion JSON subdirs")
    parser.add_argument("--meta", help="Path to meta.txt")
    parser.add_argument(
        "-o", "--output", help="Output HTML path (default: <input_dir>/report.html)"
    )
    parser.add_argument(
        "--previous",
        help="Previous run directory for diff %% (auto-detected if omitted)",
    )
    parser.add_argument(
        "--no-diff", action="store_true", help="Disable diff comparison"
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"Error: {input_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    meta = read_meta(Path(args.meta)) if args.meta else {}

    all_results = []
    for sub in sorted(input_dir.iterdir()):
        if sub.is_dir() and sub.name.startswith("tree_bench"):
            result = parse_criterion_dir(sub)
            if result and result["entries"]:
                all_results.append(result)
            else:
                print(f"Warning: no benchmark entries in {sub.name}", file=sys.stderr)

    if not all_results:
        print("Error: no benchmark results found", file=sys.stderr)
        sys.exit(1)

    # Load previous run for diff comparison
    prev_lookup = None
    if not args.no_diff:
        prev_dir = Path(args.previous) if args.previous else find_previous_run(input_dir)
        if prev_dir:
            prev_results = load_previous_results(prev_dir)
            if prev_results:
                prev_lookup = build_previous_lookup(prev_results)
                print(f"Comparing against previous run: {prev_dir.name}", file=sys.stderr)

    hierarchy = build_hierarchy(all_results, prev_lookup)
    html_content = generate_html(hierarchy, meta)

    output_path = Path(args.output) if args.output else input_dir / "report.html"
    output_path.write_text(html_content)
    print(f"Wrote {output_path} ({len(html_content):,} bytes)")


if __name__ == "__main__":
    main()

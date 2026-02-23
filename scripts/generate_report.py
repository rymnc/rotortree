#!/usr/bin/env python3
"""Parse divan benchmark output and generate an HTML report."""

import argparse
import html
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ── Data types ──────────────────────────────────────────────────────────────


@dataclass
class BenchEntry:
    name: str
    raw_name: str
    parent: str | None
    fastest: str
    slowest: str
    median: str
    mean: str
    samples: int
    iters: int
    tp_fastest: str = ""
    tp_slowest: str = ""
    tp_median: str = ""
    tp_mean: str = ""


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

# Regex to strip ANSI escape codes
RE_ANSI = re.compile(r"\x1b\[[0-9;]*m")

# Regex for the header line (binary name + column headers)
RE_HEADER = re.compile(r"^(\S+)\s+fastest\s+\u2502")

# Regex for a timing line: branch char + name + timing columns
# Divan uses │ (U+2502) as column separator
RE_TIMING = re.compile(
    r"^([\u2502\s]*)"  # leading indent
    r"[\u251c\u2570]\u2500\s+"  # branch char (├─ or ╰─)
    r"(\S+)\s+"  # benchmark name or arg value
    r"([\d.,]+\s+\S+)\s+\u2502\s+"  # fastest
    r"([\d.,]+\s+\S+)\s+\u2502\s+"  # slowest
    r"([\d.,]+\s+\S+)\s+\u2502\s+"  # median
    r"([\d.,]+\s+\S+)\s+\u2502\s+"  # mean
    r"(\d+)\s+\u2502\s+"  # samples
    r"(\d+)"  # iters
)

# Regex for a throughput line (continuation with item/s data)
RE_THROUGHPUT = re.compile(
    r"^[\u2502\s]+"  # leading indent/continuation
    r"([\d.,]+\s+\S*item/s)\s+\u2502\s+"  # fastest throughput
    r"([\d.,]+\s+\S*item/s)\s+\u2502\s+"  # slowest throughput
    r"([\d.,]+\s+\S*item/s)\s+\u2502\s+"  # median throughput
    r"([\d.,]+\s+\S*item/s)"  # mean throughput
)

# Regex for a parent-only line (e.g., special_insert_many with no timing)
RE_PARENT = re.compile(
    r"^([\u2502\s]*)"
    r"[\u251c\u2570]\u2500\s+"
    r"(\S+)\s*$"
)

# Regex to decompose crabtime-generated benchmark names
RE_BENCH_NAME = re.compile(r"^(.+?)_n(\d+)_(\d+)(?:_every(\d+))?$")


# ── Time parsing ───────────────────────────────────────────────────────────

TIME_UNITS = {"ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}
RE_TIME = re.compile(r"^([\d.,]+)\s+(\S+)$")


def parse_time_to_ns(s: str) -> float | None:
    """Parse a timing string like '109.1 ns' or '2.735 ms' into nanoseconds."""
    m = RE_TIME.match(s.strip())
    if not m:
        return None
    value_str, unit = m.groups()
    multiplier = TIME_UNITS.get(unit)
    if multiplier is None:
        return None
    return float(value_str.replace(",", "")) * multiplier


def compute_diff_pct(current: str, previous: float | None) -> float | None:
    """Compute % change from previous to current. Positive = regression (slower)."""
    if previous is None:
        return None
    cur_ns = parse_time_to_ns(current)
    if cur_ns is None or previous == 0:
        return None
    return ((cur_ns - previous) / previous) * 100.0


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
                fastest_ns=parse_time_to_ns(entry.fastest),
                slowest_ns=parse_time_to_ns(entry.slowest),
                median_ns=parse_time_to_ns(entry.median),
                mean_ns=parse_time_to_ns(entry.mean),
            )
    return lookup


def load_previous_results(prev_dir: Path) -> list[dict]:
    """Load and parse benchmark results from a previous run directory."""
    results = []
    for txt_file in sorted(prev_dir.glob("tree_bench*.txt")):
        text = txt_file.read_text()
        result = parse_divan_output(text, txt_file.stem)
        if result and result["entries"]:
            results.append(result)
    return results


# ── Parsing ─────────────────────────────────────────────────────────────────


def parse_divan_output(text: str, fallback_name: str) -> dict:
    """Parse raw divan output text into structured data."""
    binary_name = fallback_name
    entries: list[BenchEntry] = []
    current_parent: str | None = None
    last_entry: BenchEntry | None = None

    for raw_line in text.splitlines():
        line = RE_ANSI.sub("", raw_line)

        # Header line
        m = RE_HEADER.match(line)
        if m:
            binary_name = m.group(1)
            current_parent = None
            continue

        # Timing line
        m = RE_TIMING.match(line)
        if m:
            indent, name, fastest, slowest, median, mean, samples, iters = m.groups()
            depth = indent.count("\u2502")

            if depth > 0 and current_parent:
                full_name = f"{current_parent}_{name}"
            else:
                full_name = name
                current_parent = None

            entry = BenchEntry(
                name=full_name,
                raw_name=name,
                parent=current_parent if depth > 0 else None,
                fastest=fastest.strip(),
                slowest=slowest.strip(),
                median=median.strip(),
                mean=mean.strip(),
                samples=int(samples),
                iters=int(iters),
            )
            entries.append(entry)
            last_entry = entry
            continue

        # Throughput line
        m = RE_THROUGHPUT.match(line)
        if m and last_entry:
            tp_f, tp_s, tp_med, tp_mean = m.groups()
            last_entry.tp_fastest = tp_f.strip()
            last_entry.tp_slowest = tp_s.strip()
            last_entry.tp_median = tp_med.strip()
            last_entry.tp_mean = tp_mean.strip()
            continue

        # Parent-only line (no timing data)
        m = RE_PARENT.match(line)
        if m:
            current_parent = m.group(2)
            last_entry = None
            continue

    return {"binary": binary_name, "entries": entries}


def decompose_name(entry: BenchEntry) -> BenchId:
    """Decompose a benchmark name into structured components."""
    m = RE_BENCH_NAME.match(entry.name)
    if not m:
        return BenchId(operation=entry.name, n=0, count=0)

    op, n, count, every = m.groups()
    extra = {}
    if every:
        extra["every"] = int(every)
    return BenchId(operation=op, n=int(n), count=int(count), extra=extra)


def format_count(n: int) -> str:
    """Format a count with thousand separators."""
    return f"{n:,}"


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

            # Compute diffs against previous run
            prev = prev_lookup.get((binary, entry.name)) if prev_lookup else None

            row = BenchRow(
                label=label,
                fastest=entry.fastest,
                slowest=entry.slowest,
                median=entry.median,
                mean=entry.mean,
                tp_fastest=entry.tp_fastest,
                tp_slowest=entry.tp_slowest,
                tp_median=entry.tp_median,
                tp_mean=entry.tp_mean,
                samples=entry.samples,
                iters=entry.iters,
                diff_fastest=compute_diff_pct(entry.fastest, prev.fastest_ns) if prev else None,
                diff_slowest=compute_diff_pct(entry.slowest, prev.slowest_ns) if prev else None,
                diff_median=compute_diff_pct(entry.median, prev.median_ns) if prev else None,
                diff_mean=compute_diff_pct(entry.mean, prev.mean_ns) if prev else None,
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
        '<a href="../viz/" style="font-size:12px;color:#096B68;text-decoration:none;'
        'border:1px solid #096B68;padding:4px 10px;border-radius:4px">viz &rarr;</a>'
        "</div>"
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
    parser.add_argument("input_dir", help="Directory containing .txt files")
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
    for txt_file in sorted(input_dir.glob("tree_bench*.txt")):
        text = txt_file.read_text()
        result = parse_divan_output(text, txt_file.stem)
        if result and result["entries"]:
            all_results.append(result)
        else:
            print(f"Warning: no benchmark entries in {txt_file.name}", file=sys.stderr)

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

#!/usr/bin/env python3
from __future__ import annotations

import shutil
import fnmatch
import sys
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from urllib.parse import quote


TITLE = "datasets"
RESOURCES_DIRNAME = "resources"
SKIP_NAMES = {"index.html", RESOURCES_DIRNAME}


def load_ignore_patterns(path: Path) -> list[str]:
    if not path.is_file():
        return []

    patterns: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns

def is_ignored(path: Path, ignore_patterns: list[str]) -> bool:
    name = path.name
    return any(fnmatch.fnmatch(name, pattern) for pattern in ignore_patterns)

def href_for(path: Path) -> str:
    return "/".join(quote(part) for part in path.parts)


def resources_href_for(dir_path: Path, root: Path) -> str:
    rel_dir = dir_path.relative_to(root)
    if rel_dir == Path("."):
        return RESOURCES_DIRNAME
    return "/".join([".."] * len(rel_dir.parts) + [RESOURCES_DIRNAME])


def visible_children(dir_path: Path, ignore_patterns: list[str]) -> list[Path]:
    return [
        p for p in dir_path.iterdir()
        if p.name not in SKIP_NAMES and not is_ignored(p, ignore_patterns)
    ]

def display_name_for_file(path: Path) -> str:
    return path.stem if path.suffix else path.name

def entry_sort_key(path: Path) -> tuple[int, str]:
    # folders first, then alphabetical
    return (1 if path.is_file() else 0, path.name.lower())


def icon_name_for_file(path: Path) -> str:
    suffixes = [s.lower() for s in path.suffixes]
    if not suffixes:
        return "icon_file.svg"

    joined = "".join(suffixes)
    if joined.endswith(".jsonld"):
        return "icon_jsonld.svg"

    ext = suffixes[-1]
    if ext == ".csv":
        return "icon_csv.svg"
    if ext in {".xls", ".xlsx"}:
        return "icon_xls.svg"
    if ext == ".xml":
        return "icon_xml.svg"
    if ext == ".rdf":
        return "icon_rdf.svg"
    if ext == ".ttl":
        return "icon_ttl.svg"
    if ext == ".json":
        return "icon_json.svg"

    return "icon_file.svg"


def display_extension(path: Path) -> str:
    suffixes = [s.lower() for s in path.suffixes]
    if not suffixes:
        return "—"

    joined = "".join(suffixes)
    if joined.endswith(".jsonld"):
        return "jsonld"

    return suffixes[-1].lstrip(".")


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    unit_index = 0

    while value >= 1000 and unit_index < len(units) - 1:
        value /= 1000.0
        unit_index += 1

    if unit_index == 0:
        return f"{int(value)} B"

    text = f"{value:.1f}".rstrip("0").rstrip(".")
    return f"{text} {units[unit_index]}"


def format_clock(dt: datetime) -> str:
    return dt.strftime("%I:%M%p").lstrip("0").lower()


def format_mtime(ts: float, now: datetime) -> str:
    dt = datetime.fromtimestamp(ts)
    today = now.date()
    yesterday = today - timedelta(days=1)

    if dt.date() == today:
        return f"Today {format_clock(dt)}"
    if dt.date() == yesterday:
        return f"Yesterday {format_clock(dt)}"
    return f"{dt.strftime('%Y-%m-%d')} {format_clock(dt)}"


def directory_size_bytes(
    dir_path: Path,
    cache: dict[Path, int],
    ignore_patterns: list[str],
) -> int:
    if dir_path in cache:
        return cache[dir_path]

    total = 0
    for child in visible_children(dir_path, ignore_patterns):
        if child.is_dir():
            total += directory_size_bytes(child, cache, ignore_patterns)
        elif child.is_file():
            total += child.stat().st_size

    cache[dir_path] = total
    return total


def copy_icon_assets(src_resources: Path, dst_resources: Path) -> None:
    dst_resources.mkdir(parents=True, exist_ok=True)

    for p in src_resources.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() != ".svg":
            continue
        shutil.copy2(p, dst_resources / p.name)


def write_index(
    dir_path: Path,
    root: Path,
    cssstyle: str,
    header: str,
    sort_js: str,
    dir_size_cache: dict[Path, int],
    ignore_patterns: list[str],
) -> None:
    entries = sorted(visible_children(dir_path, ignore_patterns), key=entry_sort_key)

    rel_dir = dir_path.relative_to(root)
    pretty_dir = "/" if rel_dir == Path(".") else f"/{rel_dir.as_posix()}/"
    resources_href = resources_href_for(dir_path, root)
    dir_icon_href = f"{resources_href}/icon_dir.svg"

    style_split = ["    " + s for s in cssstyle.splitlines()]
    header_split = ["  " + s for s in header.splitlines()]
    script_split = ["    " + s for s in sort_js.splitlines()]

    now = datetime.now()

    lines: list[str] = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='utf-8'>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"  <title>{escape(TITLE)} – {escape(pretty_dir)}</title>",
        "  <style>",
    ]
    lines += style_split
    lines += [
        "  </style>",
        "</head>",
        "<body>",
    ]
    lines += header_split
    lines += [
        "  <main class='content'>",
        f"    <h1>{escape(TITLE)}</h1>",
        f"    <div class='path'>{escape(pretty_dir)}</div>",
        "    <div class='table-wrap'>",
        "      <table class='index-table'>",
        "        <thead>",
        "          <tr>",
        "            <th><button class='sort-btn' type='button' data-sort-key='name'>Name <span class='sort-indicator'></span></button></th>",
        "            <th><button class='sort-btn' type='button' data-sort-key='ext'>Ext <span class='sort-indicator'></span></button></th>",
        "            <th><button class='sort-btn' type='button' data-sort-key='size'>Size <span class='sort-indicator'></span></button></th>",
        "            <th><button class='sort-btn' type='button' data-sort-key='mtime'>Modified <span class='sort-indicator'></span></button></th>",
        "          </tr>",
        "        </thead>",
        "        <tbody id='index-body'>",
    ]

    if dir_path != root:
        lines += [
            "          <tr data-parent='1'>",
            "            <td class='col-name'>",
            "              <div class='name-cell'>",
            "                <a class='icon-link' href='../index.html' aria-label='Go up'>",
            f"                  <img class='entry-icon' src='{escape(dir_icon_href)}' alt=''>",
            "                </a>",
            "                <a class='name-link' href='../index.html'>..</a>",
            "              </div>",
            "            </td>",
            "            <td class='col-ext'></td>",
            "            <td class='col-size'>—</td>",
            "            <td class='col-mtime'>—</td>",
            "          </tr>",
        ]

    for idx, entry in enumerate(entries):
        if entry.is_dir():
            href = href_for(Path(entry.name) / "index.html")
            icon_href = dir_icon_href
            ext_text = "" # or "dir"
            size_bytes = directory_size_bytes(entry, dir_size_cache, ignore_patterns)
            display_size = human_size(size_bytes)
            mtime_ts = int(entry.stat().st_mtime)
            display_mtime = format_mtime(mtime_ts, now)
            display_name = f"{entry.name}/"
        else:
            href = href_for(Path(entry.name))
            icon_name = icon_name_for_file(entry)
            icon_href = f"{resources_href}/{icon_name}"
            ext_text = display_extension(entry)
            size_bytes = entry.stat().st_size
            display_size = human_size(size_bytes)
            mtime_ts = int(entry.stat().st_mtime)
            display_mtime = format_mtime(mtime_ts, now)
            display_name = display_name_for_file(entry)

        lines += [
            "          <tr"
            f" data-is-dir='{'1' if entry.is_dir() else '0'}'"
            f" data-name='{escape(entry.name.lower())}'"
            f" data-ext='{escape(ext_text.lower())}'"
            f" data-size='{size_bytes}'"
            f" data-mtime='{mtime_ts}'"
            f" data-original-index='{idx}'"
            ">",
            "            <td class='col-name'>",
            "              <div class='name-cell'>",
            f"                <a class='icon-link' href='{escape(href)}' aria-label='Open {escape(display_name)}'>",
            f"                  <img class='entry-icon' src='{escape(icon_href)}' alt=''>",
            "                </a>",
            f"                <a class='name-link' href='{escape(href)}'>{escape(display_name)}</a>",
            "              </div>",
            "            </td>",
            f"            <td class='col-ext'>{escape(ext_text)}</td>",
            f"            <td class='col-size'>{escape(display_size)}</td>",
            f"            <td class='col-mtime'>{escape(display_mtime)}</td>",
            "          </tr>",
        ]

    lines += [
        "        </tbody>",
        "      </table>",
        "    </div>",
        "  </main>",
        "  <script>",
    ]
    lines += script_split
    lines += [
        "  </script>",
        "</body>",
        "</html>",
    ]

    (dir_path / "index.html").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: make_index.py <root_dir>", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    src_resources = script_dir / RESOURCES_DIRNAME

    cssstyle = (src_resources / "style.css").read_text(encoding="utf-8")
    header = (src_resources / "header.html").read_text(encoding="utf-8")
    sort_js = (src_resources / "sort.js").read_text(encoding="utf-8")
    ignore_patterns = load_ignore_patterns(src_resources / "ignore.txt")

    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    copy_icon_assets(src_resources, root / RESOURCES_DIRNAME)

    dir_size_cache: dict[Path, int] = {}

    dirs = [root] + sorted(
        (
            p for p in root.rglob("*")
            if p.is_dir() and p.name != RESOURCES_DIRNAME
        ),
        key=lambda p: str(p),
    )

    for d in dirs:
        write_index(d, root, cssstyle, header, sort_js, dir_size_cache, ignore_patterns)

    print(f"Generated index.html in {len(dirs)} directories under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
#!/usr/bin/env python3
from __future__ import annotations

import sys
from html import escape
from pathlib import Path
from urllib.parse import quote


TITLE = "datasets"

def href_for(path: Path) -> str:
    return "/".join(quote(part) for part in path.parts)


def write_index(dir_path: Path, root: Path, cssstyle: str, header: str) -> None:
    entries = sorted(
        [p for p in dir_path.iterdir() if p.name != "index.html"],
        key=lambda p: (p.is_file(), p.name.lower()),
    )

    rel_dir = dir_path.relative_to(root)
    pretty_dir = "/" if rel_dir == Path(".") else f"/{rel_dir.as_posix()}/"

    style_split = ["    " + s for s in cssstyle.splitlines()]
    header_split = ["  " + s for s in header.splitlines()]

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
        "    <ul>",
    ]

    if dir_path != root:
        lines.append("    <li class='dir'><a href='../index.html'>..</a></li>")

    for entry in entries:
        if entry.is_dir():
            href = href_for(Path(entry.name) / "index.html")
            lines.append(
                f"    <li class='dir'><a href='{escape(href)}'>{escape(entry.name)}/</a></li>"
            )
        else:
            href = href_for(Path(entry.name))
            size = entry.stat().st_size
            lines.append(
                f"    <li class='file'><a href='{escape(href)}'>{escape(entry.name)}</a>"
                f"<span class='meta'>({size} bytes)</span></li>"
            )

    lines += [
        "    </ul>",
        "  </main>",
        "</body>",
        "</html>",
    ]

    (dir_path / "index.html").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: make_index.py <root_dir>", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    cssstyle = (script_dir / "style.css").read_text(encoding="utf-8")
    header = (script_dir / "header.html").read_text(encoding="utf-8")

    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    dirs = [root] + sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: str(p))
    for d in dirs:
        write_index(d, root, cssstyle, header)

    print(f"Generated index.html in {len(dirs)} directories under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

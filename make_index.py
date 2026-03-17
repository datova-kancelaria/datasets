#!/usr/bin/env python3
from __future__ import annotations

import sys
from html import escape
from pathlib import Path
from urllib.parse import quote


TITLE = "datasets"


def href_for(path: Path) -> str:
    return "/".join(quote(part) for part in path.parts)


def rel_href(from_dir: Path, to_path: Path) -> str:
    rel = to_path.relative_to(from_dir)
    return href_for(rel)


def write_index(dir_path: Path, root: Path) -> None:
    entries = sorted(
        [p for p in dir_path.iterdir() if p.name != "index.html"],
        key=lambda p: (p.is_file(), p.name.lower()),
    )

    rel_dir = dir_path.relative_to(root)
    pretty_dir = "/" if rel_dir == Path(".") else f"/{rel_dir.as_posix()}/"

    lines: list[str] = [
        "<!doctype html>",
        "<html lang='en'>",
        "<head>",
        "  <meta charset='utf-8'>",
        "  <meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"  <title>{escape(TITLE)} – {escape(pretty_dir)}</title>",
        "  <style>",
        "    body { font-family: sans-serif; max-width: 900px; margin: 40px auto; padding: 0 16px; line-height: 1.5; }",
        "    h1 { margin-bottom: 0.2em; }",
        "    .path { color: #666; margin-bottom: 1.5em; }",
        "    ul { list-style: none; padding-left: 0; }",
        "    li { margin: 0.35em 0; }",
        "    a { text-decoration: none; }",
        "    a:hover { text-decoration: underline; }",
        "    .dir a::before { content: '📁 '; }",
        "    .file a::before { content: '📄 '; }",
        "    .meta { color: #666; font-size: 0.92em; margin-left: 0.4em; }",
        "  </style>",
        "</head>",
        "<body>",
        f"  <h1>{escape(TITLE)}</h1>",
        f"  <div class='path'>{escape(pretty_dir)}</div>",
        "  <ul>",
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
        "  </ul>",
        "</body>",
        "</html>",
    ]

    (dir_path / "index.html").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: make_index.py <root_dir>", file=sys.stderr)
        return 2

    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    dirs = [root] + sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: str(p))
    for d in dirs:
        write_index(d, root)

    print(f"Generated index.html in {len(dirs)} directories under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

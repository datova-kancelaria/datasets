from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from rdflib import Graph

from .io_utils import atomic_write_bytes, atomic_write_text


def merge_csv_chunks(chunks: list[bytes]) -> bytes:
    out_lines: list[str] = []
    header_norm: Optional[str] = None

    for b in chunks:
        if not b:
            continue
        txt = b.decode("utf-8", errors="replace")
        if not txt.strip():
            continue

        lines = txt.splitlines(True)
        if not lines:
            continue

        first_line = lines[0].lstrip("\ufeff")
        if header_norm is None:
            header_norm = first_line.strip("\r\n")
            out_lines.append(first_line if first_line.endswith(("\n", "\r")) else first_line + "\n")
            out_lines.extend(lines[1:])
        else:
            if first_line.lstrip("\ufeff").strip("\r\n") == header_norm:
                out_lines.extend(lines[1:])
            else:
                out_lines.extend(lines)

    return "".join(out_lines).encode("utf-8")


def merge_rdfxml_chunks(payloads: list[bytes]) -> bytes:
    payloads = [p for p in payloads if p and p.strip()]
    if not payloads:
        return b""
    if len(payloads) == 1:
        return payloads[0]

    g = Graph()
    for b in payloads:
        g.parse(data=b, format="xml")

    out = g.serialize(format="pretty-xml")
    return out.encode("utf-8") if isinstance(out, str) else bytes(out)


def write_chunk_files(chunk_dir: Path, chunks: list[tuple[str, bytes]]) -> list[Path]:
    paths: list[Path] = []
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in chunks:
        p = chunk_dir / filename
        atomic_write_bytes(p, payload)
        paths.append(p)
    return paths


def write_chunk_manifest(
    manifest_path: Path,
    *,
    dataset: str,
    fmt: str,
    d_from: str,
    d_to: str,
    merge_strategy: str,
    chunk_files: list[Path],
) -> None:
    payload = {
        "dataset": dataset,
        "format": fmt,
        "dateFrom": d_from,
        "dateTo": d_to,
        "merged": False,
        "reason": f"merge_strategy={merge_strategy}",
        "chunks": [p.name for p in chunk_files],
    }
    atomic_write_text(manifest_path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

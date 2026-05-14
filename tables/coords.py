from __future__ import annotations

from openpyxl.utils import get_column_letter

ColumnRef = int | str


def column_to_index(column: ColumnRef) -> int:
    """Convert 1-based column index or Excel letters to a 1-based index."""
    if isinstance(column, int):
        if column < 1:
            raise ValueError(f"Column index must be >= 1, got {column}")
        return column

    if not isinstance(column, str):
        raise TypeError(f"Column must be int or str, got {type(column).__name__}")

    text = column.strip().upper()
    if not text:
        raise ValueError("Column string cannot be empty")

    value = 0
    for ch in text:
        if not "A" <= ch <= "Z":
            raise ValueError(f"Invalid Excel column {column!r}")
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def index_to_column(index: int) -> str:
    if not isinstance(index, int):
        raise TypeError(f"Column index must be an int, got {type(index).__name__}")
    if index < 1:
        raise ValueError(f"Column index must be >= 1, got {index}")
    return get_column_letter(index)


def cell_ref(column: ColumnRef, row: int) -> str:
    if row < 1:
        raise ValueError(f"Row index must be >= 1, got {row}")
    return f"{index_to_column(column_to_index(column))}{row}"

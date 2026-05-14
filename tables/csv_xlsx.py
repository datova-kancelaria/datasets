from __future__ import annotations

import csv
import re
from pathlib import Path

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from tqdm import tqdm


COL_HEADER = "FF1F4E78" # ARGB
COL_HEADER_TEXT = "FFFFFFFF"

# Data-row striping is applied through conditional formatting, not per-cell fills.
# Row 2 is the first data row, so even rows get yellow, odd rows get blue.
COL_ROW_EVEN = "FFFFE4C4"
COL_ROW_ODD = "FFC9EDFF"

EXCEL_MAX_ROWS = 1_048_576
DEFAULT_MIN_WIDTH = 8
DEFAULT_MAX_WIDTH = 80
DEFAULT_PADDING = 2


def _safe_sheet_name(name: str) -> str:
    name = re.sub(r"[\[\]\:\*\?\/\\]", "_", name).strip()
    return (name or "Sheet1")[:31]


def _display_width(value: str) -> int:
    if not value:
        return 0
    return max(len(part) for part in str(value).splitlines())


def _inspect_csv(csv_path: Path, *, show_progress: bool = True) -> tuple[int, list[int]]:
    row_count = 0
    widths: list[int] = []

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = tqdm(
            reader,
            desc=f"Scanning {csv_path.name}",
            unit="row",
            leave=False,
            disable=not show_progress,
        )
        for row in rows:
            row_count += 1
            if row_count > EXCEL_MAX_ROWS:
                raise ValueError(
                    f"{csv_path} has more than Excel's {EXCEL_MAX_ROWS} row limit"
                )

            if len(row) > len(widths):
                widths.extend([0] * (len(row) - len(widths)))

            for idx, value in enumerate(row):
                widths[idx] = max(widths[idx], _display_width(value))

    return row_count, widths


def _add_zebra_conditional_formatting(ws, *, row_count: int, col_count: int) -> None:
    if row_count <= 1 or col_count <= 0:
        return

    last_col = get_column_letter(col_count)
    data_range = f"A2:{last_col}{row_count}"

    yellow_fill = PatternFill(
        fill_type="solid",
        start_color=COL_ROW_EVEN,
        end_color=COL_ROW_EVEN,
    )
    blue_fill = PatternFill(
        fill_type="solid",
        start_color=COL_ROW_ODD,
        end_color=COL_ROW_ODD,
    )

    ws.conditional_formatting.add(
        data_range,
        FormulaRule(formula=["MOD(ROW()-2,2)=0"], fill=yellow_fill),
    )
    ws.conditional_formatting.add(
        data_range,
        FormulaRule(formula=["MOD(ROW()-2,2)=1"], fill=blue_fill),
    )


def csv_to_xlsx(
    csv_path: str | Path,
    xlsx_path: str | Path | None = None,
    *,
    sheet_name: str | None = None,
    min_width: int = DEFAULT_MIN_WIDTH,
    max_width: int = DEFAULT_MAX_WIDTH,
    padding: int = DEFAULT_PADDING,
    show_progress: bool = True,
) -> Path:
    csv_path = Path(csv_path)
    xlsx_path = Path(xlsx_path) if xlsx_path is not None else csv_path.with_suffix(".xlsx")

    row_count, widths = _inspect_csv(csv_path, show_progress=show_progress)

    wb = Workbook(write_only=True)
    ws = wb.create_sheet(_safe_sheet_name(sheet_name or csv_path.stem))

    # In write-only mode, set sheet-level properties before appending rows.
    if row_count > 1:
        ws.freeze_panes = "A2"

    if row_count >= 1 and widths:
        last_col = get_column_letter(len(widths))
        ws.auto_filter.ref = f"A1:{last_col}{row_count}"

    for idx, width in enumerate(widths, start=1):
        letter = get_column_letter(idx)
        ws.column_dimensions[letter].width = max(
            min_width,
            min(max_width, width + padding),
        )

    header_fill = PatternFill(fill_type="solid", fgColor=COL_HEADER)
    header_font = Font(bold=True, color=COL_HEADER_TEXT)
    header_alignment = Alignment(vertical="center")

    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = tqdm(
            reader,
            total=row_count,
            desc=f"Writing {csv_path.name}",
            unit="row",
            leave=False,
            disable=not show_progress,
        )

        for row_idx, row in enumerate(rows, start=1):
            if row_idx == 1:
                out_row = []
                for value in row:
                    cell = WriteOnlyCell(ws, value=value)
                    cell.fill = header_fill
                    cell.font = header_font
                    cell.alignment = header_alignment
                    cell.number_format = "@"
                    out_row.append(cell)
                ws.append(out_row)
            else:
                ws.append(row)

    if row_count >= 1 and widths:
        _add_zebra_conditional_formatting(
            ws,
            row_count=row_count,
            col_count=len(widths),
        )

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path


def write_xlsx_for_csv_tree(root: str | Path, *, show_progress: bool = True) -> list[Path]:
    root = Path(root)
    csv_files = sorted(root.rglob("*.csv"))
    written: list[Path] = []

    if show_progress:
        print(f"Creating XLSX files for {len(csv_files)} CSV files...", flush=True)

    for csv_path in tqdm(
        csv_files,
        desc="Creating XLSX files",
        unit="file",
        disable=not show_progress,
    ):
        if show_progress:
            tqdm.write(f"[XLSX] {csv_path.relative_to(root)}")
        written.append(csv_to_xlsx(csv_path, show_progress=show_progress))

    return written

from __future__ import annotations

from typing import Any, Iterable, Tuple

ColorLike = str | tuple[int, int, int] | list[int] | None

# CSS-ish named colors plus a few Excel-friendly aliases.
# Values are RRGGBB without alpha.
NAMED_COLORS: dict[str, str] = {
    "transparent": "",
    "none": "",
    "black": "000000",
    "white": "FFFFFF",
    "red": "FF0000",
    "green": "008000",
    "blue": "0000FF",
    "yellow": "FFFF00",
    "orange": "FFA500",
    "purple": "800080",
    "pink": "FFC0CB",
    "brown": "A52A2A",
    "gray": "808080",
    "grey": "808080",
    "lightgray": "D3D3D3",
    "lightgrey": "D3D3D3",
    "darkgray": "A9A9A9",
    "darkgrey": "A9A9A9",
    "silver": "C0C0C0",
    "gold": "FFD700",
    "cyan": "00FFFF",
    "aqua": "00FFFF",
    "magenta": "FF00FF",
    "fuchsia": "FF00FF",
    "lime": "00FF00",
    "navy": "000080",
    "teal": "008080",
    "olive": "808000",
    "maroon": "800000",
    "violet": "EE82EE",
    "indigo": "4B0082",
    "turquoise": "40E0D0",
    "coral": "FF7F50",
    "salmon": "FA8072",
    "khaki": "F0E68C",
    "beige": "F5F5DC",
    "ivory": "FFFFF0",
    "lavender": "E6E6FA",
    "plum": "DDA0DD",
    "tan": "D2B48C",
    "wheat": "F5DEB3",
    "mint": "98FF98",
    "mintcream": "F5FFFA",
    "skyblue": "87CEEB",
    "steelblue": "4682B4",
    "royalblue": "4169E1",
    "dodgerblue": "1E90FF",
    "deepskyblue": "00BFFF",
    "powderblue": "B0E0E6",
    "lightblue": "ADD8E6",
    "darkblue": "00008B",
    "darkgreen": "006400",
    "lightgreen": "90EE90",
    "palegreen": "98FB98",
    "seagreen": "2E8B57",
    "springgreen": "00FF7F",
    "forestgreen": "228B22",
    "darkred": "8B0000",
    "firebrick": "B22222",
    "crimson": "DC143C",
    "tomato": "FF6347",
    "orangered": "FF4500",
    "darkorange": "FF8C00",
    "lightyellow": "FFFFE0",
    "lemonchiffon": "FFFACD",
    "lightgoldenrodyellow": "FAFAD2",
    "darkkhaki": "BDB76B",
    "darkviolet": "9400D3",
    "darkorchid": "9932CC",
    "orchid": "DA70D6",
    "hotpink": "FF69B4",
    "deeppink": "FF1493",
    "lightpink": "FFB6C1",
    "peachpuff": "FFDAB9",
    "seashell": "FFF5EE",
    "linen": "FAF0E6",
    "snow": "FFFAFA",
    "whitesmoke": "F5F5F5",
    "ghostwhite": "F8F8FF",
    "aliceblue": "F0F8FF",
    "honeydew": "F0FFF0",
    "azure": "F0FFFF",
    # Slovak / colloquial conveniences.
    "cierna": "000000",
    "čierna": "000000",
    "biela": "FFFFFF",
    "cervena": "FF0000",
    "červená": "FF0000",
    "zelena": "008000",
    "zelená": "008000",
    "modra": "0000FF",
    "modrá": "0000FF",
}


def _clean_hex(value: str) -> str:
    value = value.strip()
    if value.startswith("#"):
        value = value[1:]
    if value.lower().startswith("0x"):
        value = value[2:]
    return value


def normalize_rgb(color: ColorLike, *, none_ok: bool = True) -> str | None:
    """Return RRGGBB, or None for transparent/no color.

    Accepts '#RRGGBB', 'RRGGBB', '#RGB', 'RGB', common named colors, or
    (r, g, b) tuples/lists.
    """
    if color is None:
        if none_ok:
            return None
        raise ValueError("Color cannot be None here")

    if isinstance(color, (tuple, list)):
        if len(color) != 3:
            raise ValueError(f"RGB color must have exactly 3 numbers, got {color!r}")
        parts: list[int] = []
        for item in color:
            if not isinstance(item, int):
                raise TypeError(f"RGB color values must be integers, got {color!r}")
            if not 0 <= item <= 255:
                raise ValueError(f"RGB color values must be in 0..255, got {color!r}")
            parts.append(item)
        return "".join(f"{p:02X}" for p in parts)

    if not isinstance(color, str):
        raise TypeError(f"Unsupported color value: {color!r}")

    key = color.strip().lower().replace(" ", "").replace("_", "").replace("-", "")
    if key in NAMED_COLORS:
        rgb = NAMED_COLORS[key]
        return rgb or None

    raw = _clean_hex(color)
    if len(raw) == 3 and all(ch in "0123456789abcdefABCDEF" for ch in raw):
        raw = "".join(ch * 2 for ch in raw)
    if len(raw) == 6 and all(ch in "0123456789abcdefABCDEF" for ch in raw):
        return raw.upper()
    if len(raw) == 8 and all(ch in "0123456789abcdefABCDEF" for ch in raw):
        # Accept AARRGGBB / RRGGBBAA-ish input by keeping the last 6 as RGB.
        return raw[-6:].upper()

    raise ValueError(f"Unknown color {color!r}. Use '#RRGGBB', (r,g,b), or a named color.")


def to_argb(color: ColorLike, *, none_ok: bool = True) -> str | None:
    rgb = normalize_rgb(color, none_ok=none_ok)
    if rgb is None:
        return None
    return "FF" + rgb

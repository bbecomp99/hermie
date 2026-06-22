"""Tiny pure-stdlib line-chart → PNG renderer for Argus Mattermost alerts.

No matplotlib / PIL / numpy — Argus ships zero pip deps, so the chart is drawn
straight onto an RGB framebuffer and encoded as a truecolour PNG with `zlib`.
The look matches the dashboard's dark "Obsidian" theme: slate background, faint
gridlines, a soft filled area under an accent-coloured trend line, and a bright
marker on the latest sample. `line_png(values, accent=...)` returns PNG bytes.
"""
import struct
import zlib

# Theme (RGB). Background + gridlines mirror the Tailwind slate palette the
# dashboard uses; the accent (line/fill/marker) is supplied per alert.
BG = (15, 23, 42)        # slate-900
GRID = (30, 41, 59)      # slate-800
AXIS = (51, 65, 85)      # slate-700


def _blend(fg, bg, a):
    """Alpha-composite fg over bg at opacity a (0..1) — fakes translucency on an
    opaque RGB buffer (no alpha channel to keep the PNG small/simple)."""
    return tuple(round(f * a + b * (1 - a)) for f, b in zip(fg, bg))


def _encode_png(width, height, buf):
    """RGB framebuffer (bytearray, width*height*3) → PNG bytes (colour type 2)."""
    def chunk(typ, data):
        body = typ + data
        return (struct.pack(">I", len(data)) + body
                + struct.pack(">I", zlib.crc32(body) & 0xffffffff))

    raw = bytearray()
    stride = width * 3
    for y in range(height):
        raw.append(0)                       # filter type 0 (None) per scanline
        raw += buf[y * stride:(y + 1) * stride]
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
            + chunk(b"IEND", b""))


def line_png(values, accent=(96, 165, 250), width=600, height=170, scale=2):
    """Render `values` (a numeric series, ≥2 points) as a filled trend line.

    `accent` is the line/fill/marker colour (RGB). Rendered at `scale`× for crisp
    downscaling in chat clients. Returns PNG bytes, or None when there's nothing
    meaningful to draw."""
    vals = [float(v) for v in values if isinstance(v, (int, float))]
    if len(vals) < 2:
        return None

    W, H = width * scale, height * scale
    pad = 8 * scale
    plot_w, plot_h = W - 2 * pad, H - 2 * pad
    buf = bytearray(bytes(BG) * (W * H))

    def px(x, y, color):
        if 0 <= x < W and 0 <= y < H:
            i = (y * W + x) * 3
            buf[i:i + 3] = bytes(color)

    # Horizontal gridlines at 25/50/75/100%.
    for f in (0.0, 0.25, 0.5, 0.75, 1.0):
        gy = pad + round(f * plot_h)
        color = AXIS if f in (0.0, 1.0) else GRID
        for gx in range(pad, pad + plot_w):
            px(gx, gy, color)

    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    n = len(vals)
    fill = _blend(accent, BG, 0.20)
    thickness = max(1, scale)            # half-width of the line in px

    def y_at(col):
        pos = col / (plot_w - 1) * (n - 1)
        i = int(pos)
        frac = pos - i
        v = vals[i] if i + 1 >= n else vals[i] + (vals[i + 1] - vals[i]) * frac
        return pad + round((1 - (v - lo) / span) * plot_h)

    # Filled area + line, column by column (continuous, no segment gaps).
    last = None
    for c in range(plot_w):
        x = pad + c
        y = y_at(c)
        for fy in range(y, pad + plot_h):       # area under the curve
            px(x, fy, fill)
        lo_y, hi_y = (y, last) if last is not None and last >= y else (last, y)
        if last is None:
            lo_y = hi_y = y
        for yy in range(lo_y - thickness, hi_y + thickness + 1):  # the line itself
            px(x, yy, accent)
        last = y

    # Bright marker on the most recent sample.
    mx, my = pad + plot_w - 1, y_at(plot_w - 1)
    r = 3 * scale
    for dx in range(-r, r + 1):
        for dy in range(-r, r + 1):
            if dx * dx + dy * dy <= r * r:
                px(mx + dx, my + dy, (241, 245, 249))   # slate-100 dot
    for dx in range(-r, r + 1):
        for dy in range(-r, r + 1):
            d = dx * dx + dy * dy
            if (r - scale) ** 2 <= d <= r * r:
                px(mx + dx, my + dy, accent)             # accent ring

    return _encode_png(W, H, buf)

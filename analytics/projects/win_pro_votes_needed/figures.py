"""Charts shared by the DATA-2809 and DATA-2810 findings pages, so the same finding draws the same way on both."""

from __future__ import annotations

import html

import pandas as pd
from analyze import BANDS, BUNDLE_TEXTS, over_range

esc = html.escape
VOL_MAX = 20000  # axis end for median texts; the 10,000+ band median sits near 17,000

# Page CSS for the charts below; each page includes it once in its stylesheet.
CSS = """
.rows.hero.vol { grid-template-columns: 110px minmax(160px, 1fr) 60px 170px; min-width: 520px; }
.rows .hd { color: var(--muted); font-size: 11px; font-weight: 500; white-space: normal; }
.rows .val.wide { text-align: left; }
.track.bar .bar { position: absolute; left: 0; top: 2px; height: 10px; background: var(--blue); border-radius: 0 2px 2px 0; }
.track.bar::before { display: none; }
.track.bar .ref { z-index: 1; }
h3.ctitle { font: 600 15px/1.3 var(--sans); margin: 0; }
"""


def _vol_row(ws: pd.DataFrame, b: str) -> str:
    t = ws[(ws["band"] == b) & ws["texted"]]
    k = t[t["texts_known"]]["texts_sent"]
    med = k.median()
    tip = esc(
        f"{b} votes needed: median {med:,.0f} texts | {len(k)} texters with a known count | "
        f"over {BUNDLE_TEXTS:,}: {over_range(t)} of {len(t)} texters"
    )
    line = f'<i class="ref" style="left:{BUNDLE_TEXTS / VOL_MAX * 100:.1f}%"></i>'
    return (
        f'<div class="lab">{esc(b)}</div>'
        f'<div class="track bar" title="{tip}">{line}<i class="bar" style="width:{min(med, VOL_MAX) / VOL_MAX * 100:.1f}%"></i></div>'
        f'<div class="val" title="{tip}">{med:,.0f}</div>'
        f'<div class="val wide" title="{tip}">{esc(over_range(t))}</div>'
    )


def volume_chart(ws: pd.DataFrame) -> str:
    """Median texts per texter by votes needed, against the 5,000-text line, with the share over it."""
    return (
        '<figure class="dots"><h3 class="ctitle">Median texts per texter, by votes needed to win</h3>'
        '<div class="scroll"><div class="rows hero vol">'
        '<div class="lab hd">Votes needed</div><div class="lab hd">Median texts per texter <span class="sep">|</span> dashed line: 5,000</div>'
        '<div class="val hd">Texts</div><div class="val wide hd">Texters over 5,000</div>'
        + "".join(_vol_row(ws, b) for b in BANDS if b != "unknown")
        + '</div></div><div class="axis-note">Texters with a known count, six months up to election day. Bars run 0 to 20,000 texts. '
        "Hover for counts. A range because some texters have no recorded count; see the note below.</div></figure>"
    )

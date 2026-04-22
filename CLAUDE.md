# Kevin Dashboard

A daily-refreshing revenue + IVT dashboard built from a public Google Sheet. Single-file
Python ETL + self-contained HTML output. Designed to be regenerated each morning by an
unattended scheduled task.

## Source

Google Sheet (publicly viewable as of 2026-04-22):
https://docs.google.com/spreadsheets/d/1J0K3ADKoXVrT8sfFeVJNsQKN5qm2o1TO1IBoRvpeG5o/edit

Sheet ID is hard-coded as `SHEET_ID` at the top of `build_dashboard.py`.

The sheet must stay shared as "Anyone with the link → Viewer." The script downloads via
the public xlsx export endpoint with no auth.

## Files

| File | Purpose | Regenerated? |
|------|---------|--------------|
| `build_dashboard.py` | ETL + dashboard renderer (the only source file) | No — edit this |
| `kevin_dashboard.html` | Self-contained dashboard with embedded JSON data | Every run |
| `source.xlsx` | Cached download of the source sheet | Every run (unless `--no-download`) |
| `CLAUDE.md` | This file | Manual |

## Run

```sh
# Full refresh: download latest sheet, regenerate HTML
python3 build_dashboard.py

# Or write output to a different folder
python3 build_dashboard.py /path/to/output

# Use cached source.xlsx without re-downloading (fast iteration on UI changes)
python3 build_dashboard.py . --no-download
```

Dependency: `openpyxl` (only). `pip3 install openpyxl`.

## Architecture

`build_dashboard.py` does four things in sequence:

1. **Download** the sheet as `.xlsx` (urllib, no auth, public export URL).
2. **Parse** every property tab (anything not in `NON_PROPERTY_TABS`) into long-form
   rows: `{date, property_tab, property_label, oem_group, ad_revenue, page_views, ...}`.
   Also parses the `IVT Report` tab into `{date, origin_domain, impressions, fraud, fraud_pct, ...}`.
3. **Map** IVT origin domains to property tabs via `build_domain_property_map`. Strategy:
   exact root match → manual `overrides` dict → substring fuzzy match. Returns matched
   map + list of unmatched domains (third-party publishers, surfaced in dashboard).
4. **Render** a single HTML file with all data embedded as JSON. UI is vanilla JS + Chart.js.

The "Revenue" tab in the source workbook is intentionally **skipped** — it's a wide
month-by-month pivot that's painful to parse. Per-property tabs are aggregated instead;
totals match the Revenue tab within rounding.

## Source workbook quirks (read before editing the parser)

- Property tabs share a common 11-column schema starting at the row whose first cell
  is "Date". Some tabs have a junk header row above; `parse_property_tab` skips down to
  the "Date" header row.
- Many cells are the literal string `'#DIV/0!'` — `to_float` coerces these to None.
- OEM group is encoded in the tab-name prefix: `OEM-`, `OEM2-`, `OEM4-`, `OEM5-`. Tabs
  without a number ("OEM- Flavor Feed") are normalized to group `OEM1`. "Pop Dash" has
  no OEM prefix and falls into `OTHER`.
- `IVT Report` is keyed by `Date + Origin Domain`. The `<blank>` domain is real data —
  ad serves with no origin domain attribution. Keep it; don't drop the row.
- Tabs ignored entirely: `Revenue`, `IVT Report` (parsed separately), `Sheet31`,
  `Platform`, `GEO`, `Conversion Data`. See `NON_PROPERTY_TABS`.

## Domain → property mapping

In `build_domain_property_map`:

- Property comparison key: `clean_property_label(tab) → lowercase → strip non-alphanumeric`.
- Domain comparison key: strip `www./https://`, take everything before first `.`,
  strip non-alphanumeric.
- Manual `overrides` dict handles cases where the simple key match fails (e.g.,
  `clickticks` → `OEM5- Click Ticks`, `dailydash` → `OEM4 -Daily Dash`).
- 52 of ~79 unique IVT domains currently match. The other ~31 are external publisher
  domains (cdpn.io, kagiproxy.com, appinstallcheck.com, etc.) that won't ever map to a
  property tab; the dashboard shows them in a collapsed "unmatched" section.

If new property tabs appear in the source sheet and IVT starts referencing them, add a
mapping to `overrides` if fuzzy match doesn't catch it.

## Verification

When changing parser logic, verify against the source workbook before shipping:

```python
import json, re, openpyxl, datetime as dt
from pathlib import Path
html = Path("kevin_dashboard.html").read_text()
data = json.loads(re.search(r"const DATA = (\{.*?\});", html, re.DOTALL).group(1))

wb = openpyxl.load_workbook("source.xlsx", data_only=True)
# Pick a property tab + date and compare payload value to the cell in the source.
```

Spot-checks done so far: bearfing daily revenue matches source to 4 decimals across
multiple sample dates; per-month totals across all property tabs are sane and
monotonically reasonable.

## Dashboard UI

Single self-contained HTML, no build step. Sections:

- Filters: date preset (7/30/90/all/custom), OEM-group multiselect, property multiselect.
- KPI tiles: revenue, page views, clicks, CTR, RPM, IVT impressions, network fraud %.
- Two charts: daily revenue (multi-line when ≤10 properties selected, area otherwise)
  and network IVT fraud % + impressions (dual axis).
- "By date" summary table, sortable.
- "By property" breakout table with per-property fraud % when domain matched.
- **Property × Day matrix**: rows = properties, columns = dates, metric selectable
  (revenue / views / clicks / CTR / RPM / fraud %). Sticky property column + total.
  Heatmap shading toggleable.
- **Property daily detail**: long-form sortable table, capped at 500 rows.
- Collapsed unmatched-IVT-domains list.

Chart.js loaded from CDN (`cdnjs.cloudflare.com`). No other external assets.

## Design decisions already locked

- Output format: HTML, not Excel. (User considered both; HTML chosen for refresh-once,
  view-many.)
- Refresh: scheduled task that re-downloads + rebuilds. (Not Power Query, not manual.)
- IVT linkage: BOTH network rollup AND per-property when domain matches. (Not one or
  the other.)
- Date scope: filterable in the UI; dataset always includes full history.
- Single-file script. No package, no config, no env vars.

## Daily refresh

Set up via whatever scheduler the host machine has (cron, launchd, Task Scheduler, or
Cowork's scheduled tasks). Trigger:

```sh
python3 /full/path/to/build_dashboard.py /full/path/to/output/folder
```

If download fails, the script raises and exits non-zero before touching the existing
HTML — yesterday's dashboard stays in place.

## Open items / things future-me might want

- No persistence layer. If you want week-over-week comparisons that survive the source
  sheet being edited retroactively, you'll need to snapshot daily into a SQLite or
  similar.
- The "By property" breakout uses a single date range; there's no built-in YoY or
  WoW delta. Adding one means joining the row aggregate against an offset window.
- vRPM column in the breakout currently shows "—" because the per-day aggregation logic
  doesn't roll up vRPM (it's a derived metric requiring a weighted calc). Add if needed.
- The matrix can get very wide for "all" date range. Consider auto-bucketing to weeks
  when range > 60 days.

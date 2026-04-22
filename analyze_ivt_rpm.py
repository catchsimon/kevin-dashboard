#!/usr/bin/env python3
"""
IVT × RPM correlation report.

Reuses the parsing logic in build_dashboard.py to load revenue and IVT data
from source.xlsx, joins them at the (property, date) grain where the IVT
origin domain matched a property tab, computes correlations at three levels,
and emits a standalone HTML report.
"""
from __future__ import annotations

import json
import math
import sys
import importlib.util
from pathlib import Path
from collections import defaultdict

HERE = Path(__file__).resolve().parent

# Reuse the dashboard parser (single source of truth)
spec = importlib.util.spec_from_file_location("bd", str(HERE / "build_dashboard.py"))
bd = importlib.util.module_from_spec(spec); spec.loader.exec_module(bd)

import openpyxl


def pearson(xs: list[float], ys: list[float]) -> tuple[float, int]:
    """Pearson correlation; returns (r, n)."""
    n = len(xs)
    if n < 3:
        return float("nan"), n
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return float("nan"), n
    return num / (dx * dy), n


def t_pvalue(r: float, n: int) -> float:
    """Two-sided p-value for a Pearson r via Student-t approx."""
    if n < 4 or math.isnan(r) or abs(r) >= 1:
        return float("nan")
    t = r * math.sqrt((n - 2) / (1 - r * r))
    # Two-sided survival via a numerical approximation of the t-distribution.
    df = n - 2
    # Use Hill's approximation -> normal approx for moderate df
    # Convert t -> approximate two-sided p via 2*(1-Phi(|t|))
    z = t * (1 - 1 / (4 * df)) / math.sqrt(1 + t * t / (2 * df))
    # Phi via erf
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))
    return p


def load_data(src_xlsx: Path):
    wb = openpyxl.load_workbook(src_xlsx, data_only=True)
    revenue = []
    properties = {}
    for tab in wb.sheetnames:
        if tab in bd.NON_PROPERTY_TABS:
            continue
        rows = bd.parse_property_tab(wb[tab])
        if not rows:
            continue
        oem = bd.parse_oem_group(tab)
        label = bd.clean_property_label(tab)
        properties[tab] = {"tab": tab, "label": label, "oem_group": oem}
        for r in rows:
            r["property_tab"] = tab
            r["property_label"] = label
            r["oem_group"] = oem
            revenue.append(r)
    ivt = bd.parse_ivt_tab(wb["IVT Report"])
    domains = sorted({r["origin_domain"] for r in ivt})
    domain_map, _ = bd.build_domain_property_map(list(properties), domains)
    for r in ivt:
        r["matched_property_tab"] = domain_map.get(r["origin_domain"])
    return revenue, ivt, properties


def build(src: Path, out_html: Path) -> dict:
    revenue, ivt, properties = load_data(src)

    # ------- (property, date) join -------
    # Aggregate IVT to (property, date)
    ivt_pd: dict[tuple[str, str], dict] = {}
    for r in ivt:
        t = r.get("matched_property_tab")
        if not t:
            continue
        k = (t, r["date"])
        d = ivt_pd.setdefault(k, {"impressions": 0.0, "fraud": 0.0})
        d["impressions"] += r["impressions"] or 0
        d["fraud"] += r["fraud"] or 0

    # Build joined records
    joined: list[dict] = []
    for r in revenue:
        if r["property_tab"] not in {t for (t, _) in ivt_pd.keys()} \
                and (r["property_tab"], r["date"]) not in ivt_pd:
            continue
        i = ivt_pd.get((r["property_tab"], r["date"]))
        if not i or not i["impressions"]:
            continue
        if not r.get("page_views") or r["page_views"] < 100:
            continue  # RPM is not meaningful with tiny volume
        if not r.get("ad_revenue") or r["ad_revenue"] <= 0:
            continue  # no monetization on this day
        rpm = (r["ad_revenue"] / r["page_views"]) * 1000
        fraud_pct = i["fraud"] / i["impressions"] if i["impressions"] else None
        if fraud_pct is None:
            continue
        joined.append({
            "date": r["date"],
            "property_tab": r["property_tab"],
            "property_label": r["property_label"],
            "oem_group": r["oem_group"],
            "page_views": r["page_views"],
            "ad_clicks": r.get("ad_clicks") or 0,
            "ad_revenue": r["ad_revenue"],
            "rpm": rpm,
            "impressions": i["impressions"],
            "fraud": i["fraud"],
            "fraud_pct": fraud_pct,
        })

    # ------- Level 1: Network daily -------
    by_day: dict[str, dict] = {}
    for r in joined:
        d = by_day.setdefault(r["date"], {
            "ad_revenue": 0.0, "page_views": 0.0, "impressions": 0.0, "fraud": 0.0,
        })
        d["ad_revenue"] += r["ad_revenue"]
        d["page_views"] += r["page_views"]
        d["impressions"] += r["impressions"]
        d["fraud"] += r["fraud"]
    network_daily = []
    for date, v in sorted(by_day.items()):
        if v["page_views"] < 1000 or v["impressions"] < 100:
            continue
        network_daily.append({
            "date": date,
            "rpm": (v["ad_revenue"] / v["page_views"]) * 1000,
            "fraud_pct": v["fraud"] / v["impressions"],
            "page_views": v["page_views"],
            "impressions": v["impressions"],
        })
    nd_r, nd_n = pearson(
        [x["fraud_pct"] for x in network_daily],
        [x["rpm"] for x in network_daily],
    )
    nd_p = t_pvalue(nd_r, nd_n)

    # ------- Level 2: Per-property aggregate -------
    by_prop: dict[str, dict] = {}
    for r in joined:
        p = by_prop.setdefault(r["property_tab"], {
            "label": r["property_label"], "oem_group": r["oem_group"],
            "ad_revenue": 0.0, "page_views": 0.0, "impressions": 0.0, "fraud": 0.0,
            "days": 0,
        })
        p["ad_revenue"] += r["ad_revenue"]
        p["page_views"] += r["page_views"]
        p["impressions"] += r["impressions"]
        p["fraud"] += r["fraud"]
        p["days"] += 1
    prop_agg = []
    for tab, v in by_prop.items():
        if v["days"] < 5 or v["page_views"] < 1000 or v["impressions"] < 100:
            continue
        prop_agg.append({
            "tab": tab, "label": v["label"], "oem_group": v["oem_group"],
            "days": v["days"],
            "rpm": (v["ad_revenue"] / v["page_views"]) * 1000,
            "fraud_pct": v["fraud"] / v["impressions"],
            "ad_revenue": v["ad_revenue"],
            "page_views": v["page_views"],
            "impressions": v["impressions"],
        })
    pa_r, pa_n = pearson(
        [x["fraud_pct"] for x in prop_agg],
        [x["rpm"] for x in prop_agg],
    )
    pa_p = t_pvalue(pa_r, pa_n)

    # ------- Level 3: Within-property (centered per-property), all (prop,day) -------
    # Subtract each property's mean fraud_pct and mean rpm from its records,
    # then compute pooled correlation -> "within-property" effect.
    within_xs: list[float] = []
    within_ys: list[float] = []
    per_prop_corr = []
    for tab, recs in _group_by(joined, "property_tab").items():
        if len(recs) < 7:
            continue
        fp = [r["fraud_pct"] for r in recs]
        rpm = [r["rpm"] for r in recs]
        mfp = sum(fp) / len(fp)
        mrpm = sum(rpm) / len(rpm)
        for f, r in zip(fp, rpm):
            within_xs.append(f - mfp)
            within_ys.append(r - mrpm)
        r_p, n_p = pearson(fp, rpm)
        per_prop_corr.append({
            "tab": tab,
            "label": recs[0]["property_label"],
            "oem_group": recs[0]["oem_group"],
            "n": n_p,
            "r": r_p,
            "mean_rpm": mrpm,
            "mean_fraud": mfp,
        })
    wi_r, wi_n = pearson(within_xs, within_ys)
    wi_p = t_pvalue(wi_r, wi_n)

    # Sort per-property correlations by absolute strength
    per_prop_corr.sort(key=lambda x: -abs(x["r"]) if not math.isnan(x["r"]) else 0)

    # ------- Threshold analysis: bin fraud % and compute RPM stats -------
    BINS = [
        (0.00, 0.01,  "0–1%"),
        (0.01, 0.025, "1–2.5%"),
        (0.025, 0.05, "2.5–5%"),
        (0.05, 0.10,  "5–10%"),
        (0.10, 0.20,  "10–20%"),
        (0.20, 1.01,  "20%+"),
    ]
    def bin_for(fp):
        for lo, hi, label in BINS:
            if lo <= fp < hi:
                return label
        return None

    # Cross-sectional bins: every (property, day) record
    cross_bins = {label: {"rpms": [], "page_views": 0.0, "ad_revenue": 0.0,
                          "impressions": 0.0, "fraud": 0.0}
                  for _, _, label in BINS}
    for r in joined:
        b = bin_for(r["fraud_pct"])
        if not b:
            continue
        bd = cross_bins[b]
        bd["rpms"].append(r["rpm"])
        bd["page_views"] += r["page_views"]
        bd["ad_revenue"] += r["ad_revenue"]
        bd["impressions"] += r["impressions"]
        bd["fraud"] += r["fraud"]
    cross_bin_rows = []
    for _, _, label in BINS:
        b = cross_bins[label]
        rpms = sorted(b["rpms"])
        n = len(rpms)
        if not n:
            cross_bin_rows.append({"bin": label, "n": 0})
            continue
        weighted_rpm = (b["ad_revenue"] / b["page_views"]) * 1000 if b["page_views"] else None
        cross_bin_rows.append({
            "bin": label,
            "n": n,
            "mean_rpm": sum(rpms) / n,
            "median_rpm": rpms[n // 2] if n % 2 else (rpms[n // 2 - 1] + rpms[n // 2]) / 2,
            "weighted_rpm": weighted_rpm,
            "actual_fraud_pct": b["fraud"] / b["impressions"] if b["impressions"] else None,
            "page_views": b["page_views"],
            "ad_revenue": b["ad_revenue"],
        })
    # Baseline (lowest bin with data) and % of baseline for each
    baseline_w = next((r["weighted_rpm"] for r in cross_bin_rows
                       if r.get("weighted_rpm") and r["n"] > 0), None)
    for r in cross_bin_rows:
        if r["n"] and baseline_w:
            r["pct_of_baseline"] = (r["weighted_rpm"] / baseline_w) if r.get("weighted_rpm") else None

    # Within-property bins: normalize each property-day's RPM to that property's mean,
    # then average the normalized RPM per bin. Removes property-level confound.
    prop_mean_rpm: dict[str, float] = {}
    prop_recs = _group_by(joined, "property_tab")
    for tab, recs in prop_recs.items():
        if len(recs) < 5:
            continue
        prop_mean_rpm[tab] = sum(r["rpm"] for r in recs) / len(recs)
    within_bins = {label: {"vals": []} for _, _, label in BINS}
    for r in joined:
        if r["property_tab"] not in prop_mean_rpm:
            continue
        m = prop_mean_rpm[r["property_tab"]]
        if m <= 0:
            continue
        b = bin_for(r["fraud_pct"])
        if not b:
            continue
        within_bins[b]["vals"].append(r["rpm"] / m)  # 1.0 = at-property-average
    within_bin_rows = []
    for _, _, label in BINS:
        vals = within_bins[label]["vals"]
        n = len(vals)
        if not n:
            within_bin_rows.append({"bin": label, "n": 0})
            continue
        within_bin_rows.append({
            "bin": label,
            "n": n,
            "mean_rel_rpm": sum(vals) / n,  # ratio: 1.0 = at the property's typical RPM
            "median_rel_rpm": sorted(vals)[n // 2],
        })

    # Find the threshold: first bin where weighted RPM drops at least 25% below baseline
    threshold_bin = None
    for r in cross_bin_rows:
        if r["n"] and r.get("pct_of_baseline") is not None and r["pct_of_baseline"] <= 0.75:
            threshold_bin = r["bin"]
            break

    # Per-property scatter for the report (already in per_property)
    return {
        "summary": {
            "joined_rows": len(joined),
            "properties_used": len(prop_agg),
            "network_daily": {"r": nd_r, "n": nd_n, "p": nd_p},
            "per_property_aggregate": {"r": pa_r, "n": pa_n, "p": pa_p},
            "within_property_pooled": {"r": wi_r, "n": wi_n, "p": wi_p},
            "date_range": [
                min((r["date"] for r in joined), default=None),
                max((r["date"] for r in joined), default=None),
            ],
        },
        "network_daily": network_daily,
        "per_property": prop_agg,
        "per_property_corr": per_prop_corr,
        "all_pairs": joined,
        "threshold": {
            "bins": cross_bin_rows,
            "within_bins": within_bin_rows,
            "baseline_weighted_rpm": baseline_w,
            "threshold_bin": threshold_bin,
        },
    }


def _group_by(rows, key):
    out = defaultdict(list)
    for r in rows:
        out[r[key]].append(r)
    return out


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------

def render(report: dict, out_html: Path):
    payload = json.dumps(report, default=str)
    html = HTML.replace("__DATA_JSON__", payload)
    out_html.write_text(html, encoding="utf-8")


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8">
<title>IVT × RPM correlation report</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  :root { --bg:#0f1115; --panel:#181b22; --panel2:#1f232c; --line:#2a2f3a;
    --text:#e8eaf0; --muted:#8b93a7; --accent:#6aa9ff; --good:#4ade80;
    --warn:#fbbf24; --bad:#f87171; }
  * { box-sizing:border-box; }
  html, body { margin:0; padding:0; background:var(--bg); color:var(--text);
    font:14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  header { padding:20px 28px; border-bottom:1px solid var(--line); }
  header h1 { margin:0; font-size:20px; font-weight:600; letter-spacing:-0.01em;}
  header .sub { color:var(--muted); font-size:13px; margin-top:4px;}
  main { padding:24px 28px 60px; max-width:1200px; margin:0 auto; }
  h2 { font-size:15px; text-transform:uppercase; letter-spacing:0.06em; color:var(--muted);
       margin:36px 0 12px; font-weight:600;}
  .card { background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px 22px; margin-bottom:18px;}
  .filter-card { padding:14px 22px; }
  .preset-buttons { display:flex; gap:6px; flex-wrap:wrap;}
  .preset-buttons button { background:var(--panel2); color:var(--text); border:1px solid var(--line);
    border-radius:6px; padding:6px 12px; cursor:pointer; font:inherit; font-size:13px; }
  .preset-buttons button:hover { border-color:var(--accent); }
  .preset-buttons button.active { background:var(--accent); color:#0a1020; border-color:var(--accent); font-weight:600; }
  .verdict { font-size:18px; line-height:1.5; }
  .verdict .strong { font-weight:700; }
  .verdict .pos { color:var(--good); }
  .verdict .neg { color:var(--bad); }
  .verdict .neu { color:var(--warn); }
  .levels { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px,1fr)); gap:14px; margin-top:16px;}
  .level { background:var(--panel2); border:1px solid var(--line); border-radius:10px; padding:14px;}
  .level .label { font-size:11px; text-transform:uppercase; color:var(--muted); letter-spacing:0.05em;}
  .level .r { font-size:28px; font-weight:700; margin-top:6px; font-variant-numeric: tabular-nums;}
  .level .meta { font-size:12px; color:var(--muted); margin-top:4px;}
  .chart-wrap { position:relative; height:340px; }
  table { width:100%; border-collapse:collapse; font-size:13px;}
  th, td { text-align:left; padding:7px 9px; border-bottom:1px solid var(--line);}
  th { font-size:11px; text-transform:uppercase; color:var(--muted); letter-spacing:0.05em;
       position:sticky; top:0; background:var(--panel); z-index:1;}
  table.sortable th[data-k] { cursor:pointer; user-select:none; }
  table.sortable th[data-k]:hover { color:var(--text); }
  table.sortable th .arr { display:inline-block; width:10px; margin-left:4px; opacity:0.4; font-size:10px;}
  table.sortable th.sort-asc .arr,
  table.sortable th.sort-desc .arr { opacity:1; color:var(--accent);}
  td.num, th.num { text-align:right; font-variant-numeric: tabular-nums; }
  .scrollx { overflow-x:auto; max-height:520px; overflow-y:auto;}
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600;}
  .pill.muted { background:rgba(139,147,167,0.18); color:var(--muted);}
  .pill.pos { background:rgba(74,222,128,0.18); color:var(--good);}
  .pill.neg { background:rgba(248,113,113,0.18); color:var(--bad);}
  .pill.neu { background:rgba(139,147,167,0.18); color:var(--muted);}
  .grid-2 { display:grid; grid-template-columns: 1fr 1fr; gap:18px;}
  @media (max-width:1000px) { .grid-2 { grid-template-columns: 1fr; } }
  .legend { font-size:12px; color:var(--muted); margin-top:6px; }
  details { margin-top:8px; }
  details summary { cursor:pointer; color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:0.05em;}
</style>
</head><body>
<header>
  <h1>IVT × RPM — correlation analysis</h1>
  <div class="sub" id="sub"></div>
</header>
<main>
  <div class="card filter-card">
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
      <span style="font-size:11px;text-transform:uppercase;letter-spacing:0.05em;color:var(--muted);">Date range</span>
      <div class="preset-buttons" id="presets">
        <button data-d="yesterday">Yesterday</button>
        <button data-d="7">Last 7d</button>
        <button data-d="14">Last 14d</button>
        <button data-d="30">Last 30d</button>
        <button data-d="90">Last 90d</button>
        <button data-d="all" class="active">All</button>
      </div>
      <div style="display:flex;gap:8px;align-items:center;">
        <label style="font-size:11px;color:var(--muted);text-transform:uppercase;">From</label>
        <input type="date" id="from" style="background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:6px;padding:6px 8px;font:inherit;">
        <label style="font-size:11px;color:var(--muted);text-transform:uppercase;">To</label>
        <input type="date" id="to" style="background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:6px;padding:6px 8px;font:inherit;">
      </div>
      <span id="filterMeta" style="margin-left:auto;font-size:12px;color:var(--muted);"></span>
    </div>
  </div>

  <div class="card">
    <div class="verdict" id="verdict"></div>
    <div class="levels" id="levels"></div>
    <div class="legend" style="margin-top:14px;">
      Pearson <em>r</em> ranges from −1 (perfectly inverse) to +1 (perfectly positive); <em>0</em> means no linear relationship.
      <em>p</em> is the two-sided significance — values below 0.05 are conventionally "statistically significant" given the sample size.
    </div>
  </div>

  <h2>Threshold — at what fraud % does RPM drop?</h2>
  <div class="card">
    <div id="thresholdHeadline" style="font-size:16px;line-height:1.5;"></div>
    <div class="grid-2" style="margin-top:14px;">
      <div>
        <div class="chart-wrap" style="height:300px;"><canvas id="binChart"></canvas></div>
        <div class="legend">Volume-weighted RPM by fraud-% bucket. The bar height shows actual revenue-per-1000-views earned in each bucket. The dashed line is the lowest-fraud bucket's baseline.</div>
      </div>
      <div>
        <div class="chart-wrap" style="height:300px;"><canvas id="withinBinChart"></canvas></div>
        <div class="legend">Within-property: each day's RPM divided by that property's average RPM, then averaged per bucket. 1.0 = "at this property's typical RPM"; 0.75 = "25% below typical". Removes the cross-property confound.</div>
      </div>
    </div>
    <div class="scrollx" style="margin-top:14px;">
      <table id="binTable" class="sortable">
        <thead><tr>
          <th data-k="bin_order">Fraud %</th>
          <th class="num" data-k="n">n property-days</th>
          <th class="num" data-k="weighted_rpm">Weighted RPM</th>
          <th class="num" data-k="mean_rpm">Mean RPM</th>
          <th class="num" data-k="median_rpm">Median RPM</th>
          <th class="num" data-k="pct_of_baseline">% of baseline</th>
          <th class="num" data-k="page_views">Page views</th>
          <th class="num" data-k="ad_revenue">Revenue</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <h2>Network daily — fraud % vs. RPM</h2>
  <div class="card grid-2">
    <div>
      <div class="chart-wrap"><canvas id="ndScatter"></canvas></div>
      <div class="legend">Each dot is one day across all properties combined. The volume-weighted network fraud % vs. the network RPM that day.</div>
    </div>
    <div>
      <div class="chart-wrap"><canvas id="ndTime"></canvas></div>
      <div class="legend">Same data over time — fraud % (left axis) and RPM (right axis).</div>
    </div>
  </div>

  <h2>Advertiser demand — what advertisers pay per click</h2>
  <div class="card">
    <div style="font-size:13px;color:var(--muted);margin-bottom:12px;line-height:1.55;">
      <strong style="color:var(--text);">Why this view:</strong>
      RPM = CTR × CPC. A high-RPM property might just have a clicky audience.
      <strong style="color:var(--text);">CPC</strong> (revenue per click) is what advertisers are actually willing
      to pay for this audience — independent of how often users click. The clean-CPC axis below also discounts for fraud,
      since advertisers will re-bid down on inventory they discover is invalid. Properties on the right with clean
      (green) inventory and headroom on the volume axis are the strongest scale candidates.
    </div>
    <div class="chart-wrap" style="height:440px;"><canvas id="demandScatter"></canvas></div>
    <div class="legend" style="display:flex;gap:20px;flex-wrap:wrap;align-items:center;margin-top:10px;">
      <span>One bubble per property.</span>
      <span style="display:flex;align-items:center;gap:6px;">
        <span style="width:8px;height:8px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        <span style="width:22px;height:22px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        bubble size = revenue captured
      </span>
      <span style="display:flex;align-items:center;gap:6px;">
        <span style="width:14px;height:14px;border-radius:50%;background:#f87171;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#fbbf24;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#4ade80;display:inline-block;"></span>
        color = fraud (red 20%+, amber 5–20%, green &lt;5%)
      </span>
      <span><strong style="color:var(--good);">Right + green</strong> = strong advertiser demand on clean inventory.
            <strong style="color:var(--accent);">Right + green + tall (lots of views)</strong> = your highest-leverage scale targets.</span>
    </div>
  </div>

  <h2>Per-property — fraud × RPM × CTR × volume</h2>
  <div class="card">
    <div class="chart-wrap" style="height:420px;"><canvas id="ppScatter"></canvas></div>
    <div class="legend" style="display:flex;gap:20px;flex-wrap:wrap;align-items:center;margin-top:10px;">
      <span>One bubble per property.</span>
      <span style="display:flex;align-items:center;gap:6px;">
        <span style="width:8px;height:8px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        <span style="width:22px;height:22px;border-radius:50%;background:#94a3b8;display:inline-block;"></span>
        bubble size = page-view volume
      </span>
      <span style="display:flex;align-items:center;gap:6px;">
        <span style="width:14px;height:14px;border-radius:50%;background:#f87171;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#fbbf24;display:inline-block;"></span>
        <span style="width:14px;height:14px;border-radius:50%;background:#4ade80;display:inline-block;"></span>
        color = CTR (low → high tertile)
      </span>
      <span><strong style="color:var(--good);">Top-left + green + large</strong> = candidates to scale up.
            <strong style="color:var(--bad);">Bottom-right + red</strong> = audit or cut.</span>
    </div>
  </div>
  <div class="card">
    <div style="font-size:12px;color:var(--muted);margin-bottom:10px;line-height:1.5;">
      <strong style="color:var(--text);">Demand tier</strong> ranks properties by clean CPC = CPC × (1 − fraud %) — the price advertisers
      effectively pay once invalid traffic is discounted. Tertiles within the current filter window:
      <span class="pill pos" style="font-size:10px;">High</span> top third &nbsp;·&nbsp;
      <span class="pill" style="background:rgba(251,191,36,0.18);color:var(--warn);font-size:10px;">Mid</span> middle third &nbsp;·&nbsp;
      <span class="pill" style="background:rgba(139,147,167,0.18);color:var(--muted);font-size:10px;">Low</span> bottom third.
      Hover any tier pill to see the exact CPC math. The score column is just the demand tier as a number, useful for sorting.
      No cut/scale recommendations are made — interpret in context.
    </div>
    <div class="scrollx">
      <table id="propTable" class="sortable">
        <thead><tr>
          <th data-k="oem_group">OEM</th>
          <th data-k="label">Property</th>
          <th class="num" data-k="days">Days</th>
          <th class="num" data-k="cpc">CPC</th>
          <th class="num" data-k="cpc_clean">Clean CPC</th>
          <th class="num" data-k="rpm">RPM</th>
          <th class="num" data-k="ctr">CTR</th>
          <th class="num" data-k="fraud_pct">Fraud %</th>
          <th class="num" data-k="page_views">Page Views</th>
          <th class="num" data-k="ad_revenue">Revenue</th>
          <th class="num" data-k="focus_score">Score</th>
          <th data-k="demand_tier">Demand tier</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <h2>Per-property — within-property correlation strength</h2>
  <div class="card">
    <p style="margin:0 0 10px;color:var(--muted);font-size:13px;">
      For each property with ≥7 observed days, the correlation between that property's daily fraud % and its daily RPM.
      A strong negative <em>r</em> means: at this property, days with more fraud also tend to be days with lower RPM.
    </p>
    <div class="scrollx">
      <table id="ppCorrTable" class="sortable">
        <thead><tr>
          <th data-k="oem_group">OEM</th>
          <th data-k="label">Property</th>
          <th class="num" data-k="n">n days</th>
          <th class="num" data-k="r">r (fraud %, RPM)</th>
          <th class="num" data-k="mean_rpm">mean RPM</th>
          <th class="num" data-k="mean_fraud">mean fraud %</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <details>
    <summary>Methodology &amp; caveats</summary>
    <div class="card" style="background:transparent;border:none;padding:8px 0;">
      <ul style="line-height:1.6;color:var(--muted);">
        <li>Source: revenue from per-property tabs joined to IVT Report by date + matched origin domain (52 of 79 IVT domains map to a property tab; the rest are third-party publishers and excluded).</li>
        <li>Inclusion filter for (property, day) pairs: page views ≥ 100 AND non-zero revenue AND non-zero IVT impressions. Filters guard against meaningless RPM at very low volume.</li>
        <li>RPM = (ad_revenue ÷ page_views) × 1000. Fraud % = fraud impressions ÷ total impressions, taken from the IVT Report.</li>
        <li>Three correlation views: <em>network daily</em> (one row per day, summed across properties), <em>per-property aggregate</em> (one row per property, lifetime totals), <em>within-property pooled</em> (each property centered to its own mean to remove cross-property differences and isolate the day-to-day effect).</li>
        <li><em>p</em>-value uses a Student-t approximation; treat as indicative, not authoritative.</li>
        <li>Causation is not implied. Correlation only indicates that the two metrics move together.</li>
      </ul>
    </div>
  </details>
</main>

<script>
const D = __DATA_JSON__;
const ALL_PAIRS = D.all_pairs;
const DATA_MIN = D.summary.date_range[0];
const DATA_MAX = D.summary.date_range[1];

const fmt = {
  num: (v,d=3) => v==null||Number.isNaN(v)?"—":Number(v).toLocaleString(undefined,{maximumFractionDigits:d, minimumFractionDigits:Math.min(d,2)}),
  pct: v => v==null||Number.isNaN(v)?"—":(v*100).toFixed(2)+"%",
  money: v => v==null||Number.isNaN(v)?"—":"$"+Number(v).toLocaleString(undefined,{maximumFractionDigits:0}),
  int: v => v==null||Number.isNaN(v)?"—":Math.round(v).toLocaleString(),
  r: v => v==null||Number.isNaN(v)?"—":(v>=0?"+":"")+v.toFixed(3),
  p: v => v==null||Number.isNaN(v)?"—":v<0.001?"<0.001":v.toFixed(3),
};

// ---- Stats ----
function pearson(xs, ys) {
  const n = xs.length;
  if (n < 3) return [NaN, n];
  let sx=0, sy=0;
  for (let i=0;i<n;i++){ sx+=xs[i]; sy+=ys[i]; }
  const mx=sx/n, my=sy/n;
  let num=0, dx=0, dy=0;
  for (let i=0;i<n;i++){
    const a=xs[i]-mx, b=ys[i]-my;
    num+=a*b; dx+=a*a; dy+=b*b;
  }
  if (dx===0 || dy===0) return [NaN, n];
  return [num / Math.sqrt(dx*dy), n];
}
function tPvalue(r, n) {
  if (n < 4 || Number.isNaN(r) || Math.abs(r) >= 1) return NaN;
  const df = n - 2;
  const t = r * Math.sqrt(df / (1 - r*r));
  const z = t * (1 - 1/(4*df)) / Math.sqrt(1 + t*t/(2*df));
  return 2 * (1 - 0.5 * (1 + erf(Math.abs(z) / Math.SQRT2)));
}
function erf(x) {
  const sign = x < 0 ? -1 : 1;
  x = Math.abs(x);
  const a1=0.254829592, a2=-0.284496736, a3=1.421413741, a4=-1.453152027, a5=1.061405429, p=0.3275911;
  const t = 1 / (1 + p*x);
  const y = 1 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t * Math.exp(-x*x);
  return sign * y;
}
function classify(r) {
  if (r==null || Number.isNaN(r)) return {pill:"neu", label:"N/A"};
  const a = Math.abs(r);
  if (a < 0.1) return {pill:"neu", label:"essentially none"};
  if (a < 0.3) return {pill:r<0?"neg":"pos", label: (r<0?"weak negative":"weak positive")};
  if (a < 0.5) return {pill:r<0?"neg":"pos", label: (r<0?"moderate negative":"moderate positive")};
  return {pill:r<0?"neg":"pos", label: (r<0?"strong negative":"strong positive")};
}

// ---- Compute analysis bundle from filtered (property × day) records ----
const BIN_SPECS = [
  [0.00, 0.01,  "0–1%"],
  [0.01, 0.025, "1–2.5%"],
  [0.025, 0.05, "2.5–5%"],
  [0.05, 0.10,  "5–10%"],
  [0.10, 0.20,  "10–20%"],
  [0.20, 1.01,  "20%+"],
];
function binFor(fp) {
  for (const [lo, hi, label] of BIN_SPECS) if (fp >= lo && fp < hi) return label;
  return null;
}

function computeAnalysis(joined) {
  // Deduplicated dates
  const dates = [...new Set(joined.map(r => r.date))].sort();

  // ----- Network daily -----
  const byDay = {};
  for (const r of joined) {
    const v = byDay[r.date] || (byDay[r.date] = {ad_revenue:0, page_views:0, impressions:0, fraud:0});
    v.ad_revenue += r.ad_revenue || 0;
    v.page_views += r.page_views || 0;
    v.impressions += r.impressions || 0;
    v.fraud += r.fraud || 0;
  }
  const network_daily = [];
  for (const d of Object.keys(byDay).sort()) {
    const v = byDay[d];
    if (v.page_views < 1000 || v.impressions < 100) continue;
    network_daily.push({
      date: d,
      rpm: (v.ad_revenue / v.page_views) * 1000,
      fraud_pct: v.fraud / v.impressions,
      page_views: v.page_views,
      impressions: v.impressions,
    });
  }
  const [nd_r, nd_n] = pearson(network_daily.map(x=>x.fraud_pct), network_daily.map(x=>x.rpm));

  // ----- Per-property aggregate -----
  const byProp = {};
  for (const r of joined) {
    const p = byProp[r.property_tab] || (byProp[r.property_tab] = {
      tab:r.property_tab, label:r.property_label, oem_group:r.oem_group,
      ad_revenue:0, page_views:0, ad_clicks:0, impressions:0, fraud:0, days:0,
    });
    p.ad_revenue += r.ad_revenue || 0;
    p.page_views += r.page_views || 0;
    p.ad_clicks += r.ad_clicks || 0;
    p.impressions += r.impressions || 0;
    p.fraud += r.fraud || 0;
    p.days += 1;
  }
  const per_property = [];
  for (const tab of Object.keys(byProp)) {
    const v = byProp[tab];
    const minDays = Math.min(5, dates.length);
    if (v.days < minDays || v.page_views < 1000 || v.impressions < 100) continue;
    per_property.push({
      tab: v.tab, label: v.label, oem_group: v.oem_group, days: v.days,
      rpm: (v.ad_revenue / v.page_views) * 1000,
      ctr: v.page_views ? v.ad_clicks / v.page_views : null,
      fraud_pct: v.fraud / v.impressions,
      ad_revenue: v.ad_revenue, page_views: v.page_views,
      ad_clicks: v.ad_clicks, impressions: v.impressions,
    });
  }
  // ---- Advertiser-demand metrics (purely descriptive — no cut/scale labels) ----
  // CPC = revenue per click — what advertisers are willing to pay per click on this
  //   property's audience. Decoupled from CTR, so it isolates demand from engagement.
  // Demand-adjusted CPC = CPC × (1 - fraud%) — discounts inventory advertisers
  //   would re-bid down on once fraud is known.
  for (const p of per_property) {
    p.cpc = (p.ad_clicks > 0) ? p.ad_revenue / p.ad_clicks : null;
    p.cpc_clean = p.cpc != null ? p.cpc * (1 - p.fraud_pct) : null;
  }
  // Demand tier by fraud-adjusted CPC tertile (descriptive only)
  if (per_property.length >= 3) {
    const sorted = per_property.map(p => p.cpc_clean).filter(v => v != null && !Number.isNaN(v)).sort((a,b)=>a-b);
    const t1 = sorted[Math.floor(sorted.length * 1/3)];
    const t2 = sorted[Math.floor(sorted.length * 2/3)];
    for (const p of per_property) {
      if (p.cpc_clean == null) {
        p.demand_tier = "—";
        p.demand_why = "No clicks recorded in this window.";
      } else if (p.cpc_clean >= t2) {
        p.demand_tier = "High";
        p.demand_why = `CPC $${p.cpc.toFixed(3)} × (1 − fraud ${(p.fraud_pct*100).toFixed(1)}%) = $${p.cpc_clean.toFixed(3)} clean CPC — top tertile.`;
      } else if (p.cpc_clean >= t1) {
        p.demand_tier = "Mid";
        p.demand_why = `Clean CPC $${p.cpc_clean.toFixed(3)} — middle tertile.`;
      } else {
        p.demand_tier = "Low";
        p.demand_why = `Clean CPC $${p.cpc_clean.toFixed(3)} — bottom tertile of advertiser bids.`;
      }
    }
  } else {
    for (const p of per_property) { p.demand_tier = "—"; p.demand_why = ""; }
  }
  // A continuous score for sorting: revenue capture potential at current CPC.
  // Higher = stronger advertiser demand × volume × inventory quality.
  for (const p of per_property) {
    p.focus_score = (p.cpc_clean != null) ? p.cpc_clean * Math.log10(Math.max(10, p.page_views || 0)) : null;
  }
  const [pa_r, pa_n] = pearson(per_property.map(x=>x.fraud_pct), per_property.map(x=>x.rpm));

  // ----- Within-property pooled (centered) -----
  const propRecs = {};
  for (const r of joined) (propRecs[r.property_tab] || (propRecs[r.property_tab] = [])).push(r);
  const within_xs = [], within_ys = [];
  const per_property_corr = [];
  for (const tab of Object.keys(propRecs)) {
    const recs = propRecs[tab];
    const minN = Math.max(3, Math.min(7, dates.length));
    if (recs.length < minN) continue;
    const fp = recs.map(r=>r.fraud_pct), rpm = recs.map(r=>r.rpm);
    const mfp = fp.reduce((a,b)=>a+b,0)/fp.length;
    const mrpm = rpm.reduce((a,b)=>a+b,0)/rpm.length;
    for (let i=0;i<fp.length;i++){
      within_xs.push(fp[i]-mfp);
      within_ys.push(rpm[i]-mrpm);
    }
    const [r_p, n_p] = pearson(fp, rpm);
    per_property_corr.push({
      tab, label: recs[0].property_label, oem_group: recs[0].oem_group,
      n: n_p, r: r_p, mean_rpm: mrpm, mean_fraud: mfp,
    });
  }
  const [wi_r, wi_n] = pearson(within_xs, within_ys);
  per_property_corr.sort((a,b)=>{
    const ax = Number.isNaN(a.r) ? -1 : Math.abs(a.r);
    const bx = Number.isNaN(b.r) ? -1 : Math.abs(b.r);
    return bx - ax;
  });

  // ----- Threshold bins -----
  const cross_bins = BIN_SPECS.map(([_,__,label]) => ({
    bin: label, n: 0, rpms: [], page_views: 0, ad_revenue: 0, impressions: 0, fraud: 0,
  }));
  const bIdx = Object.fromEntries(cross_bins.map((b,i)=>[b.bin,i]));
  for (const r of joined) {
    const b = binFor(r.fraud_pct); if (!b) continue;
    const x = cross_bins[bIdx[b]];
    x.rpms.push(r.rpm); x.n += 1;
    x.page_views += r.page_views || 0;
    x.ad_revenue += r.ad_revenue || 0;
    x.impressions += r.impressions || 0;
    x.fraud += r.fraud || 0;
  }
  for (const b of cross_bins) {
    if (!b.n) { b.weighted_rpm = null; b.mean_rpm = null; b.median_rpm = null; b.actual_fraud_pct = null; continue; }
    const sorted = b.rpms.slice().sort((a,b)=>a-b);
    b.mean_rpm = sorted.reduce((a,b)=>a+b,0)/b.n;
    b.median_rpm = b.n % 2 ? sorted[(b.n-1)/2] : (sorted[b.n/2-1]+sorted[b.n/2])/2;
    b.weighted_rpm = b.page_views ? (b.ad_revenue/b.page_views)*1000 : null;
    b.actual_fraud_pct = b.impressions ? b.fraud/b.impressions : null;
    delete b.rpms;
  }
  const baseline_w = (cross_bins.find(b => b.n && b.weighted_rpm) || {}).weighted_rpm || null;
  for (const b of cross_bins) {
    if (b.n && baseline_w && b.weighted_rpm != null) b.pct_of_baseline = b.weighted_rpm / baseline_w;
  }
  // Within-property normalized bins
  const propMeanRpm = {};
  for (const tab of Object.keys(propRecs)) {
    const recs = propRecs[tab]; if (recs.length < 5) continue;
    propMeanRpm[tab] = recs.reduce((a,r)=>a+r.rpm,0) / recs.length;
  }
  const within_bins = BIN_SPECS.map(([_,__,label]) => ({bin: label, n: 0, vals: []}));
  const wbIdx = Object.fromEntries(within_bins.map((b,i)=>[b.bin,i]));
  for (const r of joined) {
    const m = propMeanRpm[r.property_tab]; if (!m || m <= 0) continue;
    const b = binFor(r.fraud_pct); if (!b) continue;
    within_bins[wbIdx[b]].vals.push(r.rpm / m);
    within_bins[wbIdx[b]].n += 1;
  }
  for (const b of within_bins) {
    if (!b.n) { b.mean_rel_rpm = null; b.median_rel_rpm = null; continue; }
    const v = b.vals.slice().sort((a,b)=>a-b);
    b.mean_rel_rpm = v.reduce((a,b)=>a+b,0)/b.n;
    b.median_rel_rpm = v[Math.floor(b.n/2)];
    delete b.vals;
  }
  let threshold_bin = null;
  for (const b of cross_bins) {
    if (b.n && b.pct_of_baseline != null && b.pct_of_baseline <= 0.75) { threshold_bin = b.bin; break; }
  }

  return {
    summary: {
      joined_rows: joined.length,
      properties_used: per_property.length,
      date_range: [dates[0] || null, dates[dates.length-1] || null],
      network_daily: {r: nd_r, n: nd_n, p: tPvalue(nd_r, nd_n)},
      per_property_aggregate: {r: pa_r, n: pa_n, p: tPvalue(pa_r, pa_n)},
      within_property_pooled: {r: wi_r, n: wi_n, p: tPvalue(wi_r, wi_n)},
    },
    network_daily, per_property, per_property_corr,
    threshold: {
      bins: cross_bins, within_bins,
      baseline_weighted_rpm: baseline_w,
      threshold_bin,
    },
  };
}

// ---- Generic sortable-table helper ----
function makeSorter(tableId, defaultKey, defaultDir, renderFn) {
  const state = { k: defaultKey, dir: defaultDir };
  function paintHeader() {
    document.querySelectorAll(`#${tableId} th[data-k]`).forEach(th=>{
      const arr = th.querySelector(".arr") || (() => {
        const s = document.createElement("span"); s.className = "arr"; s.textContent = "↕";
        th.appendChild(s); return s;
      })();
      th.classList.remove("sort-asc","sort-desc");
      if (th.dataset.k === state.k) {
        th.classList.add(state.dir > 0 ? "sort-asc" : "sort-desc");
        arr.textContent = state.dir > 0 ? "▲" : "▼";
      } else arr.textContent = "↕";
    });
  }
  document.querySelectorAll(`#${tableId} th[data-k]`).forEach(th=>{
    th.addEventListener("click", ()=>{
      const k = th.dataset.k;
      if (state.k === k) state.dir = -state.dir;
      else { state.k = k; state.dir = -1; }
      paintHeader();
      renderFn(state);
    });
  });
  paintHeader();
  return () => renderFn(state);  // call to re-render with current sort
}
function sortRows(rows, key, dir) {
  return rows.slice().sort((a,b)=>{
    const x = a[key], y = b[key];
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    if (typeof x === "number" && typeof y === "number") return (x - y) * dir;
    return String(x).localeCompare(String(y)) * dir;
  });
}

// ---- Charts (created once, updated per render) ----
Chart.defaults.color = "#8b93a7";
Chart.defaults.borderColor = "rgba(255,255,255,0.05)";
const charts = {};

function buildCharts() {
  const baselinePlugin = {
    id: "baseline",
    afterDraw: (chart) => {
      const baseline = chart.options.plugins?.baselineValue;
      if (baseline == null) return;
      const {ctx, chartArea, scales} = chart;
      const y = scales.y.getPixelForValue(baseline);
      ctx.save();
      ctx.strokeStyle = "rgba(255,255,255,0.4)";
      ctx.setLineDash([4,4]);
      ctx.beginPath(); ctx.moveTo(chartArea.left, y); ctx.lineTo(chartArea.right, y); ctx.stroke();
      ctx.fillStyle = "rgba(255,255,255,0.6)"; ctx.font = "11px sans-serif";
      ctx.fillText(`baseline $${baseline.toFixed(2)}`, chartArea.left+4, y-4);
      ctx.restore();
    }
  };
  const unitLinePlugin = {
    id: "unitLine",
    afterDraw: (chart) => {
      if (!chart.options.plugins?.unitLine) return;
      const {ctx, chartArea, scales} = chart;
      const y = scales.y.getPixelForValue(1);
      ctx.save();
      ctx.strokeStyle = "rgba(255,255,255,0.4)";
      ctx.setLineDash([4,4]);
      ctx.beginPath(); ctx.moveTo(chartArea.left, y); ctx.lineTo(chartArea.right, y); ctx.stroke();
      ctx.fillStyle = "rgba(255,255,255,0.6)"; ctx.font = "11px sans-serif";
      ctx.fillText("property's typical RPM", chartArea.left+4, y-4);
      ctx.restore();
    }
  };

  charts.bin = new Chart(document.getElementById("binChart"), {
    type:"bar",
    data:{labels:[], datasets:[{data:[], backgroundColor:[], borderColor:"rgba(255,255,255,0.15)", borderWidth:1}]},
    options:{
      plugins:{legend:{display:false}, tooltip:{callbacks:{label:(c)=>c.chart.options.plugins.binTooltips?.[c.dataIndex] || ""}}},
      scales:{
        y:{title:{display:true,text:"Weighted RPM ($)"}, ticks:{callback:v=>"$"+v.toFixed(2)}, grid:{color:"rgba(255,255,255,0.05)"}},
        x:{title:{display:true,text:"Fraud % bucket"}}
      },
      maintainAspectRatio:false, responsive:true,
    },
    plugins: [baselinePlugin],
  });

  charts.withinBin = new Chart(document.getElementById("withinBinChart"), {
    type:"bar",
    data:{labels:[], datasets:[{data:[], backgroundColor:[], borderColor:"rgba(255,255,255,0.15)", borderWidth:1}]},
    options:{
      plugins:{legend:{display:false}, unitLine:true,
        tooltip:{callbacks:{label:(c)=>c.chart.options.plugins.withinTooltips?.[c.dataIndex] || ""}}},
      scales:{
        y:{title:{display:true,text:"RPM relative to property's avg"}, ticks:{callback:v=>v.toFixed(2)+"×"},
           grid:{color:"rgba(255,255,255,0.05)"}, suggestedMin:0, suggestedMax:1.5},
        x:{title:{display:true,text:"Fraud % bucket"}}
      },
      maintainAspectRatio:false, responsive:true,
    },
    plugins: [unitLinePlugin],
  });

  charts.ndScatter = new Chart(document.getElementById("ndScatter"), {
    type:"scatter",
    data:{datasets:[{label:"Network day", data:[], backgroundColor:"rgba(106,169,255,0.55)", borderColor:"rgba(106,169,255,0.9)", pointRadius:4}]},
    options:{
      plugins:{legend:{display:false}},
      scales:{
        x:{title:{display:true,text:"Network Fraud %"}, ticks:{callback:v=>(v*100).toFixed(0)+"%"}},
        y:{title:{display:true,text:"Network RPM"}, ticks:{callback:v=>"$"+v.toFixed(2)}}
      },
      maintainAspectRatio:false, responsive:true,
    }
  });

  charts.ndTime = new Chart(document.getElementById("ndTime"), {
    type:"line",
    data:{labels:[], datasets:[
      {label:"Fraud %", data:[], borderColor:"#f87171", backgroundColor:"rgba(248,113,113,0.12)", yAxisID:"y", tension:0.25, fill:true},
      {label:"RPM", data:[], borderColor:"#6aa9ff", backgroundColor:"transparent", yAxisID:"y1", tension:0.25, fill:false},
    ]},
    options:{
      plugins:{legend:{labels:{boxWidth:12}}},
      scales:{
        y:{position:"left", ticks:{callback:v=>(v*100).toFixed(0)+"%"}, title:{display:true,text:"Fraud %"}},
        y1:{position:"right", grid:{drawOnChartArea:false}, ticks:{callback:v=>"$"+v.toFixed(2)}, title:{display:true,text:"RPM"}}
      },
      maintainAspectRatio:false, responsive:true,
    }
  });

  charts.demandScatter = new Chart(document.getElementById("demandScatter"), {
    type:"bubble",
    data:{datasets:[{label:"Property", data:[], backgroundColor:[], borderColor:[], borderWidth:1.5}]},
    options:{
      plugins:{legend:{display:false},
        tooltip:{callbacks:{label:c=>{
          const r = c.raw;
          return [
            `${r.label}`,
            `Clean CPC: ${r.x!=null ? '$'+r.x.toFixed(3) : '—'}  (raw CPC ${r.cpc!=null ? '$'+r.cpc.toFixed(3) : '—'})`,
            `Volume: ${Math.round(r.y).toLocaleString()} page views`,
            `Revenue: $${Math.round(r.rev).toLocaleString()}`,
            `Fraud: ${(r.fraud*100).toFixed(2)}%`,
            `CTR: ${r.ctr!=null ? (r.ctr*100).toFixed(2)+'%' : '—'}`,
            `Demand tier: ${r.tier}`,
          ];
        }}}},
      scales:{
        x:{title:{display:true,text:"Clean CPC (advertiser bid × inventory quality, $/click)"},
           ticks:{callback:v=>"$"+v.toFixed(2)}},
        y:{title:{display:true,text:"Page views (volume / scale capacity)"},
           ticks:{callback:v=>v >= 1000 ? (v/1000).toFixed(0)+"k" : v.toLocaleString()}}
      },
      maintainAspectRatio:false, responsive:true,
    }
  });

  charts.ppScatter = new Chart(document.getElementById("ppScatter"), {
    type:"bubble",
    data:{datasets:[{label:"Property", data:[], backgroundColor:[], borderColor:[], borderWidth:1.5}]},
    options:{
      plugins:{legend:{display:false},
        tooltip:{callbacks:{label:c=>{
          const r = c.raw;
          return [
            `${r.label}`,
            `RPM: $${r.y.toFixed(2)}`,
            `Fraud: ${(r.x*100).toFixed(2)}%`,
            `CTR: ${r.ctr!=null?(r.ctr*100).toFixed(2)+'%':'—'}`,
            `Volume: ${Math.round(r.pv).toLocaleString()} page views`,
            `Action: ${r.recommendation}`,
          ];
        }}}},
      scales:{
        x:{title:{display:true,text:"Fraud %"}, ticks:{callback:v=>(v*100).toFixed(0)+"%"}},
        y:{title:{display:true,text:"RPM"}, ticks:{callback:v=>"$"+v.toFixed(2)}}
      },
      maintainAspectRatio:false, responsive:true,
    }
  });
}

// ---- Render functions ----
function renderHeader(c) {
  const sub = document.getElementById("sub");
  const r = c.summary.date_range;
  sub.textContent = `Date range: ${r[0] || "—"} → ${r[1] || "—"}  ·  ${c.summary.joined_rows.toLocaleString()} (property × day) joined records  ·  ${c.summary.properties_used} properties with sufficient data`;
}

function renderVerdict(c) {
  const pa = c.summary.per_property_aggregate;
  const wi = c.summary.within_property_pooled;
  const paC = classify(pa.r);
  let lead;
  if (Number.isNaN(pa.r)) {
    lead = `<span class="strong neu">Not enough data in this date range to compute a meaningful correlation.</span>`;
  } else if (Math.abs(pa.r) < 0.1) {
    lead = `<span class="strong neu">In this window, IVT fraud % and RPM are essentially uncorrelated.</span>`;
  } else if (pa.r < 0) {
    lead = `<span class="strong neg">Negative correlation: properties with higher fraud have lower RPM.</span>`;
  } else {
    lead = `<span class="strong pos">Positive correlation: properties with higher fraud have higher RPM (unusual — investigate).</span>`;
  }
  const wiText = Number.isNaN(wi.r) ? "n/a (need ≥4 days)" : `r = ${fmt.r(wi.r)}`;
  document.getElementById("verdict").innerHTML = `
    ${lead}
    <br><br>
    <span style="font-size:15px;">
      Across properties (n = ${pa.n}), <span class="${paC.pill}">${paC.label}, r = ${fmt.r(pa.r)}, p ${pa.p<0.001?"<":"="} ${fmt.p(pa.p)}</span>.
      Within properties (each centered to its own mean), pooled ${wiText}.
    </span>`;
}

function renderLevels(c) {
  const lv = c.summary;
  function levelCard(label, x, hint) {
    const cls = classify(x.r);
    const color = cls.pill==='neg'?'var(--bad)':cls.pill==='pos'?'var(--good)':'var(--text)';
    return `<div class="level">
      <div class="label">${label}</div>
      <div class="r" style="color:${color};">r = ${fmt.r(x.r)}</div>
      <div class="meta">n = ${x.n.toLocaleString()} · p ${x.p<0.001?"<":"="} ${fmt.p(x.p)}<br>${hint}</div>
    </div>`;
  }
  document.getElementById("levels").innerHTML = [
    levelCard("Network daily (pooled days)", lv.network_daily, "Each day is one observation."),
    levelCard("Per-property aggregate (one row per property)", lv.per_property_aggregate, "Aggregated within window."),
    levelCard("Within-property pooled (strictest)", lv.within_property_pooled, "Each property centered to its own mean."),
  ].join("");
}

function renderThresholdHeadline(c) {
  const t = c.threshold;
  const bins = t.bins;
  const filled = bins.filter(b => b.n);
  let head;
  if (!filled.length) {
    head = `<span style="color:var(--muted);">No data in this date range.</span>`;
  } else if (t.threshold_bin) {
    head = `<strong>RPM drops sharply once fraud crosses <span class="neg">${t.threshold_bin}</span>.</strong>
            Below that, RPM stays within ~25% of the baseline (the cleanest fraud bucket); above it, it falls off.`;
  } else {
    head = `RPM does not drop more than 25% below the baseline in any of the observed fraud buckets in this window.`;
  }
  if (filled.length >= 2) {
    const first = filled[0], last = filled[filled.length-1];
    const dropPct = first.weighted_rpm ? (1 - (last.weighted_rpm || 0)/first.weighted_rpm) * 100 : null;
    head += `<br><br><span style="color:var(--muted);font-size:14px;">
      In numbers: cleanest bucket (<em>${first.bin}</em>) earned weighted RPM <strong>$${first.weighted_rpm.toFixed(2)}</strong>;
      most fraud-heavy bucket (<em>${last.bin}</em>) earned <strong>${last.weighted_rpm ? '$'+last.weighted_rpm.toFixed(2) : '—'}</strong>${dropPct!=null?` — a ${dropPct.toFixed(0)}% drop`:''}.
    </span>`;
  }
  document.getElementById("thresholdHeadline").innerHTML = head;
}

function renderCharts(c) {
  // Bin chart
  const bins = c.threshold.bins;
  const baseline = c.threshold.baseline_weighted_rpm;
  charts.bin.data.labels = bins.map(b => b.bin);
  charts.bin.data.datasets[0].data = bins.map(b => b.weighted_rpm || 0);
  charts.bin.data.datasets[0].backgroundColor = bins.map(b => {
    if (!b.weighted_rpm || !baseline) return "rgba(139,147,167,0.5)";
    const ratio = b.weighted_rpm / baseline;
    if (ratio >= 0.9) return "rgba(74,222,128,0.7)";
    if (ratio >= 0.75) return "rgba(251,191,36,0.7)";
    return "rgba(248,113,113,0.7)";
  });
  charts.bin.options.plugins.baselineValue = baseline;
  charts.bin.options.plugins.binTooltips = bins.map(b => {
    if (!b.n) return "no data";
    const pct = b.pct_of_baseline ? `${(b.pct_of_baseline*100).toFixed(0)}% of baseline` : "";
    return `Weighted RPM: $${b.weighted_rpm.toFixed(2)} · n: ${b.n} · ${pct}`;
  });
  charts.bin.update();

  // Within-bin chart
  const wb = c.threshold.within_bins;
  charts.withinBin.data.labels = wb.map(b => b.bin);
  charts.withinBin.data.datasets[0].data = wb.map(b => b.mean_rel_rpm || 0);
  charts.withinBin.data.datasets[0].backgroundColor = wb.map(b => {
    if (!b.mean_rel_rpm) return "rgba(139,147,167,0.5)";
    if (b.mean_rel_rpm >= 0.95) return "rgba(74,222,128,0.7)";
    if (b.mean_rel_rpm >= 0.8) return "rgba(251,191,36,0.7)";
    return "rgba(248,113,113,0.7)";
  });
  charts.withinBin.options.plugins.withinTooltips = wb.map(b => {
    if (!b.n) return "no data";
    return `Mean: ${b.mean_rel_rpm.toFixed(2)}× (${(b.mean_rel_rpm*100).toFixed(0)}% of property avg) · n: ${b.n}`;
  });
  charts.withinBin.update();

  // Network scatter
  charts.ndScatter.data.datasets[0].data = c.network_daily.map(r => ({x:r.fraud_pct, y:r.rpm}));
  charts.ndScatter.update();

  // Network time
  const sortedDays = c.network_daily.slice().sort((a,b)=>a.date.localeCompare(b.date));
  charts.ndTime.data.labels = sortedDays.map(r => r.date);
  charts.ndTime.data.datasets[0].data = sortedDays.map(r => r.fraud_pct);
  charts.ndTime.data.datasets[1].data = sortedDays.map(r => r.rpm);
  charts.ndTime.update();

  // ----- Advertiser-demand bubble: x=clean CPC, y=page views, size=revenue, color=fraud quality -----
  {
    const props = c.per_property;
    const revenues = props.map(p => p.ad_revenue).filter(v => v > 0);
    const maxRev = revenues.length ? Math.max(...revenues) : 1;
    const minR = 5, maxR = 28;
    function fraudColor(fp) {
      if (fp == null || Number.isNaN(fp)) return ["rgba(139,147,167,0.55)", "rgba(139,147,167,0.95)"];
      if (fp >= 0.20) return ["rgba(248,113,113,0.55)", "rgba(248,113,113,0.95)"];
      if (fp >= 0.05) return ["rgba(251,191,36,0.55)", "rgba(251,191,36,0.95)"];
      return ["rgba(74,222,128,0.55)",  "rgba(74,222,128,0.95)"];
    }
    const data = props
      .filter(p => p.cpc_clean != null)
      .map(p => {
        const r = p.ad_revenue > 0 ? minR + (maxR - minR) * Math.sqrt(p.ad_revenue / maxRev) : minR;
        return {
          x: p.cpc_clean, y: p.page_views, r,
          label: p.label, cpc: p.cpc, rev: p.ad_revenue,
          fraud: p.fraud_pct, ctr: p.ctr, tier: p.demand_tier,
        };
      });
    charts.demandScatter.data.datasets[0].data = data;
    charts.demandScatter.data.datasets[0].backgroundColor = data.map(d => fraudColor(d.fraud)[0]);
    charts.demandScatter.data.datasets[0].borderColor = data.map(d => fraudColor(d.fraud)[1]);
    charts.demandScatter.update();
  }

  // ----- Per-property bubble: x=fraud, y=rpm, r=volume(pv), color=CTR tertile -----
  const props = c.per_property;
  // CTR tertile thresholds
  const ctrs = props.map(p => p.ctr).filter(v => v != null && !Number.isNaN(v)).sort((a,b)=>a-b);
  const ctrLow = ctrs.length ? ctrs[Math.floor(ctrs.length/3)] : 0;
  const ctrHigh = ctrs.length ? ctrs[Math.floor(ctrs.length*2/3)] : 0;
  // Volume → bubble radius (sqrt scaling so area is proportional to volume)
  const pvs = props.map(p => p.page_views).filter(v=>v>0);
  const maxPv = pvs.length ? Math.max(...pvs) : 1;
  const minR = 5, maxR = 26;
  function ctrColor(ctr) {
    if (ctr == null || Number.isNaN(ctr)) return ["rgba(139,147,167,0.55)", "rgba(139,147,167,0.95)"];
    if (ctr <= ctrLow)  return ["rgba(248,113,113,0.55)", "rgba(248,113,113,0.95)"];
    if (ctr >= ctrHigh) return ["rgba(74,222,128,0.55)",  "rgba(74,222,128,0.95)"];
    return ["rgba(251,191,36,0.55)", "rgba(251,191,36,0.95)"];
  }
  const data = props.map(p => {
    const r = p.page_views > 0 ? minR + (maxR - minR) * Math.sqrt(p.page_views / maxPv) : minR;
    return {
      x: p.fraud_pct, y: p.rpm, r: r,
      label: p.label, ctr: p.ctr, pv: p.page_views,
      recommendation: p.recommendation || "—",
    };
  });
  charts.ppScatter.data.datasets[0].data = data;
  charts.ppScatter.data.datasets[0].backgroundColor = props.map(p => ctrColor(p.ctr)[0]);
  charts.ppScatter.data.datasets[0].borderColor = props.map(p => ctrColor(p.ctr)[1]);
  charts.ppScatter.update();
}

// ---- Sortable tables (wired once; closures read latest `computed`) ----
let computed = null;
const reBin = makeSorter("binTable", "bin_order", 1, (state) => {
  if (!computed) return;
  const bins = computed.threshold.bins.map((b,i)=>({...b, bin_order:i}));
  const sorted = sortRows(bins, state.k, state.dir);
  document.querySelector("#binTable tbody").innerHTML = sorted.map(b => {
    if (!b.n) return `<tr><td>${b.bin}</td><td class="num" colspan="7" style="color:var(--muted);font-style:italic;">no data</td></tr>`;
    const pct = b.pct_of_baseline;
    const pill = pct == null ? "" :
      pct >= 0.9 ? `<span class="pill pos">${(pct*100).toFixed(0)}%</span>` :
      pct >= 0.75 ? `<span class="pill" style="background:rgba(251,191,36,0.18);color:var(--warn)">${(pct*100).toFixed(0)}%</span>` :
      `<span class="pill neg">${(pct*100).toFixed(0)}%</span>`;
    return `<tr>
      <td><strong>${b.bin}</strong></td>
      <td class="num">${b.n.toLocaleString()}</td>
      <td class="num">${b.weighted_rpm ? "$"+b.weighted_rpm.toFixed(2) : "—"}</td>
      <td class="num">${b.mean_rpm ? "$"+b.mean_rpm.toFixed(2) : "—"}</td>
      <td class="num">${b.median_rpm ? "$"+b.median_rpm.toFixed(2) : "—"}</td>
      <td class="num">${pill}</td>
      <td class="num">${Math.round(b.page_views).toLocaleString()}</td>
      <td class="num">$${Math.round(b.ad_revenue).toLocaleString()}</td>
    </tr>`;
  }).join("");
});

function tierPill(tier, why) {
  const t = why ? ` title="${why.replace(/"/g,"&quot;")}"` : "";
  if (tier === "High") return `<span class="pill pos"${t}>High</span>`;
  if (tier === "Mid")  return `<span class="pill" style="background:rgba(251,191,36,0.18);color:var(--warn)"${t}>Mid</span>`;
  if (tier === "Low")  return `<span class="pill" style="background:rgba(139,147,167,0.22);color:var(--muted)"${t}>Low</span>`;
  return `<span class="pill muted">—</span>`;
}
const reProp = makeSorter("propTable", "focus_score", -1, (state) => {
  if (!computed) return;
  const rows = sortRows(computed.per_property, state.k, state.dir);
  document.querySelector("#propTable tbody").innerHTML = rows.length ? rows.map(r => `
    <tr>
      <td><span class="pill muted">${r.oem_group}</span></td>
      <td>${r.label}</td>
      <td class="num">${r.days}</td>
      <td class="num">${r.cpc == null || Number.isNaN(r.cpc) ? "—" : "$"+r.cpc.toFixed(3)}</td>
      <td class="num">${r.cpc_clean == null || Number.isNaN(r.cpc_clean) ? "—" : "$"+r.cpc_clean.toFixed(3)}</td>
      <td class="num">${r.rpm == null || Number.isNaN(r.rpm) ? "—" : "$"+r.rpm.toFixed(2)}</td>
      <td class="num">${fmt.pct(r.ctr)}</td>
      <td class="num">${fmt.pct(r.fraud_pct)}</td>
      <td class="num">${fmt.int(r.page_views)}</td>
      <td class="num">${fmt.money(r.ad_revenue)}</td>
      <td class="num">${r.focus_score == null ? "—" : r.focus_score.toFixed(2)}</td>
      <td>${tierPill(r.demand_tier, r.demand_why)}</td>
    </tr>`).join("") : `<tr><td colspan="12" style="color:var(--muted);font-style:italic;text-align:center;padding:18px;">No properties met the volume threshold in this window.</td></tr>`;
});

const rePpCorr = makeSorter("ppCorrTable", "r", 1, (state) => {
  if (!computed) return;
  const rows = sortRows(computed.per_property_corr, state.k, state.dir);
  document.querySelector("#ppCorrTable tbody").innerHTML = rows.length ? rows.map(r => {
    const cl = classify(r.r);
    return `<tr>
      <td><span class="pill muted">${r.oem_group}</span></td>
      <td>${r.label}</td>
      <td class="num">${r.n}</td>
      <td class="num"><span class="pill ${cl.pill}">${fmt.r(r.r)}</span></td>
      <td class="num">$${r.mean_rpm.toFixed(2)}</td>
      <td class="num">${fmt.pct(r.mean_fraud)}</td>
    </tr>`;
  }).join("") : `<tr><td colspan="6" style="color:var(--muted);font-style:italic;text-align:center;padding:18px;">No properties have enough days in this window for per-property correlation.</td></tr>`;
});

// ---- Filter state + presets ----
const state = { from: null, to: null };

function shiftDate(iso, days) {
  const d = new Date(iso + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0,10);
}

function applyPreset(name) {
  let from, to;
  if (name === "yesterday") { from = to = DATA_MAX; }
  else if (name === "7")    { from = shiftDate(DATA_MAX, -6);  to = DATA_MAX; }
  else if (name === "14")   { from = shiftDate(DATA_MAX, -13); to = DATA_MAX; }
  else if (name === "30")   { from = shiftDate(DATA_MAX, -29); to = DATA_MAX; }
  else if (name === "90")   { from = shiftDate(DATA_MAX, -89); to = DATA_MAX; }
  else                       { from = DATA_MIN; to = DATA_MAX; name = "all"; }
  if (from < DATA_MIN) from = DATA_MIN;
  state.from = from; state.to = to;
  document.getElementById("from").value = from;
  document.getElementById("to").value = to;
  document.querySelectorAll("#presets button").forEach(b => b.classList.toggle("active", b.dataset.d === name));
  rerender();
}

function rerender() {
  const from = state.from, to = state.to;
  const filtered = ALL_PAIRS.filter(r => (!from || r.date >= from) && (!to || r.date <= to));
  computed = computeAnalysis(filtered);
  // Filter meta in header
  document.getElementById("filterMeta").textContent =
    `${state.from} → ${state.to}  ·  ${filtered.length.toLocaleString()} property-day records`;
  renderHeader(computed);
  renderVerdict(computed);
  renderLevels(computed);
  renderThresholdHeadline(computed);
  renderCharts(computed);
  reBin(); reProp(); rePpCorr();
}

// ---- Init ----
buildCharts();
document.querySelectorAll("#presets button").forEach(b => {
  b.addEventListener("click", () => applyPreset(b.dataset.d));
});
document.getElementById("from").addEventListener("change", e => {
  state.from = e.target.value;
  document.querySelectorAll("#presets button").forEach(b => b.classList.remove("active"));
  rerender();
});
document.getElementById("to").addEventListener("change", e => {
  state.to = e.target.value;
  document.querySelectorAll("#presets button").forEach(b => b.classList.remove("active"));
  rerender();
});
// Set min/max on date inputs
const fromEl = document.getElementById("from"), toEl = document.getElementById("to");
fromEl.min = DATA_MIN; fromEl.max = DATA_MAX;
toEl.min = DATA_MIN; toEl.max = DATA_MAX;

applyPreset("all");
</script>
</body></html>"""


def main(argv):
    out_dir = Path(argv[1]) if len(argv) > 1 else HERE
    src = out_dir / "source.xlsx"
    if not src.exists():
        src = HERE / "source.xlsx"
    out_html = out_dir / "ivt_rpm_correlation.html"
    report = build(src, out_html)
    render(report, out_html)
    s = report["summary"]
    print(f"[analysis] wrote {out_html}", file=sys.stderr)
    print(f"  joined rows: {s['joined_rows']}", file=sys.stderr)
    print(f"  properties used: {s['properties_used']}", file=sys.stderr)
    print(f"  network-daily r = {s['network_daily']['r']:.3f} (n={s['network_daily']['n']}, p={s['network_daily']['p']:.4f})", file=sys.stderr)
    print(f"  per-property-agg r = {s['per_property_aggregate']['r']:.3f} (n={s['per_property_aggregate']['n']}, p={s['per_property_aggregate']['p']:.4f})", file=sys.stderr)
    print(f"  within-property-pooled r = {s['within_property_pooled']['r']:.3f} (n={s['within_property_pooled']['n']}, p={s['within_property_pooled']['p']:.4f})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

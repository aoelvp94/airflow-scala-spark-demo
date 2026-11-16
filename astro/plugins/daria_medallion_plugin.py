from __future__ import annotations

import json
from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

PIPELINE_ROOT = Path("/usr/local/airflow/include/pipeline")

app = FastAPI(title="spark-daria Medallion", version="1.0.0")


def _runs() -> list[Path]:
    if not PIPELINE_ROOT.exists():
        return []
    runs = [p for p in PIPELINE_ROOT.iterdir() if p.is_dir() and (p / "run.json").exists()]
    return sorted(runs, key=lambda p: p.stat().st_mtime, reverse=True)


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _esc(value: object) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _schema_table(fields: list) -> str:
    if not fields:
        return "<p class='muted'>No schema captured.</p>"
    rows = "".join(
        f"<tr><td>{_esc(f.get('name'))}</td><td>{_esc(f.get('type'))}</td><td>{_esc(f.get('nullable'))}</td></tr>"
        for f in fields
        if isinstance(f, dict)
    )
    return f"<table><thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead><tbody>{rows}</tbody></table>"


def _kpi_cards(kpis: dict) -> str:
    if not kpis:
        return "<p class='muted'>No gold KPIs yet. Run <code>scala_spark_medallion</code>.</p>"
    labels = {
        "orders": "Orders",
        "customers": "Customers",
        "revenue": "Revenue",
        "vip_orders": "VIP orders",
        "vip_customers": "VIP customers",
        "vip_revenue": "VIP revenue",
        "vip_revenue_share": "VIP revenue share",
    }
    cards = []
    for key, label in labels.items():
        if key not in kpis:
            continue
        val = kpis[key]
        if isinstance(val, float):
            text = f"{val:.2%}" if "share" in key else f"{val:,.2f}"
        else:
            text = f"{val:,}"
        cards.append(f"<div class='kpi'><div class='label'>{_esc(label)}</div><div class='value'>{_esc(text)}</div></div>")
    return "<div class='kpis'>" + "".join(cards) + "</div>"


def _quality(quality: dict) -> str:
    if not quality:
        return "<p class='muted'>No quality report for this run.</p>"
    checks = quality.get("validations") or []
    rows = "".join(
        f"<tr><td>{_esc(c.get('name'))}</td><td class='ok'>{_esc(c.get('status'))}</td></tr>"
        for c in checks
        if isinstance(c, dict)
    )
    metrics = "".join(
        f"<li><strong>{_esc(k)}</strong>: {_esc(v)}</li>"
        for k, v in quality.items()
        if k != "validations"
    )
    table = f"<table><thead><tr><th>Check</th><th>Status</th></tr></thead><tbody>{rows}</tbody></table>" if rows else ""
    return table + (f"<ul class='metrics'>{metrics}</ul>" if metrics else "")


def _lineage_svg(summary: dict) -> str:
    datasets = summary.get("datasets") or []
    names = []
    for d in datasets:
        if not isinstance(d, dict):
            continue
        name = str(d.get("name") or "")
        short = name.rstrip("/").split("/")[-1] or name
        if short and short not in names:
            names.append(short)
    if not names:
        return "<p class='muted'>No lineage events yet.</p>"
    width = max(640, 160 * len(names))
    height = 140
    gap = width / (len(names) + 1)
    nodes = []
    coords = []
    for i, name in enumerate(names):
        x = gap * (i + 1)
        y = 50
        coords.append((x, y, name))
        nodes.append(
            f"<rect x='{x-70}' y='{y-24}' width='140' height='48' rx='8' fill='#1f6feb' />"
            f"<text x='{x}' y='{y+5}' text-anchor='middle' fill='white' font-size='12' font-family='sans-serif'>{_esc(name)}</text>"
        )
    edges = []
    for i in range(len(coords) - 1):
        x1, y1, _ = coords[i]
        x2, y2, _ = coords[i + 1]
        edges.append(f"<line x1='{x1+70}' y1='{y1}' x2='{x2-70}' y2='{y2}' stroke='#8b949e' stroke-width='2' marker-end='url(#arrow)' />")
    cols = summary.get("columnLineage") or []
    col_rows = "".join(
        f"<tr><td>{_esc(c.get('output'))}</td><td>{_esc(', '.join(map(str, c.get('inputs') or [])) if isinstance(c.get('inputs'), list) else c.get('inputs'))}</td></tr>"
        for c in cols
        if isinstance(c, dict)
    ) or "<tr><td colspan='2' class='muted'>No column-level edges captured.</td></tr>"
    svg = (
        f"<svg viewBox='0 0 {width} {height}' width='100%' height='{height}'>"
        "<defs><marker id='arrow' markerWidth='8' markerHeight='8' refX='6' refY='4' orient='auto'>"
        "<path d='M0,0 L8,4 L0,8 z' fill='#8b949e' /></marker></defs>"
        + "".join(edges + nodes)
        + "</svg>"
    )
    table = f"<table><thead><tr><th>Output column</th><th>Inputs</th></tr></thead><tbody>{col_rows}</tbody></table>"
    return svg + table


def render(run_dir: Path | None) -> str:
    manifest = _load(run_dir / "run.json") if run_dir else {}
    quality = manifest.get("quality") or _load(run_dir / "quality.json") if run_dir else {}
    summary = _load(run_dir / "lineage" / "summary.json") if run_dir else {}
    layers = manifest.get("layers") or {}
    bronze = layers.get("bronze") or {}
    silver = layers.get("silver") or {}
    gold = layers.get("gold") or {}
    run_name = run_dir.name if run_dir else "no run yet"
    options = "".join(
        f"<option value='{p.name}' {'selected' if run_dir and p == run_dir else ''}>{p.name}</option>"
        for p in _runs()
    )
    gold_bits = []
    for key, label in (
        ("revenue_by_country_month", "Revenue by country / month"),
        ("customer_segments", "Customer segments"),
        ("vip_metrics", "VIP metrics"),
    ):
        info = gold.get(key) or {}
        gold_bits.append(
            f"<div class='layer gold'><h3>{label}</h3><p>{_esc(info.get('rows', '—'))} rows</p>"
            f"<p class='path'>{_esc(info.get('path', ''))}</p></div>"
        )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>spark-daria medallion</title>
  <style>
    :root {{ color-scheme: light dark; }}
    body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 0; background: #0d1117; color: #e6edf3; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 24px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 1.6rem; }}
    h2 {{ margin-top: 28px; font-size: 1.15rem; color: #79c0ff; }}
    .muted, .path {{ color: #8b949e; font-size: 0.9rem; word-break: break-all; }}
    .layers {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
    .layer {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 16px; }}
    .layer.bronze {{ border-top: 4px solid #d29922; }}
    .layer.silver {{ border-top: 4px solid #8b949e; }}
    .layer.gold {{ border-top: 4px solid #e3b341; }}
    table {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.92rem; }}
    th, td {{ border-bottom: 1px solid #30363d; padding: 6px 8px; text-align: left; vertical-align: top; }}
    .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }}
    .kpi {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 12px; }}
    .kpi .label {{ color: #8b949e; font-size: 0.8rem; }}
    .kpi .value {{ font-size: 1.25rem; font-weight: 650; margin-top: 4px; }}
    .ok {{ color: #3fb950; }}
    select {{ background: #161b22; color: #e6edf3; border: 1px solid #30363d; padding: 6px 8px; border-radius: 6px; }}
    header {{ display: flex; justify-content: space-between; gap: 16px; flex-wrap: wrap; align-items: end; }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>spark-daria medallion</h1>
        <p class="muted">Bronze, silver and gold for the Scala Spark demo, with validation and lineage from Spark 4 QueryExecutionListener events.</p>
      </div>
      <form method="get">
        <label class="muted">Run<br/>
          <select name="run" onchange="this.form.submit()">{options or "<option>No runs yet</option>"}</select>
        </label>
      </form>
    </header>
    <h2>Layers</h2>
    <div class="layers">
      <div class="layer bronze">
        <h3>Bronze</h3>
        <p>{_esc(bronze.get("rows", "—"))} rows · {_esc(bronze.get("unique_ids", "—"))} unique ids</p>
        <p class="path">{_esc(bronze.get("path", run_name))}</p>
        {_schema_table(bronze.get("schema") or [])}
      </div>
      <div class="layer silver">
        <h3>Silver</h3>
        <p>{_esc(silver.get("rows", "—"))} rows · checksum {_esc(silver.get("checksum", "—"))}</p>
        <p class="path">{_esc(silver.get("path", ""))}</p>
        {_schema_table(silver.get("schema") or [])}
      </div>
      {"".join(gold_bits) or "<div class='layer gold'><h3>Gold</h3><p class='muted'>Not written yet.</p></div>"}
    </div>
    <h2>Gold KPIs</h2>
    {_kpi_cards(manifest.get("kpis") or {})}
    <h2>Validation and data quality</h2>
    {_quality(quality if isinstance(quality, dict) else {})}
    <h2>Lineage</h2>
    <p class="muted">{_esc(summary.get("event_count", 0))} Spark write events in this run.</p>
    {_lineage_svg(summary)}
  </main>
</body>
</html>
"""
    return html


@app.get("/", response_class=HTMLResponse)
def dashboard(run: str | None = None) -> HTMLResponse:
    runs = _runs()
    chosen = next((p for p in runs if p.name == run), runs[0] if runs else None)
    return HTMLResponse(render(chosen))


class DariaMedallionPlugin(AirflowPlugin):
    name = "daria_medallion"
    fastapi_apps = [
        {"app": app, "url_prefix": "/daria-medallion", "name": "spark-daria Medallion"}
    ]
    external_views = [
        {
            "name": "spark-daria Medallion",
            "href": "/daria-medallion/",
            "destination": "nav",
            "url_route": "daria-medallion",
            "category": "browse",
            "nav_top_level": True,
        }
    ]

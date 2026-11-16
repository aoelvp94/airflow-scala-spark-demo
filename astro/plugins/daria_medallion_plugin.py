from __future__ import annotations

from collections import defaultdict
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
    return (
        "<div class='table-scroll'><table>"
        "<thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


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
        cards.append(
            f"<div class='kpi'><div class='label'>{_esc(label)}</div>"
            f"<div class='value'>{_esc(text)}</div></div>"
        )
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
    table = (
        "<div class='table-scroll'><table><thead><tr><th>Check</th><th>Status</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
        if rows
        else ""
    )
    return table + (f"<ul class='metrics'>{metrics}</ul>" if metrics else "")


def _short(name: object) -> str:
    text = str(name or "").replace("file:", "").rstrip("/")
    parts = [p for p in text.split("/") if p]
    if not parts:
        return text
    last = parts[-1]
    parent = parts[-2] if len(parts) >= 2 else ""
    return f"gold/{last}" if parent == "gold" else last


def _dataset_nodes(summary: dict) -> list[str]:
    names: list[str] = []

    def add(name: object) -> None:
        short = _short(name) if name else ""
        if short and short not in names:
            names.append(short)

    for edge in summary.get("edges") or []:
        if isinstance(edge, dict):
            add(edge.get("from"))
            add(edge.get("to"))
    for d in summary.get("datasets") or []:
        if isinstance(d, dict):
            add(d.get("short") or d.get("name"))
    return names


def _dataset_edges(summary: dict, names: list[str]) -> list[tuple[str, str]]:
    name_set = set(names)
    edges: list[tuple[str, str]] = []
    for edge in summary.get("edges") or []:
        if not isinstance(edge, dict):
            continue
        src, dst = _short(edge.get("from")), _short(edge.get("to"))
        if src in name_set and dst in name_set and src != dst and (src, dst) not in edges:
            edges.append((src, dst))
    if edges:
        return edges
    # Fall back to a bronze → silver → gold* chain when summary has no edges.
    bronze = [n for n in names if n == "bronze"]
    silver = [n for n in names if n == "silver"]
    gold = [n for n in names if n.startswith("gold/") or n in {"revenue_by_country_month", "customer_segments", "vip_metrics"}]
    fallback: list[tuple[str, str]] = []
    if bronze and silver:
        fallback.append((bronze[0], silver[0]))
    if silver:
        fallback.extend((silver[0], g) for g in gold or [n for n in names if n not in bronze + silver])
    return fallback


def _levels(names: list[str], edges: list[tuple[str, str]]) -> dict[int, list[str]]:
    incoming: dict[str, set[str]] = defaultdict(set)
    for src, dst in edges:
        incoming[dst].add(src)
    level = {n: 0 for n in names}
    for _ in range(len(names) + 1):
        changed = False
        for src, dst in edges:
            nxt = level.get(src, 0) + 1
            if nxt > level.get(dst, 0):
                level[dst] = nxt
                changed = True
        if not changed:
            break
    preferred = {"bronze": 0, "silver": 1}
    for n in names:
        if n in preferred:
            level[n] = preferred[n]
        elif n.startswith("gold/") or n in {"revenue_by_country_month", "customer_segments", "vip_metrics"}:
            level[n] = max(level.get(n, 0), 2)
    cols: dict[int, list[str]] = defaultdict(list)
    for n in names:
        cols[level[n]].append(n)
    for items in cols.values():
        items.sort()
    return dict(sorted(cols.items()))


def _node_fill(name: str) -> str:
    if name == "bronze":
        return "#9a6700"
    if name == "silver":
        return "#6e7681"
    if name.startswith("gold/") or name in {"revenue_by_country_month", "customer_segments", "vip_metrics"}:
        return "#9a6b00"
    return "#1f6feb"


def _lineage_svg(summary: dict) -> str:
    names = _dataset_nodes(summary)
    if not names:
        return "<p class='muted'>No lineage events yet.</p>"
    edges = _dataset_edges(summary, names)
    cols = _levels(names, edges)
    col_index = {name: i for i, items in cols.items() for name in items}
    row_index = {name: items.index(name) for items in cols.values() for name in items}
    node_w = max(180, min(280, 12 * max(len(n) for n in names) + 24))
    node_h = 48
    h_gap = 96
    v_gap = 28
    margin_x, margin_y = 24, 28
    max_rows = max(len(items) for items in cols.values())
    num_cols = len(cols)
    width = margin_x * 2 + num_cols * node_w + (num_cols - 1) * h_gap
    height = margin_y * 2 + max_rows * node_h + (max_rows - 1) * v_gap
    levels = list(cols.keys())

    def pos(name: str) -> tuple[float, float]:
        col = col_index[name]
        row = row_index[name]
        rows_in_col = len(cols[col])
        col_height = rows_in_col * node_h + (rows_in_col - 1) * v_gap
        x = margin_x + levels.index(col) * (node_w + h_gap)
        y = margin_y + (height - 2 * margin_y - col_height) / 2 + row * (node_h + v_gap)
        return x, y

    coords = {name: pos(name) for name in names}
    edge_paths = []
    for src, dst in edges:
        x1, y1 = coords[src]
        x2, y2 = coords[dst]
        start_x, start_y = x1 + node_w, y1 + node_h / 2
        end_x, end_y = x2, y2 + node_h / 2
        mid_x = (start_x + end_x) / 2
        edge_paths.append(
            f"<path d='M {start_x:.1f} {start_y:.1f} C {mid_x:.1f} {start_y:.1f}, "
            f"{mid_x:.1f} {end_y:.1f}, {end_x:.1f} {end_y:.1f}' "
            "fill='none' stroke='#8b949e' stroke-width='2' marker-end='url(#arrow)' />"
        )
    nodes = []
    for name in names:
        x, y = coords[name]
        nodes.append(
            f"<rect x='{x:.1f}' y='{y:.1f}' width='{node_w}' height='{node_h}' rx='10' fill='{_node_fill(name)}' />"
            f"<text x='{x + node_w / 2:.1f}' y='{y + node_h / 2 + 5:.1f}' text-anchor='middle' fill='white' "
            f"font-size='13' font-family='ui-sans-serif, system-ui, sans-serif'>{_esc(name)}</text>"
        )
    svg = (
        f"<div class='graph-scroll'><svg viewBox='0 0 {width} {height}' width='100%' "
        f"style='max-width:{int(width)}px;height:auto;min-height:{int(height)}px' role='img' "
        "aria-label='Dataset lineage from bronze through silver to gold tables'>"
        "<defs><marker id='arrow' markerWidth='8' markerHeight='8' refX='6' refY='4' orient='auto'>"
        "<path d='M0,0 L8,4 L0,8 z' fill='#8b949e' /></marker></defs>"
        + "".join(edge_paths + nodes)
        + "</svg></div>"
    )
    return svg + _column_lineage_tables(summary)


def _column_inputs(raw: object) -> str:
    if isinstance(raw, list):
        parts = []
        for item in raw:
            if isinstance(item, dict):
                field = str(item.get("field") or item.get("name") or "")
                dataset = str(item.get("dataset") or "")
                parts.append(f"{dataset}.{field}" if dataset and field else field or str(item))
            else:
                parts.append(str(item))
        return ", ".join(p for p in parts if p)
    return "" if raw is None else str(raw)


def _column_lineage_tables(summary: dict) -> str:
    cols = [c for c in (summary.get("columnLineage") or []) if isinstance(c, dict)]
    if not cols:
        return "<p class='muted'>No column-level edges captured.</p>"
    groups: dict[str, list[dict]] = {}
    order: list[str] = []
    for c in cols:
        key = str(c.get("outputDataset") or "unknown")
        if key not in groups:
            order.append(key)
            groups[key] = []
        groups[key].append(c)
    blocks = []
    for dataset in order:
        rows = []
        for c in groups[dataset]:
            output = str(c.get("output") or "")
            path = str(c.get("path") or "").strip()
            inputs = _column_inputs(c.get("inputs"))
            if not path:
                path = f"{inputs} → {output}" if inputs else output
            # Identity self-mappings are dropped in the Scala summary; skip leftovers.
            collapsed = [p.strip() for part in path.split("; ") for p in part.split("→") if p.strip()]
            if collapsed and all(item == output for item in collapsed):
                continue
            rows.append(f"<tr><td>{_esc(output)}</td><td>{_esc(path)}</td></tr>")
        if not rows:
            continue
        blocks.append(
            f"<h3 class='lineage-target'>{_esc(dataset)}</h3>"
            "<div class='table-scroll'><table>"
            "<thead><tr><th>Output column</th><th>Lineage</th></tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table></div>"
        )
    return "".join(blocks) or "<p class='muted'>No rename or derivation edges captured.</p>"


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
    main {{ max-width: 1180px; margin: 0 auto; padding: 24px; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h1 {{ font-size: 1.6rem; }}
    h2 {{ margin-top: 28px; font-size: 1.15rem; color: #79c0ff; }}
    h3.lineage-target {{ margin-top: 18px; font-size: 0.98rem; color: #e6edf3; }}
    .muted, .path {{ color: #8b949e; font-size: 0.9rem; word-break: break-all; }}
    .layers {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 340px), 1fr)); gap: 16px; align-items: start; }}
    .layer {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 16px; min-width: 0; overflow: hidden; }}
    .layer.bronze {{ border-top: 4px solid #d29922; }}
    .layer.silver {{ border-top: 4px solid #8b949e; }}
    .layer.gold {{ border-top: 4px solid #e3b341; }}
    .table-scroll, .graph-scroll {{ overflow-x: auto; max-width: 100%; }}
    table {{ width: max-content; min-width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 0.92rem; }}
    th, td {{ border-bottom: 1px solid #30363d; padding: 6px 8px; text-align: left; vertical-align: top; white-space: nowrap; }}
    .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }}
    .kpi {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 12px; min-width: 0; }}
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
    </div>
    <h2>Gold tables</h2>
    <div class="layers">
      {"".join(gold_bits) or "<div class='layer gold'><h3>Gold</h3><p class='muted'>Not written yet.</p></div>"}
    </div>
    <h2>Gold KPIs</h2>
    {_kpi_cards(manifest.get("kpis") or {})}
    <h2>Validation and data quality</h2>
    {_quality(quality if isinstance(quality, dict) else {})}
    <h2>Lineage</h2>
    <p class="muted">{_esc(summary.get("event_count", 0))} Spark write events in this run. Dataset graph is left to right: bronze → silver → gold tables. Column lineage is grouped by target dataset and shows renames and derivations across layers.</p>
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

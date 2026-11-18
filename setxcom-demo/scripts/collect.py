#!/usr/bin/env python3
"""Collect XCom values, task states, and patched-task error messages."""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

TBILISI = timezone(timedelta(hours=4))

CONTROL_TASKS = ("extract", "score", "push_as_java", "push_java_pojo")
SCALA_TASKS = ("push_scala_map", "push_scala_seq", "push_scala_option", "push_case_class")


def to_tbilisi(ts: str | None) -> str | None:
    if not ts:
        return None
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # sqlite often stores naive UTC
    try:
        if "+" in s[10:] or s.endswith("UTC"):
            dt = datetime.fromisoformat(s.replace("UTC", "+00:00"))
        else:
            dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except ValueError:
        return ts
    return dt.astimezone(TBILISI).strftime("%Y-%m-%d %H:%M:%S") + " Asia/Tbilisi (UTC+4)"


def read_run_state(run_id: str, path: Path) -> str:
    rows = json.loads(path.read_text() or "[]")
    for row in rows:
        if row.get("run_id") == run_id:
            return row.get("state") or ""
    return ""


def load_xcoms(db_path: Path, run_id: str) -> dict[str, dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    xcom_table = next((t for t in tables if t.lower() in {"xcom_v2", "xcom"}), None)
    out: dict[str, dict] = {}
    if not xcom_table:
        return out
    cols = {c[1] for c in conn.execute(f"PRAGMA table_info({xcom_table})")}
    # Prefer joining to task_instance when xcom_v2 uses task_instance_id.
    if "task_instance_id" in cols and "task_instance" in tables:
        q = f"""
        SELECT ti.task_id AS task_id, ti.run_id AS run_id, x.key AS key, x.value AS value, x.timestamp AS timestamp
        FROM {xcom_table} x
        JOIN task_instance ti ON ti.id = x.task_instance_id
        WHERE ti.run_id = ?
        """
        rows = conn.execute(q, (run_id,)).fetchall()
    elif "run_id" in cols:
        rows = conn.execute(f"SELECT * FROM {xcom_table} WHERE run_id = ?", (run_id,)).fetchall()
    else:
        rows = conn.execute(f"SELECT * FROM {xcom_table}").fetchall()
    for row in rows:
        d = {k: (v.decode() if isinstance(v, (bytes, bytearray)) else v) for k, v in dict(row).items()}
        task_id = d.get("task_id")
        if not task_id:
            continue
        val = d.get("value")
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except json.JSONDecodeError:
                pass
        out[task_id] = {"key": d.get("key"), "value": val, "timestamp": d.get("timestamp")}
    return out


def parse_errors(log_dir: Path) -> dict[str, str]:
    errors: dict[str, str] = {}
    if not log_dir.exists():
        return errors
    # Airflow 3 task logs are often JSON lines with event.error
    for path in log_dir.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(errors="replace")
        task_id = None
        # path typically contains /dag_id=.../run_id=.../task_id=NAME/
        m = re.search(r"task_id=([^/]+)", str(path))
        if m:
            task_id = m.group(1)
        found = None
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                if "IllegalArgumentException" in line and found is None:
                    found = line
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            err = ev.get("error") or ev.get("event", {}).get("error") if isinstance(ev.get("event"), dict) else ev.get("error")
            if not err and isinstance(ev, dict):
                # sometimes the message itself is the exception
                msg = ev.get("event") if isinstance(ev.get("event"), str) else ev.get("message")
                if msg and "IllegalArgumentException" in str(msg):
                    err = msg
            if err and "IllegalArgumentException" in str(err):
                found = str(err)
        if task_id and found:
            errors[task_id] = found
    return errors


def collect(demo_root: Path, mode: str, run_id: str) -> None:
    evid = demo_root / "evidence"
    states = json.loads((evid / f"{mode}-states.json").read_text())
    xcoms = load_xcoms(evid / f"{mode}-airflow.db", run_id)
    errors = parse_errors(evid / f"logs-{mode}")
    runs = json.loads((evid / f"{mode}-runs.json").read_text() or "[]")
    run_row = next((r for r in runs if r.get("run_id") == run_id), {})

    rows = []
    for st in states:
        task_id = st["task_id"]
        xc = xcoms.get(task_id) or {}
        rows.append(
            {
                "task_id": task_id,
                "state": st.get("state"),
                "start_date_tbilisi": to_tbilisi(st.get("start_date")),
                "end_date_tbilisi": to_tbilisi(st.get("end_date")),
                "xcom_key": xc.get("key"),
                "xcom_value": xc.get("value"),
                "error": errors.get(task_id),
            }
        )
    rows.sort(key=lambda r: (CONTROL_TASKS + SCALA_TASKS).index(r["task_id"]) if r["task_id"] in CONTROL_TASKS + SCALA_TASKS else 99)

    payload = {
        "mode": mode,
        "dag_id": run_row.get("dag_id") or "setxcom_fix",
        "run_id": run_id,
        "dag_state": run_row.get("state"),
        "start_date_tbilisi": to_tbilisi(run_row.get("start_date")),
        "end_date_tbilisi": to_tbilisi(run_row.get("end_date")),
        "tasks": rows,
    }
    (evid / f"{mode}-xcom.json").write_text(json.dumps(payload, indent=2) + "\n")
    if errors:
        (evid / f"{mode}-errors.json").write_text(json.dumps(errors, indent=2) + "\n")
    print(json.dumps({"run_id": run_id, "dag_state": payload["dag_state"], "tasks": {r["task_id"]: r["state"] for r in rows}}, indent=2))


def write_evidence_md(demo_root: Path) -> None:
    evid = demo_root / "evidence"
    parts = [
        "# Reviewer demo evidence",
        "",
        "Zone: Asia/Tbilisi (UTC+4). Timestamps below are converted from UTC.",
        "These runs were produced by the README steps in this directory (local only; nothing was published).",
        "",
    ]
    for mode in ("baseline", "patched"):
        path = evid / f"{mode}-xcom.json"
        if not path.exists():
            continue
        data = json.loads(path.read_text())
        parts.append(f"## {mode}")
        parts.append("")
        parts.append(f"- dag_id: `{data['dag_id']}`")
        parts.append(f"- run_id: `{data['run_id']}`")
        parts.append(f"- start: {data.get('start_date_tbilisi')}")
        parts.append(f"- end: {data.get('end_date_tbilisi')}")
        parts.append(f"- DAG state: **{data.get('dag_state')}**")
        parts.append("")
        parts.append("| task | state | XCom `return_value` | error |")
        parts.append("|---|---|---|---|")
        for t in data["tasks"]:
            val = t.get("xcom_value")
            if val is None:
                val_s = "_(none written)_"
            else:
                val_s = f"`{json.dumps(val, ensure_ascii=False)}`"
            err = t.get("error") or ""
            if err:
                err = f"`{err}`"
            parts.append(f"| `{t['task_id']}` | {t['state']} | {val_s} | {err} |")
        parts.append("")
    (evid / "EVIDENCE.md").write_text("\n".join(parts) + "\n")
    print(f"Wrote {evid / 'EVIDENCE.md'}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--read-run-state", nargs=2, metavar=("RUN_ID", "RUNS_JSON"))
    p.add_argument("--mode")
    p.add_argument("--run-id")
    p.add_argument("--demo-root")
    p.add_argument("--write-evidence-md", action="store_true")
    args = p.parse_args()
    if args.read_run_state:
        print(read_run_state(args.read_run_state[0], Path(args.read_run_state[1])), end="")
        return
    if args.write_evidence_md:
        write_evidence_md(Path(args.demo_root or Path(__file__).resolve().parents[1]))
        return
    if not (args.mode and args.run_id and args.demo_root):
        raise SystemExit("collect.py: need --mode --run-id --demo-root, or --read-run-state, or --write-evidence-md")
    collect(Path(args.demo_root), args.mode, args.run_id)


if __name__ == "__main__":
    main()

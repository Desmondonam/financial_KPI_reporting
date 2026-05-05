#!/usr/bin/env python3
"""
Step 1: Ingest 100k Lending Club rows into PostgreSQL and validate
with Great Expectations. Generates an HTML validation report.

Usage:
    DB_HOST=localhost DB_PORT=5432 DB_NAME=lending_club \
    DB_USER=postgres DB_PASSWORD=postgres \
    python scripts/ingest_and_validate.py
"""

import os
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "lending_club")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

DATA_FILE = Path("data/accepted_2007_to_2018q4.csv/accepted_2007_to_2018Q4.csv")
NROWS     = 100_000
REPORT_PATH = Path("reports/validation_report.html")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def ingest(engine: object) -> pd.DataFrame:
    print(f"[ingest] Reading {NROWS:,} rows from {DATA_FILE} …")
    df = pd.read_csv(DATA_FILE, nrows=NROWS, low_memory=False)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    print(f"[ingest] Loading to raw.lending_club_loans …")
    df.to_sql(
        "lending_club_loans",
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=2_000,
        method="multi",
    )
    print(f"[ingest] Loaded {len(df):,} rows ✓")
    return df


# ── Great Expectations validation ─────────────────────────────────────────────

def run_validation(df: pd.DataFrame) -> list[dict]:
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")

    source = context.data_sources.add_pandas("pandas_source")
    asset  = source.add_dataframe_asset("lending_club_loans")
    batch_def = asset.add_batch_definition_whole_dataframe("full_batch")

    suite = context.suites.add(gx.ExpectationSuite(name="lending_club_suite"))

    # Check 1: loan_amnt must never be null
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="loan_amnt")
    )
    # Check 2: int_rate must be within 0–40 %
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="int_rate", min_value=0, max_value=40
        )
    )

    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="lending_club_validation",
            data=batch_def,
            suite=suite,
        )
    )

    results = val_def.run(batch_parameters={"dataframe": df})

    checks = []
    for r in results.results:
        config = r.expectation_config
        checks.append(
            {
                "expectation": config.type,
                "column":      config.column,
                "kwargs":      config.kwargs,
                "success":     bool(r.success),
                "result":      r.result,
            }
        )

    overall = bool(results.success)
    print(f"[GE] Validation {'PASSED ✓' if overall else 'FAILED ✗'}")
    for c in checks:
        status = "✓" if c["success"] else "✗"
        print(f"  {status}  {c['expectation']}({c['column']})")

    return checks, overall


# ── HTML report ───────────────────────────────────────────────────────────────

def build_html(checks: list[dict], overall: bool, row_count: int) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    badge_color = "#28a745" if overall else "#dc3545"
    badge_text  = "PASSED" if overall else "FAILED"

    rows_html = ""
    for c in checks:
        icon   = "✅" if c["success"] else "❌"
        status = "PASSED" if c["success"] else "FAILED"
        kw     = json.dumps(c["kwargs"], default=str)
        res    = json.dumps(c["result"],  default=str)
        rows_html += f"""
        <tr class="{'pass' if c['success'] else 'fail'}">
          <td>{icon} {status}</td>
          <td><code>{c['expectation']}</code></td>
          <td><code>{c['column']}</code></td>
          <td><small>{kw}</small></td>
          <td><small>{res}</small></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Great Expectations — Lending Club Validation Report</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#f4f6f8;color:#333;padding:2rem}}
    header{{background:#1a2b45;color:#fff;padding:2rem 2.5rem;border-radius:10px;margin-bottom:2rem}}
    header h1{{font-size:1.7rem;margin-bottom:.4rem}}
    header p{{opacity:.8;font-size:.95rem}}
    .badge{{display:inline-block;background:{badge_color};color:#fff;padding:.35rem 1rem;
            border-radius:20px;font-weight:700;font-size:1.1rem;margin-top:.8rem}}
    .card{{background:#fff;border-radius:10px;padding:1.5rem 2rem;margin-bottom:1.5rem;
           box-shadow:0 2px 8px rgba(0,0,0,.06)}}
    .card h2{{font-size:1.1rem;margin-bottom:1rem;color:#1a2b45;border-bottom:2px solid #e2e8f0;
              padding-bottom:.5rem}}
    .metrics{{display:flex;gap:1.5rem;flex-wrap:wrap}}
    .metric{{flex:1;min-width:160px;background:#f8fafc;border-radius:8px;padding:1rem 1.2rem;
             text-align:center;border:1px solid #e2e8f0}}
    .metric-value{{font-size:2rem;font-weight:700;color:#1a2b45}}
    .metric-label{{font-size:.8rem;color:#64748b;margin-top:.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.9rem}}
    th{{background:#1a2b45;color:#fff;padding:.7rem 1rem;text-align:left}}
    td{{padding:.65rem 1rem;border-bottom:1px solid #e2e8f0;vertical-align:top}}
    tr.pass td:first-child{{color:#16a34a;font-weight:600}}
    tr.fail td:first-child{{color:#dc2626;font-weight:600}}
    tr:hover{{background:#f8fafc}}
    code{{background:#f1f5f9;padding:.1rem .35rem;border-radius:4px;font-size:.85rem}}
    footer{{text-align:center;font-size:.8rem;color:#94a3b8;margin-top:2rem}}
  </style>
</head>
<body>
  <header>
    <h1>Great Expectations — Lending Club Validation Report</h1>
    <p>Generated: {ts} &nbsp;|&nbsp; Dataset: raw.lending_club_loans ({row_count:,} rows)</p>
    <div class="badge">{badge_text}</div>
  </header>

  <div class="card">
    <h2>Summary Metrics</h2>
    <div class="metrics">
      <div class="metric">
        <div class="metric-value">{row_count:,}</div>
        <div class="metric-label">Rows Validated</div>
      </div>
      <div class="metric">
        <div class="metric-value">{len(checks)}</div>
        <div class="metric-label">Expectations Run</div>
      </div>
      <div class="metric">
        <div class="metric-value" style="color:#16a34a">{sum(1 for c in checks if c['success'])}</div>
        <div class="metric-label">Passed</div>
      </div>
      <div class="metric">
        <div class="metric-value" style="color:#dc2626">{sum(1 for c in checks if not c['success'])}</div>
        <div class="metric-label">Failed</div>
      </div>
    </div>
  </div>

  <div class="card">
    <h2>Expectation Results</h2>
    <table>
      <thead>
        <tr>
          <th>Status</th><th>Expectation</th><th>Column</th>
          <th>Parameters</th><th>Result Detail</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>

  <footer>Lending Club Data Engineering Pipeline &mdash; Great Expectations v1.x</footer>
</body>
</html>
"""


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    if not DATA_FILE.exists():
        sys.exit(
            f"[error] Data file not found: {DATA_FILE}\n"
            "Run: dvc pull  (to fetch the tracked data file)"
        )

    engine = get_engine()
    df     = ingest(engine)
    checks, overall = run_validation(df)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    html = build_html(checks, overall, len(df))
    REPORT_PATH.write_text(html, encoding="utf-8")
    print(f"[report] Saved → {REPORT_PATH}")

    if not overall:
        sys.exit(1)


if __name__ == "__main__":
    main()

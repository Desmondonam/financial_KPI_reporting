#!/usr/bin/env python3
"""
CI seed script — runs in GitHub Actions to populate PostgreSQL with the
dbt seed CSV data and produce a lightweight Great Expectations report.

This does NOT require the full DVC-tracked 1.6 GB CSV; it uses the
40-row seed file checked into the repository.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5433"))
DB_NAME     = os.getenv("DB_NAME", "lending_club")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

SEED_CSV    = Path("lending_club_dbt/seeds/lending_club_loans.csv")
REPORT_PATH = Path("reports/validation_report.html")


def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def setup_db(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS lending_club"))
    print("[db] Schemas created: raw, staging, mart, lending_club")


def load_seed(engine):
    df = pd.read_csv(SEED_CSV, low_memory=False)
    df.columns = df.columns.str.strip().str.lower()
    df.to_sql(
        "lending_club_loans",
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method="multi",
    )
    print(f"[seed] Loaded {len(df)} rows -> raw.lending_club_loans")
    return df


def run_ge_validation(df: pd.DataFrame) -> tuple[list[dict], bool]:
    try:
        import great_expectations as gx

        context = gx.get_context(mode="ephemeral")
        source  = context.data_sources.add_pandas("ci_source")
        asset   = source.add_dataframe_asset("lending_club_loans")
        batch_def = asset.add_batch_definition_whole_dataframe("batch")

        suite = context.suites.add(gx.ExpectationSuite(name="ci_suite"))
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="loan_amnt")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="int_rate", min_value=0, max_value=40
            )
        )

        val_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name="ci_validation", data=batch_def, suite=suite
            )
        )
        results = val_def.run(batch_parameters={"dataframe": df})

        checks = [
            {
                "expectation": r.expectation_config.type,
                "column":      r.expectation_config.kwargs.get("column", ""),
                "kwargs":      r.expectation_config.kwargs,
                "success":     bool(r.success),
                "result":      r.result,
            }
            for r in results.results
        ]
        return checks, bool(results.success)

    except Exception as exc:
        print(f"[GE] Warning: could not run GE ({exc}). Falling back to pandas checks.")
        return _pandas_fallback(df)


def _pandas_fallback(df: pd.DataFrame) -> tuple[list[dict], bool]:
    checks = []
    null_count = int(df["loan_amnt"].isna().sum())
    checks.append(
        {
            "expectation": "expect_column_values_to_not_be_null",
            "column": "loan_amnt",
            "kwargs": {},
            "success": null_count == 0,
            "result": {"unexpected_count": null_count},
        }
    )
    oor = int(((df["int_rate"] < 0) | (df["int_rate"] > 40)).sum())
    checks.append(
        {
            "expectation": "expect_column_values_to_be_between",
            "column": "int_rate",
            "kwargs": {"min_value": 0, "max_value": 40},
            "success": oor == 0,
            "result": {"unexpected_count": oor},
        }
    )
    overall = all(c["success"] for c in checks)
    return checks, overall


def build_html(checks, overall, row_count):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    badge_color = "#28a745" if overall else "#dc3545"
    badge_text  = "PASSED" if overall else "FAILED"

    rows_html = ""
    for c in checks:
        icon   = "✅" if c["success"] else "❌"
        status = "PASSED" if c["success"] else "FAILED"
        kw     = json.dumps(c["kwargs"],  default=str)
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
  <title>Great Expectations — CI Validation Report</title>
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
  <footer>Lending Club Data Engineering Pipeline &mdash; CI Validation Run</footer>
</body>
</html>
"""


def main():
    engine = get_engine()
    setup_db(engine)
    df = load_seed(engine)

    checks, overall = run_ge_validation(df)

    for c in checks:
        status = "PASSED" if c["success"] else "FAILED"
        print(f"  [{status}] {c['expectation']}({c['column']})")

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    html = build_html(checks, overall, len(df))
    REPORT_PATH.write_text(html, encoding="utf-8")
    print(f"[report] Saved → {REPORT_PATH}")

    if not overall:
        sys.exit(1)
    print("[ci_seed] Done ✓")


if __name__ == "__main__":
    main()

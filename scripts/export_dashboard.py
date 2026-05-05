#!/usr/bin/env python3
"""
Generates a self-contained HTML dashboard (and PDF via weasyprint if installed)
by querying the mart layer directly.  Runs independently of Metabase.

Output files:
    reports/dashboard.html   — always generated
    reports/dashboard.pdf    — generated if weasyprint is installed

Usage:
    python scripts/export_dashboard.py
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text as sa_text

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5433"))
DB_NAME     = os.getenv("DB_NAME",     "lending_club")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

HTML_OUT = Path("reports/dashboard.html")
PDF_OUT  = Path("reports/dashboard.pdf")


def engine():
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def q(sql: str) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(sa_text(sql), conn)


# ── KPI queries ───────────────────────────────────────────────────────────────

def fetch_kpis() -> dict:
    kpi = {}

    kpi["total_loans"] = int(
        q("SELECT COUNT(*) AS n FROM staging.stg_loans").iloc[0, 0]
    )
    kpi["default_rate"] = float(
        q("SELECT ROUND(AVG(is_default::numeric)*100,2) AS v FROM staging.stg_loans")
        .iloc[0, 0]
    )
    kpi["avg_int_rate"] = float(
        q("SELECT ROUND(AVG(interest_rate),2) AS v FROM staging.stg_loans")
        .iloc[0, 0]
    )
    kpi["total_interest_revenue"] = float(
        q("SELECT ROUND(SUM(total_interest_received),0) AS v FROM staging.stg_loans")
        .iloc[0, 0]
    )

    top5 = q("""
        SELECT state AS "State", loan_count AS "Loan Count",
               total_loan_volume_usd AS "Total Volume ($)"
        FROM mart.mart_avg_int_rate_by_state
        ORDER BY loan_count DESC LIMIT 5
    """)
    kpi["top5_states"] = top5

    monthly = q("""
        SELECT month AS "Month", loan_count AS "Loans",
               ROUND(total_originated_usd,0) AS "Originated ($)"
        FROM mart.mart_monthly_originations
        ORDER BY month DESC LIMIT 12
    """)
    kpi["monthly"] = monthly

    grade = q("""
        SELECT grade AS "Grade", total_loans AS "Total",
               default_rate_pct AS "Default Rate (%)",
               round(avg_loan_amount_usd,0) AS "Avg Loan ($)"
        FROM mart.mart_default_rate_by_grade
        ORDER BY grade
    """)
    kpi["grade"] = grade

    return kpi


# ── HTML helpers ──────────────────────────────────────────────────────────────

def df_to_html(df: pd.DataFrame) -> str:
    return df.to_html(
        index=False,
        border=0,
        classes="data-table",
        na_rep="—",
    )


def bar_chart_svg(labels: list, values: list, color: str = "#2563eb") -> str:
    max_v   = max(values) if values else 1
    width   = 540
    height  = 220
    pad_l   = 60
    pad_b   = 40
    pad_t   = 20
    pad_r   = 20
    inner_w = width  - pad_l - pad_r
    inner_h = height - pad_t - pad_b
    n       = len(labels)
    bar_w   = int(inner_w / n * 0.6)
    gap     = inner_w / n

    bars = ""
    for i, (lbl, val) in enumerate(zip(labels, values)):
        bh  = int(val / max_v * inner_h)
        x   = pad_l + int(gap * i + gap * 0.2)
        y   = pad_t + inner_h - bh
        bars += (
            f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bh}" '
            f'fill="{color}" rx="3"/>'
            f'<text x="{x + bar_w//2}" y="{pad_t + inner_h + 18}" '
            f'text-anchor="middle" font-size="11" fill="#475569">{lbl}</text>'
            f'<text x="{x + bar_w//2}" y="{y - 4}" '
            f'text-anchor="middle" font-size="10" fill="#1e40af" font-weight="600">'
            f'{val:,}</text>'
        )

    # y-axis
    axis = (
        f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" '
        f'y2="{pad_t+inner_h}" stroke="#cbd5e1" stroke-width="1"/>'
        f'<line x1="{pad_l}" y1="{pad_t+inner_h}" x2="{pad_l+inner_w}" '
        f'y2="{pad_t+inner_h}" stroke="#cbd5e1" stroke-width="1"/>'
    )

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {width} {height}" width="{width}" height="{height}">'
        f'{axis}{bars}</svg>'
    )


# ── Main HTML build ───────────────────────────────────────────────────────────

def build_html(kpi: dict) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    top5   = kpi["top5_states"]
    labels = top5["State"].tolist()
    values = top5["Loan Count"].tolist()
    chart  = bar_chart_svg(labels, values)

    def fmt_usd(v):
        return f"${v:,.0f}"

    def fmt_pct(v):
        return f"{v:.2f} %"

    def fmt_int(v):
        return f"{v:,}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Lending Club — Executive KPI Dashboard</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:'Segoe UI',Arial,sans-serif;background:#f1f5f9;color:#1e293b;padding:2rem}}
    header{{background:linear-gradient(135deg,#1a2b45 0%,#2563eb 100%);color:#fff;
            padding:2rem 2.5rem;border-radius:14px;margin-bottom:2rem}}
    header h1{{font-size:1.9rem;font-weight:700;margin-bottom:.3rem}}
    header p{{opacity:.8;font-size:.95rem}}
    .kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:1.2rem;margin-bottom:1.5rem}}
    .kpi{{background:#fff;border-radius:12px;padding:1.4rem 1.6rem;
          box-shadow:0 2px 10px rgba(0,0,0,.06);border-top:4px solid #2563eb}}
    .kpi-value{{font-size:2.1rem;font-weight:800;color:#1a2b45;margin-bottom:.2rem}}
    .kpi-label{{font-size:.82rem;color:#64748b;text-transform:uppercase;letter-spacing:.06em}}
    .kpi:nth-child(2){{border-color:#dc2626}}
    .kpi:nth-child(2) .kpi-value{{color:#dc2626}}
    .kpi:nth-child(3){{border-color:#16a34a}}
    .kpi:nth-child(3) .kpi-value{{color:#16a34a}}
    .kpi:nth-child(4){{border-color:#9333ea}}
    .kpi:nth-child(4) .kpi-value{{color:#9333ea}}
    .card{{background:#fff;border-radius:12px;padding:1.5rem 2rem;margin-bottom:1.5rem;
           box-shadow:0 2px 10px rgba(0,0,0,.06)}}
    .card h2{{font-size:1.05rem;font-weight:700;color:#1a2b45;margin-bottom:1.2rem;
              border-bottom:2px solid #e2e8f0;padding-bottom:.6rem}}
    .two-col{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}}
    .data-table{{width:100%;border-collapse:collapse;font-size:.88rem}}
    .data-table th{{background:#1a2b45;color:#fff;padding:.6rem 1rem;text-align:left;
                    font-weight:600;font-size:.8rem;text-transform:uppercase}}
    .data-table td{{padding:.55rem 1rem;border-bottom:1px solid #f1f5f9}}
    .data-table tr:hover td{{background:#f8fafc}}
    .chart-wrap{{display:flex;justify-content:center}}
    footer{{text-align:center;font-size:.78rem;color:#94a3b8;margin-top:2rem}}
  </style>
</head>
<body>
  <header>
    <h1>Lending Club — Executive KPI Dashboard</h1>
    <p>Data source: mart layer (dbt) &nbsp;|&nbsp; Generated: {ts}</p>
  </header>

  <!-- 4 KPI scorecards -->
  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-value">{fmt_int(kpi['total_loans'])}</div>
      <div class="kpi-label">Total Loans</div>
    </div>
    <div class="kpi">
      <div class="kpi-value">{fmt_pct(kpi['default_rate'])}</div>
      <div class="kpi-label">Overall Default Rate</div>
    </div>
    <div class="kpi">
      <div class="kpi-value">{fmt_pct(kpi['avg_int_rate'])}</div>
      <div class="kpi-label">Avg Interest Rate</div>
    </div>
    <div class="kpi">
      <div class="kpi-value">{fmt_usd(kpi['total_interest_revenue'])}</div>
      <div class="kpi-label">Total Interest Revenue</div>
    </div>
  </div>

  <!-- Top-5 states: chart + table -->
  <div class="card">
    <h2>Top 5 US States by Loan Volume</h2>
    <div class="two-col">
      <div class="chart-wrap">{chart}</div>
      <div>{df_to_html(top5)}</div>
    </div>
  </div>

  <!-- Monthly originations + Grade breakdown -->
  <div class="two-col">
    <div class="card">
      <h2>Monthly Loan Originations (last 12 months)</h2>
      {df_to_html(kpi['monthly'])}
    </div>
    <div class="card">
      <h2>Default Rate by Loan Grade</h2>
      {df_to_html(kpi['grade'])}
    </div>
  </div>

  <footer>Lending Club Data Engineering Pipeline &mdash; mart.* via dbt &mdash; {ts}</footer>
</body>
</html>
"""


# ── PDF export ────────────────────────────────────────────────────────────────

def export_pdf(html_path: Path, pdf_path: Path):
    try:
        from weasyprint import HTML
        HTML(filename=str(html_path)).write_pdf(str(pdf_path))
        print(f"[pdf] Saved → {pdf_path}")
    except ImportError:
        print("[pdf] weasyprint not installed — skipping PDF. "
              "Install with: pip install weasyprint")
    except Exception as exc:
        print(f"[pdf] Warning: PDF export failed: {exc}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("[dashboard] Fetching KPIs from mart layer …")
    kpi  = fetch_kpis()
    html = build_html(kpi)

    HTML_OUT.parent.mkdir(parents=True, exist_ok=True)
    HTML_OUT.write_text(html, encoding="utf-8")
    print(f"[dashboard] HTML saved → {HTML_OUT}")

    export_pdf(HTML_OUT, PDF_OUT)

    print()
    print("KPI snapshot:")
    print(f"  Total loans            : {kpi['total_loans']:,}")
    print(f"  Overall default rate   : {kpi['default_rate']:.2f} %")
    print(f"  Avg interest rate      : {kpi['avg_int_rate']:.2f} %")
    print(f"  Total interest revenue : ${kpi['total_interest_revenue']:,.0f}")
    print("  Top-5 states:")
    for _, row in kpi["top5_states"].iterrows():
        print(f"    {row['State']}: {row['Loan Count']:,} loans")


if __name__ == "__main__":
    main()

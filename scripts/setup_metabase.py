#!/usr/bin/env python3
"""
Automates Metabase setup via the REST API:
  1. Waits for Metabase to become ready
  2. Runs initial setup (admin account + DB connection)
  3. Creates the 5 KPI cards + 1 bar chart
  4. Assembles the executive dashboard
  5. Enables public sharing and prints the live link

Usage (after docker-compose up -d && dbt run):
    python scripts/setup_metabase.py
"""

import os
import sys
import time
import json
import requests

# ── Config ────────────────────────────────────────────────────────────────────
MB_URL   = os.getenv("MB_URL",       "http://localhost:3001")
# Metabase runs inside Docker, so it reaches PG via the Docker service name
DB_HOST  = os.getenv("MB_DB_HOST",   "postgres")
DB_PORT  = int(os.getenv("MB_DB_PORT", "5432"))     # internal Docker port
DB_NAME  = os.getenv("DB_NAME",      "lending_club")
DB_USER  = os.getenv("DB_USER",      "postgres")
DB_PASS  = os.getenv("DB_PASSWORD",  "postgres")

ADMIN_EMAIL    = os.getenv("MB_ADMIN_EMAIL",    "admin@lendingclub.local")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "MetaAdmin1!")
ADMIN_FIRST    = "Admin"
ADMIN_LAST     = "User"
SITE_NAME      = "Lending Club Analytics"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

class MetabaseClient:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.base}{path}"

    def get(self, path, **kw):
        return self.session.get(self._url(path), **kw)

    def post(self, path, data=None, **kw):
        return self.session.post(self._url(path), json=data, **kw)

    def put(self, path, data=None, **kw):
        return self.session.put(self._url(path), json=data, **kw)

    def set_token(self, token: str):
        self.session.headers["X-Metabase-Session"] = token


# ── Startup wait ──────────────────────────────────────────────────────────────

def wait_for_metabase(client: MetabaseClient, timeout: int = 300):
    print("[wait] Waiting for Metabase to become ready …", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = client.get("/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print(" ready ✓")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(5)
    print()
    sys.exit("[error] Metabase did not become ready in time.")


# ── Initial setup ─────────────────────────────────────────────────────────────

def initial_setup(client: MetabaseClient) -> str:
    # Check if already set up
    r = client.get("/api/session/properties")
    props = r.json()
    setup_token = props.get("setup-token")

    if not setup_token:
        print("[setup] Already configured — logging in …")
        r = client.post("/api/session", {
            "username": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD
        })
        if r.status_code != 200:
            sys.exit(f"[error] Login failed: {r.text}")
        token = r.json()["id"]
        client.set_token(token)
        print("[setup] Logged in ✓")
        return token

    print("[setup] Running first-time setup …")
    payload = {
        "token": setup_token,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name":  ADMIN_LAST,
            "email":      ADMIN_EMAIL,
            "password":   ADMIN_PASSWORD,
            "site_name":  SITE_NAME,
        },
        "prefs": {
            "site_name":       SITE_NAME,
            "allow_tracking":  False,
        },
    }
    r = client.post("/api/setup", payload)
    if r.status_code == 403:
        # Already set up — fall through to login
        print("[setup] Already set up — logging in …")
        r2 = client.post("/api/session", {
            "username": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        if r2.status_code != 200:
            sys.exit(f"[error] Login failed: {r2.text}")
        token = r2.json()["id"]
        client.set_token(token)
        print("[setup] Logged in ✓")
        return token
    if r.status_code not in (200, 201):
        sys.exit(f"[error] Setup failed ({r.status_code}): {r.text}")

    token = r.json()["id"]
    client.set_token(token)
    print("[setup] Admin account created ✓")
    return token


# ── Database connection ───────────────────────────────────────────────────────

def add_database(client: MetabaseClient) -> int:
    # Check for existing DB
    r = client.get("/api/database")
    for db in r.json().get("data", []):
        if db.get("engine") == "postgres" and db.get("details", {}).get("dbname") == DB_NAME:
            db_id = db["id"]
            print(f"[db] Found existing database id={db_id} ✓")
            return db_id

    print("[db] Adding PostgreSQL database …")
    payload = {
        "name":    "Lending Club PostgreSQL",
        "engine":  "postgres",
        "details": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "dbname":   DB_NAME,
            "user":     DB_USER,
            "password": DB_PASS,
            "ssl":      False,
        },
    }
    r = client.post("/api/database", payload)
    if r.status_code not in (200, 201):
        sys.exit(f"[error] DB add failed ({r.status_code}): {r.text}")

    db_id = r.json()["id"]
    print(f"[db] Database added id={db_id} — syncing …")

    # Trigger metadata sync
    client.post(f"/api/database/{db_id}/sync_schema")
    time.sleep(8)
    print("[db] Sync triggered ✓")
    return db_id


# ── Card (question) helpers ───────────────────────────────────────────────────

def make_scalar(name: str, sql: str, db_id: int) -> dict:
    return {
        "name":          name,
        "display":       "scalar",
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native":   {"query": sql},
        },
        "visualization_settings": {
            "scalar.decimals": 2,
        },
    }


def make_bar(name: str, sql: str, db_id: int, x_col: str, y_col: str) -> dict:
    return {
        "name":          name,
        "display":       "bar",
        "dataset_query": {
            "type":     "native",
            "database": db_id,
            "native":   {"query": sql},
        },
        "visualization_settings": {
            "graph.dimensions":  [x_col],
            "graph.metrics":     [y_col],
            "graph.x_axis.axis_enabled": True,
            "graph.y_axis.axis_enabled": True,
        },
    }


def create_card(client: MetabaseClient, payload: dict) -> int:
    # Skip if a card with the same name already exists
    r = client.get("/api/card")
    for c in r.json():
        if c["name"] == payload["name"]:
            cid = c["id"]
            print(f"  [card] '{payload['name']}' already exists id={cid}")
            return cid

    r = client.post("/api/card", payload)
    if r.status_code not in (200, 201):
        sys.exit(f"[error] Card create failed ({r.status_code}): {r.text}")
    cid = r.json()["id"]
    print(f"  [card] Created '{payload['name']}' id={cid} ✓")
    return cid


# ── Dashboard assembly ────────────────────────────────────────────────────────

DASHBOARD_NAME = "Lending Club — Executive KPI Dashboard"


def create_dashboard(client: MetabaseClient) -> int:
    r = client.get("/api/dashboard")
    for d in r.json():
        if d["name"] == DASHBOARD_NAME:
            did = d["id"]
            print(f"[dashboard] Already exists id={did}")
            return did

    r = client.post("/api/dashboard", {
        "name":        DASHBOARD_NAME,
        "description": "5 KPI cards: total loans, default rate, avg int rate, "
                       "total interest revenue, top-5 states by loan volume.",
    })
    if r.status_code not in (200, 201):
        sys.exit(f"[error] Dashboard create failed: {r.text}")
    did = r.json()["id"]
    print(f"[dashboard] Created id={did} ✓")
    return did


def add_cards_to_dashboard(client: MetabaseClient, dashboard_id: int, card_ids: list[int]):
    r = client.get(f"/api/dashboard/{dashboard_id}")
    existing = {dc["card_id"] for dc in r.json().get("dashcards", [])}

    # 2×3 grid layout: row=0 kpi row, row=4 chart row
    kpi_layout = [
        {"row": 0, "col": 0,  "size_x": 4, "size_y": 3},  # total loans
        {"row": 0, "col": 4,  "size_x": 4, "size_y": 3},  # default rate
        {"row": 0, "col": 8,  "size_x": 4, "size_y": 3},  # avg int rate
        {"row": 0, "col": 12, "size_x": 4, "size_y": 3},  # total interest revenue
        {"row": 4, "col": 0,  "size_x": 16, "size_y": 7}, # top-5 states bar
    ]

    # Build dashcards list — include ALL cards (existing keep their real id, new ones use temp negative id)
    existing_map = {dc["card_id"]: dc["id"] for dc in r.json().get("dashcards", [])}

    dashcards = []
    for i, (card_id, layout) in enumerate(zip(card_ids, kpi_layout)):
        dashcards.append({
            "id":                     existing_map.get(card_id, -(i + 1)),
            "card_id":                card_id,
            "row":                    layout["row"],
            "col":                    layout["col"],
            "size_x":                 layout["size_x"],
            "size_y":                 layout["size_y"],
            "parameter_mappings":     [],
            "visualization_settings": {},
        })

    r = client.put(f"/api/dashboard/{dashboard_id}", {
        "dashcards": dashcards
    })
    if r.status_code not in (200, 202):
        sys.exit(f"[error] Adding cards failed ({r.status_code}): {r.text}")
    print(f"[dashboard] {len(dashcards)} cards attached ✓")


# ── Public sharing ────────────────────────────────────────────────────────────

def enable_public_link(client: MetabaseClient, dashboard_id: int) -> str:
    # Enable public sharing site-wide
    client.put("/api/setting/enable-public-sharing", {"value": True})

    r = client.post(f"/api/dashboard/{dashboard_id}/public_link")
    if r.status_code not in (200, 202):
        # might already exist — fetch it
        r2 = client.get(f"/api/dashboard/{dashboard_id}")
        uuid = r2.json().get("public_uuid", "")
    else:
        uuid = r.json().get("uuid", "")

    if uuid:
        link = f"{MB_URL}/public/dashboard/{uuid}"
        print(f"[share] Public link: {link}")
        return link
    print("[share] Could not obtain public link (sharing may be disabled).")
    return ""


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    client = MetabaseClient(MB_URL)
    wait_for_metabase(client)
    initial_setup(client)
    db_id = add_database(client)

    # ── SQL for the 5 KPI cards ──────────────────────────────────────────────
    kpi1_sql = "SELECT COUNT(*) AS \"Total Loans\" FROM staging.stg_loans"

    kpi2_sql = """
SELECT ROUND(AVG(is_default::numeric) * 100, 2) AS "Default Rate (%)"
FROM staging.stg_loans
""".strip()

    kpi3_sql = """
SELECT ROUND(AVG(interest_rate), 2) AS "Avg Interest Rate (%)"
FROM staging.stg_loans
""".strip()

    kpi4_sql = """
SELECT ROUND(SUM(total_interest_received), 0) AS "Total Interest Revenue ($)"
FROM staging.stg_loans
""".strip()

    top5_sql = """
SELECT state AS "State", loan_count AS "Loan Count"
FROM mart.mart_avg_int_rate_by_state
ORDER BY loan_count DESC
LIMIT 5
""".strip()

    print("[cards] Creating KPI cards …")
    card_ids = []
    card_ids.append(create_card(client, make_scalar("Total Loans",               kpi1_sql, db_id)))
    card_ids.append(create_card(client, make_scalar("Overall Default Rate (%)",  kpi2_sql, db_id)))
    card_ids.append(create_card(client, make_scalar("Avg Interest Rate (%)",     kpi3_sql, db_id)))
    card_ids.append(create_card(client, make_scalar("Total Interest Revenue ($)", kpi4_sql, db_id)))
    card_ids.append(create_card(client, make_bar(
        "Top 5 States by Loan Volume", top5_sql, db_id, "State", "Loan Count"
    )))

    dashboard_id = create_dashboard(client)
    add_cards_to_dashboard(client, dashboard_id, card_ids)
    public_link  = enable_public_link(client, dashboard_id)

    # Write the dashboard URL to a file for easy reference
    out = {
        "dashboard_id":   dashboard_id,
        "dashboard_url":  f"{MB_URL}/dashboard/{dashboard_id}",
        "public_link":    public_link,
    }
    import pathlib
    pathlib.Path("reports").mkdir(exist_ok=True)
    pathlib.Path("reports/metabase_links.json").write_text(
        json.dumps(out, indent=2), encoding="utf-8"
    )

    print()
    print("=" * 60)
    print("  Metabase dashboard ready!")
    print(f"  Admin UI   : {MB_URL}/dashboard/{dashboard_id}")
    if public_link:
        print(f"  Public link: {public_link}")
    print("  Credentials: admin@lendingclub.local / MetaAdmin1!")
    print("=" * 60)


if __name__ == "__main__":
    main()

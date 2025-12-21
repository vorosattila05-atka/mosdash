# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ================= PAGE =================
st.set_page_config(page_title="Mosly – Készlet", layout="wide")

# ================= SECRETS =================
def S(key):
    if key not in st.secrets or not str(st.secrets[key]).strip():
        st.error(f"Missing secret: {key}")
        st.stop()
    return st.secrets[key]

APP_PASSWORD = S("APP_PASSWORD")
SHOPIFY_STORE = S("SHOPIFY_STORE")
SHOPIFY_API_KEY = S("SHOPIFY_API_KEY")
SHOPIFY_API_PASSWORD = S("SHOPIFY_API_PASSWORD")
GOOGLE_SHEET_ID = S("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT = json.loads(S("GOOGLE_SERVICE_ACCOUNT"))

BASE_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@{SHOPIFY_STORE}/admin/api/2024-10"

# ================= AUTH =================
if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("🔒 Bejelentkezés")
    pw = st.text_input("Jelszó", type="password")
    if pw == APP_PASSWORD:
        st.session_state.auth = True
        st.rerun()
    st.stop()

# ================= GOOGLE SHEETS =================
@st.cache_resource
def gs_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT, scopes=scope)
    return gspread.authorize(creds)

gc = gs_client()
book = gc.open_by_key(GOOGLE_SHEET_ID)

ws_incoming = book.worksheet("stock_incoming")
ws_stock = book.worksheet("stock_current")
ws_orders = book.worksheet("orders_cache")
ws_snap = book.worksheet("stock_snapshots")

def df(ws):
    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])

# ================= HELPERS =================
def is_priority(title):
    t = (title or "").lower()
    return any(x in t for x in ["elsőbbségi", "priority", "express"])

def envelope_type(qty):
    if qty == 1: return "F16"
    if qty in (2, 3): return "H18"
    if qty == 4: return "I19"
    if qty in (5, 6): return "K20"
    return ""

# ================= SNAPSHOT =================
def latest_snapshot():
    snap = df(ws_snap)
    if snap.empty:
        return None, {}

    snap = snap.dropna(subset=["datetime"])
    latest_time = max(snap["datetime"])  # STRING

    base = {}
    for _, r in snap[snap["datetime"] == latest_time].iterrows():
        base[r["item_name"]] = int(float(r["quantity"]))

    return latest_time, base

# ================= SHOPIFY ORDERS =================
def shopify_orders_since(snapshot_iso):
    orders = []
    url = f"{BASE_URL}/orders.json"
    params = {
        "status": "any",
        "limit": 250,
        "order": "created_at asc"
    }
    if snapshot_iso:
        params["created_at_min"] = snapshot_iso

    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        orders.extend(r.json().get("orders", []))

        link = r.headers.get("Link")
        if not link or 'rel="next"' not in link:
            break

        url = link.split(";")[0].strip("<>")
        params = None

    return orders

# ================= ORDERS CACHE =================
def update_orders_cache():
    orders_df = df(ws_orders)
    existing = set(orders_df["order_id"]) if not orders_df.empty else set()

    snapshot_iso, _ = latest_snapshot()
    new_rows = []

    for o in shopify_orders_since(snapshot_iso):
        oid = str(o["id"])
        if oid in existing:
            continue

        items = [i for i in o["line_items"] if not is_priority(i["title"])]
        qty = sum(int(i["quantity"]) for i in items)
        env = envelope_type(qty)

        new_rows.append([
            oid,
            o["created_at"],
            qty,
            env
        ])

    if new_rows:
        ws_orders.append_rows(new_rows)

    return len(new_rows)

# ================= CALCULATE STOCK =================
def calculate_stock():
    snapshot_iso, base = latest_snapshot()
    result = dict(base)

    incoming = df(ws_incoming)
    if snapshot_iso and not incoming.empty:
        for _, r in incoming.iterrows():
            if r["datetime"] > snapshot_iso:
                result[r["item_name"]] = result.get(r["item_name"], 0) + int(float(r["quantity"]))

    orders = df(ws_orders)
    if snapshot_iso and not orders.empty:
        for _, r in orders.iterrows():
            if r["created_at"] > snapshot_iso:
                if int(r["mosolap_qty"]) > 0:
                    result["mosolap"] = result.get("mosolap", 0) - int(r["mosolap_qty"])
                if r["envelope"]:
                    result[r["envelope"]] = result.get(r["envelope"], 0) - 1

    out = pd.DataFrame(
        [{"item_name": k, "quantity": v} for k, v in result.items()]
    )

    ws_stock.update([out.columns.tolist()] + out.values.tolist())

# ================= UI =================
st.title("📦 Mosly – Aktuális készlet")

c1, c2 = st.columns(2)

with c1:
    if st.button("🔄 Shopify rendelések frissítése (snapshot óta)"):
        n = update_orders_cache()
        st.success(f"{n} új rendelés betöltve")

with c2:
    if st.button("📊 Készlet újraszámolása"):
        calculate_stock()
        st.success("Készlet frissítve")

st.markdown("---")

stock = df(ws_stock)
if not stock.empty:
    stock["quantity"] = pd.to_numeric(stock["quantity"], errors="coerce").fillna(0).astype(int)
    cols = st.columns(len(stock))
    for i, r in stock.iterrows():
        cols[i].metric(r["item_name"], r["quantity"])
    st.dataframe(stock, use_container_width=True)
else:
    st.info("Nincs készletadat.")

# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import urlparse, parse_qs

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
    if not data or len(data) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df.loc[:, ~df.columns.duplicated()]

# ================= HELPERS =================
def is_priority(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in ["elsőbbségi", "elsobsegi", "priority", "express"])

def envelope_type(qty: int) -> str:
    if qty == 1: return "F16"
    if qty in (2, 3): return "H18"
    if qty == 4: return "I19"
    if qty in (5, 6): return "K20"
    return ""

# ================= SNAPSHOT =================
def latest_snapshot_time():
    snap = df(ws_snap)
    if snap.empty:
        return None
    snap["datetime"] = pd.to_datetime(snap["datetime"], errors="coerce")
    snap = snap.dropna(subset=["datetime"])
    if snap.empty:
        return None
    return snap["datetime"].max()

# ================= SHOPIFY PAGINATION =================
def shopify_orders_since(snapshot_dt):
    orders = []
    params = {
        "status": "any",
        "limit": 250,
        "order": "created_at asc"
    }

    if snapshot_dt is not None:
        params["created_at_min"] = snapshot_dt.isoformat()

    url = f"{BASE_URL}/orders.json"

    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("orders", [])
        orders.extend(data)

        link = r.headers.get("Link")
        if not link or 'rel="next"' not in link:
            break

        # next page URL
        next_url = link.split(";")[0].strip("<>")
        url = next_url
        params = None  # next_url already has params

    return orders

# ================= ORDERS CACHE =================
def update_orders_cache():
    orders_df = df(ws_orders)
    existing_ids = set(orders_df["order_id"]) if not orders_df.empty else set()

    snap_dt = latest_snapshot_time()
    new_rows = []

    for o in shopify_orders_since(snap_dt):
        oid = str(o["id"])
        if oid in existing_ids:
            continue

        items = [i for i in o["line_items"] if not is_priority(i["title"])]
        qty = sum(int(i["quantity"]) for i in items)
        env = envelope_type(qty)

        new_rows.append([
            oid,
            o["created_at"],
            qty,
            env,
            datetime.utcnow().isoformat()
        ])

    if new_rows:
        ws_orders.append_rows(new_rows)

    return len(new_rows)

# ================= CALCULATE STOCK =================
def calculate_stock():
    snap = df(ws_snap)
    base = {}

    if not snap.empty:
        snap["datetime"] = pd.to_datetime(snap["datetime"], errors="coerce")
        latest_time = snap["datetime"].max()
        latest = snap[snap["datetime"] == latest_time]
        for _, r in latest.iterrows():
            base[r["item_name"]] = int(float(r["quantity"]))

    result = dict(base)

    incoming = df(ws_incoming)
    if not incoming.empty:
        incoming["datetime"] = pd.to_datetime(incoming["datetime"], errors="coerce")
        for _, r in incoming[incoming["datetime"] > latest_time].iterrows():
            result[r["item_name"]] = result.get(r["item_name"], 0) + int(float(r["quantity"]))

    orders = df(ws_orders)
    if not orders.empty:
        orders["created_at"] = pd.to_datetime(orders["created_at"], errors="coerce")
        for _, r in orders[orders["created_at"] > latest_time].iterrows():
            if int(r["mosolap_qty"]) > 0:
                result["mosolap"] = result.get("mosolap", 0) - int(r["mosolap_qty"])
            if r["envelope"]:
                result[r["envelope"]] = result.get(r["envelope"], 0) - 1

    out = pd.DataFrame(
        [{"item_name": k, "quantity": v} for k, v in result.items()]
    )

    ws_stock.update([out.columns.tolist()] + out.values.tolist())
    return out

# ================= UI =================
st.title("📦 Mosly – Aktuális készlet")

c1, c2 = st.columns(2)

with c1:
    if st.button("🔄 Shopify rendelések frissítése (snapshot óta)"):
        with st.spinner("Shopify → orders_cache"):
            n = update_orders_cache()
        st.success(f"{n} új rendelés eltárolva")

with c2:
    if st.button("📊 Készlet újraszámolása"):
        with st.spinner("Számolás..."):
            stock_df = calculate_stock()
        st.success("Készlet frissítve")

st.markdown("---")

stock = df(ws_stock)
if not stock.empty:
    stock["quantity"] = pd.to_numeric(stock["quantity"], errors="coerce").fillna(0).astype(int)
    cols = st.columns(len(stock))
    for i, r in stock.iterrows():
        cols[i].metric(str(r["item_name"]), int(r["quantity"]))
    st.dataframe(stock, use_container_width=True)
else:
    st.info("A készlet üres.")

st.markdown("---")

st.subheader("🧱 Készlet snapshot (helyreállítás)")
with st.form("snapshot"):
    sdt = st.datetime_input("Snapshot dátum és idő")
    sitem = st.text_input("Tétel")
    sqty = st.number_input("Mennyiség", min_value=0, step=1)
    note = st.text_input("Megjegyzés")
    if st.form_submit_button("Snapshot mentése"):
        ws_snap.append_row([sdt.isoformat(), sitem, sqty, note])
        st.success("Snapshot mentve")

st.markdown("---")

st.subheader("➕ Beérkezés rögzítése")
with st.form("incoming"):
    dt = st.datetime_input("Dátum és idő")
    item = st.text_input("Tétel")
    qty = st.number_input("Mennyiség", min_value=1, step=1)
    if st.form_submit_button("Mentés"):
        ws_incoming.append_row([dt.isoformat(), item, qty])
        st.success("Beérkezés mentve")

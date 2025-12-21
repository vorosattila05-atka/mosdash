# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import json
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

# ================= PAGE =================
st.set_page_config(page_title="Mosly – Készlet", layout="wide")

# ================= SECRETS =================
def S(key):
    if key not in st.secrets:
        st.error(f"Missing secret: {key}")
        st.stop()
    return st.secrets[key]

APP_PASSWORD = S("APP_PASSWORD")
SHOPIFY_STORE = S("SHOPIFY_STORE")
SHOPIFY_API_KEY = S("SHOPIFY_API_KEY")
SHOPIFY_API_PASSWORD = S("SHOPIFY_API_PASSWORD")
GOOGLE_SHEET_ID = S("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT = json.loads(S("GOOGLE_SERVICE_ACCOUNT"))

SHOPIFY_BASE = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@{SHOPIFY_STORE}/admin/api/2024-10"

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
def is_priority(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in ["elsőbbségi", "elsobsegi", "priority", "express"])

def envelope_type(qty: int) -> str:
    if qty == 1: return "F16"
    if qty in (2, 3): return "H18"
    if qty == 4: return "I19"
    if qty in (5, 6): return "K20"
    return ""

def shopify_orders():
    r = requests.get(
        f"{SHOPIFY_BASE}/orders.json?status=any&limit=250&order=created_at+asc",
        timeout=30
    )
    r.raise_for_status()
    return r.json()["orders"]

# ================= ORDERS CACHE =================
def update_orders_cache():
    orders_df = df(ws_orders)
    existing = set(orders_df["order_id"]) if not orders_df.empty else set()
    new_rows = []

    for o in shopify_orders():
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
            env,
            datetime.now(timezone.utc).isoformat()
        ])

    if new_rows:
        ws_orders.append_rows(new_rows)

    return len(new_rows)

# ================= SNAPSHOT =================
def latest_snapshot():
    snap = df(ws_snap)
    if snap.empty:
        return None, {}
    snap["datetime"] = pd.to_datetime(snap["datetime"])
    latest_time = snap["datetime"].max()
    latest = snap[snap["datetime"] == latest_time]
    base = {}
    for _, r in latest.iterrows():
        base[r["item_name"]] = int(r["quantity"])
    return latest_time, base

# ================= CALCULATE STOCK =================
def calculate_stock():
    snap_time, base = latest_snapshot()

    incoming = df(ws_incoming)
    orders = df(ws_orders)

    result = dict(base)

    if not incoming.empty and snap_time is not None:
        incoming["datetime"] = pd.to_datetime(incoming["datetime"])
        inc = incoming[incoming["datetime"] > snap_time]
        for _, r in inc.iterrows():
            result[r["item_name"]] = result.get(r["item_name"], 0) + int(r["quantity"])

    if not orders.empty and snap_time is not None:
        orders["created_at"] = pd.to_datetime(orders["created_at"])
        ords = orders[orders["created_at"] > snap_time]
        for _, r in ords.iterrows():
            if int(r["mosolap_qty"]) > 0:
                result["mosolap"] = result.get("mosolap", 0) - int(r["mosolap_qty"])
            if r["envelope"]:
                result[r["envelope"]] = result.get(r["envelope"], 0) - 1

    out = pd.DataFrame(
        [{"item_name": k, "quantity": v} for k, v in result.items()]
    )

    ws_stock.update([out.columns.tolist()] + out.values.tolist())
    return out, snap_time

# ================= UI =================
st.title("📦 Mosly – Aktuális készlet")

c1, c2 = st.columns(2)

with c1:
    if st.button("🔄 Shopify rendelések frissítése"):
        with st.spinner("Shopify → orders_cache"):
            n = update_orders_cache()
        st.success(f"{n} új rendelés eltárolva")

with c2:
    if st.button("📊 Készlet újraszámolása"):
        with st.spinner("Számolás..."):
            stock, snap_time = calculate_stock()
        st.success("Készlet frissítve")

st.markdown("---")

stock = df(ws_stock)
if not stock.empty:
    cols = st.columns(len(stock))
    for i, r in stock.iterrows():
        cols[i].metric(r["item_name"], int(r["quantity"]))
    st.dataframe(stock, use_container_width=True)

st.markdown("---")

st.subheader("➕ Beérkezés rögzítése")
with st.form("incoming"):
    dt = st.datetime_input("Dátum")
    item = st.text_input("Tétel (mosolap / F16 / H18 / I19 / K20)")
    qty = st.number_input("Mennyiség", min_value=1, step=1)
    if st.form_submit_button("Mentés"):
        ws_incoming.append_row([dt.isoformat(), item, qty])
        st.success("Beérkezés mentve")

st.markdown("---")

st.subheader("🧱 Készlet helyreállítás (Snapshot)")
with st.form("snapshot"):
    sdt = st.datetime_input("Snapshot dátum")
    sitem = st.text_input("Tétel")
    sqty = st.number_input("Mennyiség", min_value=0, step=1)
    note = st.text_input("Megjegyzés")
    if st.form_submit_button("Snapshot mentése"):
        ws_snap.append_row([sdt.isoformat(), sitem, sqty, note])
        st.success("Snapshot mentve – számold újra a készletet")

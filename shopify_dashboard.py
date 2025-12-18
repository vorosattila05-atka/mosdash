# -*- coding: utf-8 -*-
import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Mosly – Dashboard", layout="wide")

# ================== SECRETS ==================
def secret(k):
    if k not in st.secrets:
        st.error(f"Hiányzó Secret: {k}")
        st.stop()
    return st.secrets[k]

APP_PASSWORD = secret("APP_PASSWORD")
SHOPIFY_STORE = secret("SHOPIFY_STORE")
SHOPIFY_API_KEY = secret("SHOPIFY_API_KEY")
SHOPIFY_API_PASSWORD = secret("SHOPIFY_API_PASSWORD")
GOOGLE_SHEET_ID = secret("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT = json.loads(secret("GOOGLE_SERVICE_ACCOUNT"))

BASE_URL = f"https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@{SHOPIFY_STORE}/admin/api/2024-10"

# ================== AUTH ==================
if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("🔒 Bejelentkezés")
    pw = st.text_input("Jelszó", type="password")
    if pw == APP_PASSWORD:
        st.session_state.auth = True
        st.rerun()
    elif pw:
        st.error("Hibás jelszó")
    st.stop()

# ================== GOOGLE SHEET ==================
@st.cache_resource
def gs_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        GOOGLE_SERVICE_ACCOUNT, scopes=scope
    )
    return gspread.authorize(creds)

gc = gs_client()
sheet = gc.open_by_key(GOOGLE_SHEET_ID)
ws_stock = sheet.worksheet("stock_current")
ws_log = sheet.worksheet("stock_movements")

def load_stock():
    return pd.DataFrame(ws_stock.get_all_records())

def update_stock(item_name, delta, reason):
    df = load_stock()
    df.loc[df["item_name"] == item_name, "quantity"] += delta
    ws_stock.update([df.columns.values.tolist()] + df.values.tolist())
    ws_log.append_row([
        datetime.now().isoformat(),
        item_name,
        delta,
        reason
    ])

# ================== SHOPIFY ==================
@st.cache_data(ttl=120)
def get_orders(start, end):
    url = (
        f"{BASE_URL}/orders.json"
        f"?status=any&limit=250"
        f"&created_at_min={start}T00:00:00"
        f"&created_at_max={end}T23:59:59"
    )
    r = requests.get(url)
    r.raise_for_status()
    return r.json().get("orders", [])

def env_type(q):
    if q == 1: return "F16"
    if q in (2,3): return "H18"
    if q == 4: return "I19"
    if q in (5,6): return "K20"
    return "Nincs"

def is_priority(t):
    t = t.lower()
    return any(x in t for x in ["elsőbbségi","priority","express"])

# ================== UI ==================
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🔮 Előrejelzés", "📦 Készlet"])

# ---------- DASHBOARD ----------
with tab1:
    c1, c2 = st.columns(2)
    start = c1.date_input("Kezdő dátum")
    end = c2.date_input("Végdátum")

    if st.button("Rendelések lekérése"):
        orders = get_orders(str(start), str(end))
        rows = []
        for o in orders:
            items = [
                i for i in o["line_items"]
                if not is_priority(i["title"])
            ]
            qty = sum(i["quantity"] for i in items)
            rows.append({
                "Rendelés": o["name"],
                "Termékszám": qty,
                "Boríték": env_type(qty)
            })
        st.session_state.orders = rows

    if "orders" in st.session_state:
        st.dataframe(pd.DataFrame(st.session_state.orders))

# ---------- ELŐREJELZÉS ----------
with tab2:
    if "orders" not in st.session_state:
        st.info("Előbb kérd le a rendeléseket")
    else:
        df = pd.DataFrame(st.session_state.orders)
        avg = df["Termékszám"].mean()
        incoming = st.number_input("Beérkező mosólap db", 1)
        est = incoming / avg if avg else 0
        st.metric("Becsült rendelések", round(est))

        counts = df["Boríték"].value_counts()
        for env, c in counts.items():
            if env != "Nincs":
                st.write(env, round(c / len(df) * est))

# ---------- KÉSZLET ----------
with tab3:
    st.subheader("📦 Aktuális készlet")
    stock = load_stock()
    st.dataframe(stock, use_container_width=True)

    st.subheader("➕ / ➖ Készlet módosítás")
    item = st.selectbox("Tétel", stock["item_name"])
    delta = st.number_input("Változás (+ / −)", value=0)
    reason = st.text_input("Megjegyzés")

    if st.button("Mentés"):
        update_stock(item, delta, reason)
        st.success("Készlet frissítve")
        st.rerun()

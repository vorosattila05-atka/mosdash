# -*- coding: utf-8 -*-
import streamlit as st
import requests
import pandas as pd
import json
import math
import time
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

try:
    from dateutil.parser import isoparse
except Exception:
    isoparse = None

# ================= PAGE =================
st.set_page_config(page_title="MOSDASH", layout="wide")

# ================= SECRETS =================
def S(key: str) -> str:
    if key not in st.secrets or not str(st.secrets[key]).strip():
        st.error(f"Missing secret: {key}")
        st.stop()
    return str(st.secrets[key])

APP_PASSWORD = S("APP_PASSWORD")

# Shopify: use Admin API access token
# Secrets expected:
# SHOPIFY_STORE = "yourstore.myshopify.com"
# SHOPIFY_ADMIN_TOKEN = "shpat_..."
SHOPIFY_STORE = S("SHOPIFY_STORE")
SHOPIFY_ADMIN_TOKEN = S("SHOPIFY_ADMIN_TOKEN")

GOOGLE_SHEET_ID = S("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT = json.loads(S("GOOGLE_SERVICE_ACCOUNT"))

SHOPIFY_API_VERSION = "2024-10"
SHOPIFY_BASE = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}"

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

# Worksheets required:
# - orders_cache
# - stock_current
# - stock_snapshots
# Optional:
# - stock_incoming
ws_orders = book.worksheet("orders_cache")
ws_stock = book.worksheet("stock_current")
ws_snap = book.worksheet("stock_snapshots")
try:
    ws_incoming = book.worksheet("stock_incoming")
except Exception:
    ws_incoming = None

def ws_get_df(ws: gspread.Worksheet) -> pd.DataFrame:
    data = ws.get_all_values()
    if not data or len(data) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def ensure_headers(ws: gspread.Worksheet, headers: list[str]):
    vals = ws.get_all_values()
    if not vals:
        ws.update([headers])
        return
    if vals[0] != headers:
        ws.update([headers])

# ================= HEADERS =================
SNAP_HEADERS = ["datetime", "ts", "item_name", "quantity", "note"]
ORDERS_HEADERS = ["order_id", "created_at", "created_at_ts", "mosolap_qty", "items_qty", "envelope"]
STOCK_HEADERS = ["item_name", "quantity"]
INCOMING_HEADERS = ["datetime", "ts", "item_name", "quantity", "note"]

ensure_headers(ws_snap, SNAP_HEADERS)
ensure_headers(ws_orders, ORDERS_HEADERS)
ensure_headers(ws_stock, STOCK_HEADERS)
if ws_incoming is not None:
    ensure_headers(ws_incoming, INCOMING_HEADERS)

# ================= TIME HELPERS =================
def to_ts(value) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # "YYYY-MM-DD HH:MM:SS" -> "YYYY-MM-DDTHH:MM:SS"
    if " " in s and "T" not in s:
        s = s.replace(" ", "T")

    try:
        if isoparse:
            dt = isoparse(s)
        else:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.astimezone(timezone.utc).timestamp())

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

# ================= BUSINESS RULES =================
def is_priority(title: str) -> bool:
    t = (title or "").lower()
    return any(k in t for k in ["elsőbbségi", "elsobsegi", "priority", "express"])

def envelope_from_qty(qty: int) -> str | None:
    if qty == 1: return "F16"
    if qty in (2, 3): return "H18"
    if qty == 4: return "I19"
    if qty in (5, 6): return "K20"
    return None

def round_up_business(x: int) -> int:
    if x <= 0:
        return 0
    if x <= 100:
        return 100
    if x <= 150:
        return 150
    if x <= 200:
        return 200
    return int(math.ceil(x / 100.0) * 100)

# ================= SNAPSHOT =================
def latest_snapshot_ts_and_base() -> tuple[int | None, dict]:
    snap = ws_get_df(ws_snap)
    if snap.empty:
        return None, {}

    if "ts" not in snap.columns:
        snap["ts"] = ""

    ts_int = pd.to_numeric(snap["ts"], errors="coerce").fillna(0).astype(int)

    # backfill parse where ts is missing
    for i in range(len(snap)):
        if int(ts_int.iloc[i]) == 0:
            t = to_ts(snap.iloc[i].get("datetime"))
            if t:
                ts_int.iloc[i] = t

    snap = snap.assign(ts_int=ts_int)
    snap = snap[snap["ts_int"] > 0]
    if snap.empty:
        return None, {}

    latest_ts = int(snap["ts_int"].max())
    latest_rows = snap[snap["ts_int"] == latest_ts]

    base = {}
    for _, r in latest_rows.iterrows():
        name = str(r.get("item_name", "")).strip()
        if not name:
            continue
        try:
            qty = int(float(r.get("quantity", 0)))
        except Exception:
            qty = 0
        base[name] = qty

    return latest_ts, base

# ================= SHOPIFY =================
def shopify_get(url: str, params: dict | None = None) -> requests.Response:
    headers = {
        "Accept": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
    }
    return requests.get(url, params=params, headers=headers, timeout=40)

def shopify_orders_since(snapshot_ts: int | None) -> list[dict]:
    url = f"{SHOPIFY_BASE}/orders.json"
    params = {
        "status": "any",
        "limit": 250,
        "order": "created_at asc",
    }
    if snapshot_ts:
        params["created_at_min"] = datetime.fromtimestamp(snapshot_ts, tz=timezone.utc).isoformat()

    out = []
    while True:
        r = shopify_get(url, params=params)

        if r.status_code != 200:
            st.error(f"Shopify API hiba: {r.status_code}")
            st.code(r.text[:5000])
            return []

        payload = r.json()
        out.extend(payload.get("orders", []))

        link = r.headers.get("Link", "")
        if not link or 'rel="next"' not in link:
            break

        next_url = link.split(";")[0].strip().strip("<>").strip()
        url = next_url
        params = None

        time.sleep(0.35)  # rate limit friendly

    return out

def compute_items_qty(order: dict) -> int:
    qty = 0
    for li in order.get("line_items", []) or []:
        if is_priority(li.get("title", "")):
            continue
        try:
            qty += int(li.get("quantity", 0))
        except Exception:
            pass
    return int(qty)

def update_orders_cache_from_shopify() -> int:
    ensure_headers(ws_orders, ORDERS_HEADERS)

    existing = ws_get_df(ws_orders)
    existing_ids = set()
    if not existing.empty and "order_id" in existing.columns:
        existing_ids = set(existing["order_id"].astype(str).tolist())

    snap_ts, _ = latest_snapshot_ts_and_base()
    orders = shopify_orders_since(snap_ts)

    new_rows = []
    for o in orders:
        oid = str(o.get("id", "")).strip()
        if not oid or oid in existing_ids:
            continue

        created_at = str(o.get("created_at", "")).strip()
        created_ts = to_ts(created_at)
        if not created_ts:
            continue

        items_qty = compute_items_qty(o)
        mosolap_qty = items_qty  # jelenleg ugyanaz: nem-firstbbségi termékek összege
        env = envelope_from_qty(items_qty) or ""

        new_rows.append([
            oid,
            created_at,
            str(created_ts),
            str(mosolap_qty),
            str(items_qty),
            env
        ])

    if new_rows:
        ws_orders.append_rows(new_rows)

    return len(new_rows)

# ================= STOCK CALC =================
def recompute_current_stock(write_to_sheet: bool = True) -> pd.DataFrame:
    snap_ts, base = latest_snapshot_ts_and_base()
    if not snap_ts:
        df_out = pd.DataFrame([{"item_name": k, "quantity": int(v)} for k, v in base.items()])
        if write_to_sheet:
            ws_stock.update([STOCK_HEADERS] + df_out[["item_name", "quantity"]].values.tolist() if not df_out.empty else [STOCK_HEADERS])
        return df_out

    result = dict(base)

    # Incoming
    if ws_incoming is not None:
        inc = ws_get_df(ws_incoming)
        if not inc.empty:
            ts_int = pd.to_numeric(inc.get("ts", ""), errors="coerce").fillna(0).astype(int)
            for i in range(len(inc)):
                if int(ts_int.iloc[i]) == 0:
                    t = to_ts(inc.iloc[i].get("datetime"))
                    if t:
                        ts_int.iloc[i] = t
            inc = inc.assign(ts_int=ts_int)
            inc = inc[inc["ts_int"] > snap_ts]

            for _, r in inc.iterrows():
                name = str(r.get("item_name", "")).strip()
                if not name:
                    continue
                try:
                    q = int(float(r.get("quantity", 0)))
                except Exception:
                    q = 0
                result[name] = result.get(name, 0) + q

    # Orders consumption
    orders = ws_get_df(ws_orders)
    if not orders.empty:
        created_ts = pd.to_numeric(orders.get("created_at_ts", ""), errors="coerce").fillna(0).astype(int)
        orders = orders.assign(created_ts=created_ts)
        orders = orders[orders["created_ts"] > snap_ts]

        mos = pd.to_numeric(orders.get("mosolap_qty", 0), errors="coerce").fillna(0).astype(int).sum()
        result["mosolap"] = result.get("mosolap", 0) - int(mos)

        env_counts = orders.get("envelope", "").fillna("").astype(str).value_counts().to_dict()
        for env, cnt in env_counts.items():
            env = str(env).strip()
            if not env:
                continue
            result[env] = result.get(env, 0) - int(cnt)

    df_out = pd.DataFrame([{"item_name": k, "quantity": int(v)} for k, v in result.items()])
    if not df_out.empty:
        df_out = df_out.sort_values("item_name").reset_index(drop=True)

    if write_to_sheet:
        ws_stock.update([STOCK_HEADERS] + df_out[["item_name", "quantity"]].values.tolist() if not df_out.empty else [STOCK_HEADERS])

    return df_out

# ================= UI =================
st.title("MOSDASH – Készlet + Analitika")

tab_stock, tab_analytics, tab_admin = st.tabs(["📦 Készlet", "📊 Analitika", "⚙️ Admin / Sync"])

# ----------------- ADMIN / SYNC -----------------
with tab_admin:
    st.subheader("Shopify → orders_cache (snapshot óta)")

    colA, colB = st.columns(2)
    with colA:
        if st.button("🔄 Rendelések frissítése Shopifyból", use_container_width=True):
            with st.spinner("Shopify lekérés..."):
                n = update_orders_cache_from_shopify()
            st.success(f"{n} új rendelés bekerült az orders_cache-be.")

    with colB:
        if st.button("📊 Készlet újraszámolása és mentése", use_container_width=True):
            with st.spinner("Számolás..."):
                df_new = recompute_current_stock(write_to_sheet=True)
            st.success("Készlet frissítve (stock_current).")
            st.dataframe(df_new, use_container_width=True)

    st.markdown("---")
    st.subheader("🧱 Snapshot rögzítés (készlet helyreállítás)")

    snap_ts, _ = latest_snapshot_ts_and_base()
    if snap_ts:
        st.caption(f"Legutóbbi snapshot: {datetime.fromtimestamp(snap_ts, tz=timezone.utc).isoformat()} (ts={snap_ts})")
    else:
        st.warning("Nincs snapshot. Először rögzíts egyet, különben a készletszámítás nem indul el.")

    with st.form("snapshot_form", clear_on_submit=False):
        dt = st.text_input("Snapshot datetime (ISO)", value=now_iso_utc())
        item = st.selectbox("Tétel", ["mosolap", "F16", "H18", "I19", "K20"])
        qty = st.number_input("Mennyiség", min_value=0, step=1, value=0)
        note = st.text_input("Megjegyzés", value="")
        if st.form_submit_button("✅ Snapshot mentése"):
            t = to_ts(dt)
            if not t:
                st.error("Nem sikerült értelmezni a dátumot. Pl: 2025-12-12T15:45:00+00:00")
            else:
                ws_snap.append_row([dt, str(t), item, str(int(qty)), note])
                st.success("Snapshot mentve.")

    if ws_incoming is not None:
        st.markdown("---")
        st.subheader("➕ Beérkezés rögzítése (opcionális)")

        with st.form("incoming_form", clear_on_submit=False):
            dt2 = st.text_input("Beérkezés datetime (ISO)", value=now_iso_utc())
            item2 = st.selectbox("Beérkező tétel", ["mosolap", "F16", "H18", "I19", "K20"], key="inc_item")
            qty2 = st.number_input("Beérkezett mennyiség", min_value=1, step=1, value=100, key="inc_qty")
            note2 = st.text_input("Megjegyzés", value="", key="inc_note")
            if st.form_submit_button("✅ Beérkezés mentése"):
                t2 = to_ts(dt2)
                if not t2:
                    st.error("Nem sikerült értelmezni a dátumot (ISO kell).")
                else:
                    ws_incoming.append_row([dt2, str(t2), item2, str(int(qty2)), note2])
                    st.success("Beérkezés mentve.")

# ----------------- STOCK TAB -----------------
with tab_stock:
    st.subheader("Aktuális készlet (stock_current)")

    stock = ws_get_df(ws_stock)
    if stock.empty:
        st.info("stock_current üres. Admin fülön nyomd meg: 'Készlet újraszámolása és mentése'.")
    else:
        stock["quantity"] = pd.to_numeric(stock.get("quantity", 0), errors="coerce").fillna(0).astype(int)
        # Metrics
        key_items = ["mosolap", "F16", "H18", "I19", "K20"]
        show = stock[stock["item_name"].isin(key_items)].copy()
        if show.empty:
            show = stock.copy()

        cols = st.columns(min(len(show), 5) if len(show) else 1)
        for i, r in enumerate(show.itertuples(index=False)):
            if i >= len(cols):
                break
            cols[i].metric(str(r.item_name), int(r.quantity))

        st.dataframe(stock.sort_values("item_name"), use_container_width=True)

    st.markdown("---")
    if st.button("🧮 Újraszámolás (csak sheet adatokból)", use_container_width=True):
        with st.spinner("Számolás..."):
            df_new = recompute_current_stock(write_to_sheet=True)
        st.success("Készlet frissítve.")
        st.dataframe(df_new, use_container_width=True)

# ----------------- ANALYTICS TAB -----------------
with tab_analytics:
    st.subheader("Analitika (orders_cache alapján)")

    orders = ws_get_df(ws_orders)
    if orders.empty:
        st.warning("orders_cache üres. Admin fülön frissítsd Shopifyból.")
        st.stop()

    orders["created_at_ts_int"] = pd.to_numeric(orders.get("created_at_ts", 0), errors="coerce").fillna(0).astype(int)
    orders["items_qty_int"] = pd.to_numeric(orders.get("items_qty", 0), errors="coerce").fillna(0).astype(int)
    orders["mosolap_qty_int"] = pd.to_numeric(orders.get("mosolap_qty", 0), errors="coerce").fillna(0).astype(int)
    orders["envelope"] = orders.get("envelope", "").fillna("").astype(str)

    col1, col2 = st.columns([1, 1])
    with col1:
        start_iso = st.text_input("Statisztika kezdő dátum (ISO)", value="2025-12-01T00:00:00+00:00")
    with col2:
        incoming_mosolap = st.number_input("Beérkező mosólap (db)", min_value=0, step=100, value=1000)

    start_ts = to_ts(start_iso)
    if not start_ts:
        st.error("Kezdő dátum hibás. Pl: 2025-12-01T00:00:00+00:00")
        st.stop()

    filt = orders[orders["created_at_ts_int"] >= start_ts].copy()
    st.caption(f"Szűrt rendelések: {len(filt)} db")

    st.markdown("### Boríték fogyás (csökkenő sorrend)")
    env_counts = (
        filt[filt["envelope"].str.strip() != ""]
        .groupby("envelope")
        .size()
        .reset_index(name="db")
        .sort_values("db", ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(env_counts, use_container_width=True)

    st.markdown("### Boríték arányok (%)")
    if not env_counts.empty:
        total_env = int(env_counts["db"].sum())
        env_ratio = env_counts.copy()
        env_ratio["%"] = (env_ratio["db"] / total_env * 100).round(1)
        st.dataframe(env_ratio, use_container_width=True)
    else:
        st.info("Nincs boríték adat ebben az időszakban.")

    st.markdown("---")
    st.subheader("Előrejelzés + rendelési javaslat (felfelé kerekítve)")

    # avg items per order
    avg_items = float(filt["items_qty_int"].replace(0, pd.NA).dropna().mean()) if len(filt) else 0.0
    if not avg_items or math.isnan(avg_items):
        avg_items = 1.0

    expected_orders = incoming_mosolap / avg_items if avg_items > 0 else 0.0
    st.write(f"Átlagos rendelési darabszám (items_qty): **{avg_items:.2f}**")
    st.write(f"Várható rendelésszám: **{expected_orders:.0f}**")

    # current stock map
    stock = ws_get_df(ws_stock)
    stock_map = {}
    if not stock.empty:
        stock["quantity"] = pd.to_numeric(stock.get("quantity", 0), errors="coerce").fillna(0).astype(int)
        stock_map = dict(zip(stock["item_name"].astype(str), stock["quantity"].astype(int)))

    if env_counts.empty or expected_orders <= 0:
        st.info("Nincs elég adat forecasthoz.")
    else:
        total_env = int(env_counts["db"].sum())
        rows = []
        for _, r in env_counts.iterrows():
            env = str(r["envelope"]).strip()
            cnt = int(r["db"])
            ratio = cnt / total_env if total_env > 0 else 0.0

            need_raw = expected_orders * ratio
            need_int = int(math.ceil(need_raw))
            need_round = round_up_business(need_int)

            in_stock = int(stock_map.get(env, 0))
            to_order = max(0, need_round - in_stock)

            rows.append({
                "Boríték": env,
                "Arány %": round(ratio * 100, 1),
                "Várható igény (db)": need_int,
                "Ajánlott készlet (kerekítve)": need_round,
                "Készleten": in_stock,
                "Rendelendő": to_order
            })

        forecast_df = pd.DataFrame(rows).sort_values("Rendelendő", ascending=False).reset_index(drop=True)
        st.dataframe(forecast_df, use_container_width=True)

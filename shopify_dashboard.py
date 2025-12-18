# -*- coding: utf-8 -*-
import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime, timezone
import gspread
from google.oauth2.service_account import Credentials

# ================== PAGE ==================
st.set_page_config(page_title="Mosly – Készlet & Boríték Dashboard", layout="wide")

# ================== SECRETS ==================
def secret(key: str) -> str:
    if key not in st.secrets or not str(st.secrets[key]).strip():
        st.error(f"Hiányzó Secret: {key}")
        st.stop()
    return str(st.secrets[key]).strip()

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
    if pw:
        if pw == APP_PASSWORD:
            st.session_state.auth = True
            st.rerun()
        else:
            st.error("Hibás jelszó")
    st.stop()

# ================== GOOGLE SHEETS ==================
@st.cache_resource
def gs_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT, scopes=scope)
    return gspread.authorize(creds)

gc = gs_client()
gbook = gc.open_by_key(GOOGLE_SHEET_ID)

ws_stock = gbook.worksheet("stock_current")
ws_log = gbook.worksheet("stock_movements")
ws_settings = gbook.worksheet("settings")

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _df_from_ws(ws) -> pd.DataFrame:
    return pd.DataFrame(ws.get_all_records())

def load_stock_df() -> pd.DataFrame:
    df = _df_from_ws(ws_stock)
    if df.empty:
        st.error("A stock_current üres.")
        st.stop()
    for col in ["item_type", "item_name", "quantity"]:
        if col not in df.columns:
            st.error(f"A stock_current hiányos, nincs '{col}' oszlop.")
            st.stop()
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    df["item_type"] = df["item_type"].astype(str)
    df["item_name"] = df["item_name"].astype(str)
    return df

def write_stock_df(df: pd.DataFrame) -> None:
    ws_stock.update([df.columns.values.tolist()] + df.values.tolist())

def load_log_df() -> pd.DataFrame:
    df = _df_from_ws(ws_log)
    if df.empty:
        return pd.DataFrame(columns=["timestamp","item_name","change","reason","source","source_id"])
    for col in ["timestamp","item_name","change","reason","source","source_id"]:
        if col not in df.columns:
            df[col] = ""
    df["change"] = pd.to_numeric(df["change"], errors="coerce").fillna(0).astype(int)
    df["source_id"] = df["source_id"].astype(str)
    df["source"] = df["source"].astype(str)
    return df

def append_log(timestamp: str, item_name: str, change: int, reason: str, source: str, source_id: str):
    ws_log.append_row([timestamp, item_name, int(change), reason, source, str(source_id or "")])

def get_setting(key: str) -> str:
    df = _df_from_ws(ws_settings)
    if df.empty or "key" not in df.columns or "value" not in df.columns:
        st.error("A settings tabnak 'key' és 'value' oszlop kell.")
        st.stop()
    row = df.loc[df["key"] == key]
    if row.empty:
        return ""
    return str(row.iloc[0]["value"]).strip()

def set_setting(key: str, value: str) -> None:
    df = _df_from_ws(ws_settings)
    if df.empty:
        df = pd.DataFrame([{"key": key, "value": value}])
        ws_settings.update([df.columns.values.tolist()] + df.values.tolist())
        return

    if "key" not in df.columns or "value" not in df.columns:
        st.error("A settings tabnak 'key' és 'value' oszlop kell.")
        st.stop()

    if (df["key"] == key).any():
        df.loc[df["key"] == key, "value"] = value
    else:
        df = pd.concat([df, pd.DataFrame([{"key": key, "value": value}])], ignore_index=True)

    ws_settings.update([df.columns.values.tolist()] + df.values.tolist())

def parse_iso_dt(s: str) -> datetime:
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty datetime")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# ================== SHOPIFY HELPERS ==================
def is_priority_item(title: str) -> bool:
    t = (title or "").lower()
    keywords = ["elsőbbségi", "elsobsegi", "priority", "express", "gyorsított", "gyorsitott"]
    return any(k in t for k in keywords)

def envelope_type(qty: int) -> str:
    if qty == 1:
        return "F16"
    if qty in (2, 3):
        return "H18"
    if qty == 4:
        return "I19"
    if qty in (5, 6):
        return "K20"
    return "Nincs"

def shopify_get_orders_since(baseline_dt: datetime, limit: int = 250):
    created_min = baseline_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    url = f"{BASE_URL}/orders.json"
    params = {
        "status": "any",
        "limit": limit,
        "order": "created_at asc",
        "created_at_min": created_min,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("orders", [])

@st.cache_data(ttl=120)
def cached_orders_since(baseline_iso: str):
    dt = parse_iso_dt(baseline_iso)
    return shopify_get_orders_since(dt)

# ================== SESSION STATE ==================
if "orders_rows" not in st.session_state:
    st.session_state.orders_rows = []

if "stock_tab_opened_once" not in st.session_state:
    st.session_state.stock_tab_opened_once = False

# ================== CORE: APPLY SHOPIFY DELTAS ==================
def apply_shopify_deductions(baseline_iso: str):
    baseline_dt = parse_iso_dt(baseline_iso)

    stock_df = load_stock_df()
    log_df = load_log_df()

    processed_ids = set(
        log_df.loc[log_df["source"].str.lower() == "shopify", "source_id"].astype(str).tolist()
    )

    orders = cached_orders_since(baseline_iso)

    new_orders = []
    for o in orders:
        oid = str(o.get("id") or "")
        if not oid or oid in processed_ids:
            continue

        created_at = o.get("created_at")
        if not created_at:
            continue
        try:
            odt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if odt <= baseline_dt:
            continue

        new_orders.append(o)

    if not new_orders:
        return 0, 0, {"F16": 0, "H18": 0, "I19": 0, "K20": 0}

    env_counts = {"F16": 0, "H18": 0, "I19": 0, "K20": 0}
    mosolap_used = 0
    ts = now_iso()

    def ensure_item(item_name: str, item_type: str):
        nonlocal stock_df
        if not (stock_df["item_name"] == item_name).any():
            stock_df = pd.concat([stock_df, pd.DataFrame([{
                "item_type": item_type,
                "item_name": item_name,
                "quantity": 0
            }])], ignore_index=True)

    def add_to_stock(item_name: str, delta: int, item_type: str):
        nonlocal stock_df
        ensure_item(item_name, item_type)
        i = stock_df.index[stock_df["item_name"] == item_name][0]
        stock_df.at[i, "quantity"] = int(stock_df.at[i, "quantity"]) + int(delta)

    for o in new_orders:
        oid = str(o.get("id") or "")
        name = o.get("name", "")
        items = o.get("line_items", []) or []
        filtered = [i for i in items if not is_priority_item(i.get("title", ""))]

        qty = sum(int(i.get("quantity", 0)) for i in filtered)

        if qty > 0:
            add_to_stock("mosolap", -qty, "mosolap")
            mosolap_used += qty
            append_log(ts, "mosolap", -qty, f"Auto Shopify ({name})", "shopify", oid)

        env = envelope_type(qty)
        if env in env_counts:
            add_to_stock(env, -1, "envelope")
            env_counts[env] += 1
            append_log(ts, env, -1, f"Auto Shopify ({name})", "shopify", oid)

    stock_df["quantity"] = pd.to_numeric(stock_df["quantity"], errors="coerce").fillna(0).astype(int)
    write_stock_df(stock_df)

    return len(new_orders), mosolap_used, env_counts

# ================== UI ==================
st.title("📦 Mosly – rendelés, boríték és készlet")

tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🔮 Előrejelzés", "📦 Készlet"])

# ---------- DASHBOARD ----------
with tab1:
    st.caption("Elsőbbségi / priority szállítási tétel nem számít bele a termékszámba.")
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        start = st.date_input("Kezdő dátum")
    with c2:
        end = st.date_input("Végdátum")
    with c3:
        fetch = st.button("🔄 Utolsó 250 rendelés lekérése", use_container_width=True)

    if fetch:
        url = (
            f"{BASE_URL}/orders.json"
            f"?status=any&limit=250"
            f"&created_at_min={str(start)}T00:00:00"
            f"&created_at_max={str(end)}T23:59:59"
            f"&order=created_at+desc"
        )
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            orders = r.json().get("orders", [])
        except Exception as e:
            st.error(f"Shopify API hiba: {e}")
            orders = []

        rows = []
        for o in orders:
            items = o.get("line_items", []) or []
            filtered = [i for i in items if not is_priority_item(i.get("title", ""))]
            qty = sum(int(i.get("quantity", 0)) for i in filtered)
            rows.append({
                "Rendelés": o.get("name"),
                "Shopify Order ID": o.get("id"),
                "Termékszám": qty,
                "Boríték": envelope_type(qty),
                "Created": o.get("created_at", "")
            })
        st.session_state.orders_rows = rows

    if st.session_state.orders_rows:
        st.subheader("📋 Rendelések")
        st.dataframe(pd.DataFrame(st.session_state.orders_rows), use_container_width=True)
    else:
        st.info("Kérj le rendeléseket a fenti dátumszűrővel.")

# ---------- FORECAST ----------
with tab2:
    if not st.session_state.orders_rows:
        st.info("Előrejelzéshez előbb kérj le rendeléseket a Dashboard fülön.")
    else:
        df = pd.DataFrame(st.session_state.orders_rows)
        total_orders = len(df)
        avg_qty = df["Termékszám"].mean() if total_orders else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("Összes rendelés", total_orders)
        c2.metric("Átlagos termékszám", f"{avg_qty:.2f}")
        c3.metric("Boríték kategóriák", df["Boríték"].nunique())

        st.markdown("---")
        incoming = st.number_input("Beérkező mosólap darabszám", min_value=1, step=1)

        if avg_qty > 0:
            est_orders = incoming / avg_qty
            st.write(f"**Becsült rendelések száma:** {est_orders:.0f} db")

            counts = df["Boríték"].value_counts()
            st.write("**Várható borítékigény (arányosan):**")
            for env, c in counts.items():
                if env in ["F16", "H18", "I19", "K20"]:
                    need = round((c / total_orders) * est_orders)
                    st.write(f"- {env}: **{need} db**")
        else:
            st.warning("Nem tudok átlagot számolni (0 rendelés / 0 termékszám).")

# ---------- STOCK ----------
with tab3:
    st.subheader("⏱️ Baseline (utolsó „igaz készlet” időpont)")

    current_baseline = get_setting("baseline_datetime")
    if not current_baseline:
        current_baseline = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        set_setting("baseline_datetime", current_baseline)

    try:
        baseline_dt = parse_iso_dt(current_baseline)
    except Exception:
        baseline_dt = datetime.now(timezone.utc).replace(microsecond=0)

    b1, b2, b3 = st.columns([2, 2, 2])
    with b1:
        new_base_date = st.date_input("Baseline dátum", value=baseline_dt.date(), key="base_date")
    with b2:
        new_base_time = st.time_input("Baseline idő", value=baseline_dt.time().replace(microsecond=0), key="base_time")
    with b3:
        save_baseline = st.button("💾 Baseline mentése + azonnali szinkron", use_container_width=True)

    sync_col1, sync_col2 = st.columns([2, 2])
    with sync_col1:
        manual_sync = st.button("🔄 Shopify → készlet szinkron (baseline óta)", use_container_width=True)
    with sync_col2:
        st.caption("Trigger: Készlet fül megnyitása (első betöltés) vagy gombok.")

    do_sync = False
    if save_baseline:
        combined = datetime.combine(new_base_date, new_base_time).replace(tzinfo=timezone.utc).replace(microsecond=0)
        new_iso = combined.isoformat()
        set_setting("baseline_datetime", new_iso)
        st.cache_data.clear()
        do_sync = True

    if not st.session_state.stock_tab_opened_once:
        st.session_state.stock_tab_opened_once = True
        do_sync = True

    if manual_sync:
        do_sync = True

    if do_sync:
        baseline_now = get_setting("baseline_datetime")
        try:
            with st.spinner("Shopify rendelések feldolgozása baseline óta..."):
                processed_orders, mosolap_used, env_counts = apply_shopify_deductions(baseline_now)
            env_msg = ", ".join([f"{k}:{v}" for k, v in env_counts.items() if v > 0]) or "nincs"
            st.success(
                f"✅ Új rendelések: {processed_orders} | "
                f"🧺 Mosólap levonás: {mosolap_used} | "
                f"✉️ Boríték: {env_msg}"
            )
        except Exception as e:
            st.error(f"Szinkron hiba: {e}")

    st.markdown("---")

    st.subheader("📦 Aktuális készlet (Google Sheet)")
    stock_df = load_stock_df()

    def get_qty(name: str) -> int:
        if (stock_df["item_name"] == name).any():
            return int(stock_df.loc[stock_df["item_name"] == name, "quantity"].values[0])
        return 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Mosólap", get_qty("mosolap"))
    m2.metric("F16", get_qty("F16"))
    m3.metric("H18", get_qty("H18"))
    m4.metric("I19", get_qty("I19"))
    m5.metric("K20", get_qty("K20"))

    st.dataframe(stock_df, use_container_width=True)

    st.markdown("---")

    st.subheader("🛠️ Manuális készletmódosítás (és baseline beállítás)")
    left, mid, right = st.columns([2, 1, 2])

    with left:
        item = st.selectbox("Tétel", stock_df["item_name"].tolist(), key="manual_item")
    with mid:
        amount = st.number_input("Mennyiség", min_value=1, step=1, key="manual_amount")
    with right:
        reason = st.text_input("Megjegyzés (kötelező)", placeholder="pl. Beérkezés, leltár, selejt", key="manual_reason")

    d1, d2 = st.columns([1, 1])
    with d1:
        change_date = st.date_input("Változtatás dátuma", value=datetime.now().date(), key="change_date")
    with d2:
        change_time = st.time_input("Változtatás ideje", value=datetime.now().time().replace(microsecond=0), key="change_time")

    def apply_manual_delta(delta: int):
        if not reason.strip():
            st.error("Megjegyzés kötelező!")
            return

        # mindig frissen töltsük be a készletet
        df = load_stock_df()

        if not (df["item_name"] == item).any():
            st.error("Ismeretlen tétel a stock_current-ben.")
            return

        i = df.index[df["item_name"] == item][0]
        df.at[i, "quantity"] = int(df.at[i, "quantity"]) + int(delta)
        write_stock_df(df)

        ts = datetime.combine(change_date, change_time).replace(tzinfo=timezone.utc).replace(microsecond=0).isoformat()
        append_log(ts, item, delta, reason, "manual", "")

        # baseline = a manuális változtatás ideje
        set_setting("baseline_datetime", ts)
        st.cache_data.clear()

        # azonnali sync
        with st.spinner("Baseline mentve, Shopify szinkron fut..."):
            processed_orders, mosolap_used, env_counts = apply_shopify_deductions(ts)

        env_msg = ", ".join([f"{k}:{v}" for k, v in env_counts.items() if v > 0]) or "nincs"
        st.success(
            f"Mentve ({item} {delta:+d}). Baseline: {ts}. "
            f"Új rendelések: {processed_orders}. Mosólap levonás: {mosolap_used}. Boríték: {env_msg}"
        )
        st.rerun()

    a1, a2 = st.columns(2)
    with a1:
        if st.button("➕ Feltöltés", use_container_width=True):
            apply_manual_delta(int(amount))

    with a2:
        if st.button("➖ Levonás", use_container_width=True):
            current_qty = int(stock_df.loc[stock_df["item_name"] == item, "quantity"].values[0])
            if amount > current_qty:
                st.error("Nincs ennyi készleten!")
            else:
                apply_manual_delta(-int(amount))

    st.markdown("---")

    st.subheader("🧾 Készletmozgások (log) – utolsó 200 sor")
    log_df = load_log_df()
    if log_df.empty:
        st.info("Még nincs log bejegyzés.")
    else:
        st.dataframe(log_df.tail(200), use_container_width=True)

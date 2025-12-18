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
    return df

def write_stock_df(df: pd.DataFrame) -> None:
    ws_stock.update([df.columns.values.tolist()] + df.values.tolist())

def load_log_df() -> pd.DataFrame:
    df = _df_from_ws(ws_log)
    # elvárt oszlopok: timestamp,item_name,change,reason,source,source_id
    if df.empty:
        return pd.DataFrame(columns=["timestamp","item_name","change","reason","source","source_id"])
    for col in ["timestamp","item_name","change","reason","source","source_id"]:
        if col not in df.columns:
            # ha valami hiányzik, hozzuk létre, hogy ne haljon el
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
    # várható: 2025-12-18T15:45:00 (timezone nélkül) vagy timezone-os
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty datetime")
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:
        raise ValueError(f"Invalid ISO datetime: {s}") from e
    # ha nincs tz, tekintsük UTC-nek (egyszerű és determinisztikus)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

# ================== SHOPIFY ==================
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
    # Shopify REST orders.json, baseline UTC
    # created_at_min/max - iso8601
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

if "last_sync_msg" not in st.session_state:
    st.session_state.last_sync_msg = ""

if "stock_tab_opened_once" not in st.session_state:
    st.session_state.stock_tab_opened_once = False

# ================== CORE: APPLY SHOPIFY DELTAS ==================
def apply_shopify_deductions(baseline_iso: str):
    """
    Baseline óta keletkezett Shopify orderekből:
    - mosolap: -sum(quantity)
    - boríték: -1 (env típus)
    Csak olyan order_id-t dolgoz fel, ami még nincs a logban (source=shopify, source_id=order_id).
    """
    baseline_dt = parse_iso_dt(baseline_iso)

    # betöltés
    stock_df = load_stock_df()
    log_df = load_log_df()

    processed_ids = set(
        log_df.loc[log_df["source"].str.lower() == "shopify", "source_id"].astype(str).tolist()
    )

    orders = cached_orders_since(baseline_iso)

    # csak baseline UTÁN (Shopify created_at_min inclusive; mi szigorítunk > baseline)
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
            # ha bármi gáz, kihagyjuk
            continue
        if odt <= baseline_dt:
            continue

        new_orders.append(o)

    if not new_orders:
        return 0, 0, {}

    # aggregálás
    total_mosolap_delta = 0
    env_counts = {"F16": 0, "H18": 0, "I19": 0, "K20": 0}
    ts = now_iso()

    # segéd: készlet módosítás a df-ben
    def add_to_stock(item_name: str, delta: int):
        nonlocal stock_df
        idx = stock_df.index[stock_df["item_name"] == item_name].tolist()
        if not idx:
            # ha nincs a táblában, hozzáadjuk envelope-ként (mosolap már legyen)
            stock_df = pd.concat([stock_df, pd.DataFrame([{
                "item_type": "envelope" if item_name != "mosolap" else "mosolap",
                "item_name": item_name,
                "quantity": 0
            }])], ignore_index=True)
            idx = stock_df.index[stock_df["item_name"] == item_name].tolist()

        i = idx[0]
        stock_df.at[i, "quantity"] = int(stock_df.at[i, "quantity"]) + int(delta)

    # feldolgozás
    for o in new_orders:
        oid = str(o.get("id") or "")
        name = o.get("name", "")
        items = o.get("line_items", []) or []

        filtered = [i for i in items if not is_priority_item(i.get("title", ""))]

        # mosólap: quantity összege
        mos_qty = sum(int(i.get("quantity", 0)) for i in filtered)
        # boríték: a rendelés termékszáma alapján
        env = envelope_type(mos_qty)

        # mosólap levonás
        if mos_qty > 0:
            add_to_stock("mosolap", -mos_qty)
            total_mosolap_delta += mos_qty  # pozitív fogyásként számoljuk
            append_log(ts, "mosolap", -mos_qty, f"Auto Shopify ({name})", "shopify", oid)

        # boríték levonás
        if env in env_counts:
            add_to_stock(env, -1)
            env_counts[env] += 1
            append_log(ts, env, -1, f"Auto Shopify ({name})", "shopify", oid)

        # ha env == "Nincs", nem vonunk borítékot (vagy később dönthetsz másképp)

    # mentés készlet
    stock_df["quantity"] = pd.to_numeric(stock_df["quantity"], errors="coerce").fillna(0).astype(int)
    write_stock_df(stock_df)

    processed_orders = len(new_orders)
    return processed_orders, total_mosolap_delta, env_counts

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
        # egyszerű lekérés időintervallumra (nem baseline)
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
    # Baseline megjelenítés + állítás
    st.subheader("⏱️ Baseline (utolsó „igaz készlet” időpont)")

    current_baseline = get_setting("baseline_datetime")
    if not current_baseline:
        # ha üres, adjunk egy defaultot (most)
        current_baseline = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        set_setting("baseline_datetime", current_baseline)

    # UI: baseline editor
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

    # Készlet tab trigger: első megnyitáskor automatikus sync (plusz manuális gomb)
    sync_col1, sync_col2 = st.columns([2, 2])
    with sync_col1:
        manual_sync = st.button("🔄 Shopify → készlet szinkron (baseline óta)", use_container_width=True)
    with sync_col2:
        st.write("")
        st.caption("Trigger: Készlet fül megnyitása (első betöltés) vagy a fenti gombok.")

    # sync futtatás feltételek
    do_sync = False

    # baseline mentés + sync
    if save_baseline:
        combined = datetime.combine(new_base_date, new_base_time)
        combined = combined.replace(tzinfo=timezone.utc)  # egyszerű: UTC
        new_iso = combined.replace(microsecond=0).isoformat()
        set_setting("baseline_datetime", new_iso)
        # cache miatt:
        st.cache_data.clear()
        do_sync = True
        st.session_state.last_sync_msg = f"Baseline beállítva: {new_iso}"

    # első készlet-tab betöltéskor sync
    if not st.session_state.stock_tab_opened_once:
        st.session_state.stock_tab_opened_once = True
        do_sync = True

    if manual_sync:
        do_sync = True

    # sync lefuttatása
    if do_sync:
        baseline_now = get_setting("baseline_datetime")
        try:
            with st.spinner("Shopify rendelések feldolgozása baseline óta..."):
                processed_orders, mosolap_used, env_counts = apply_shopify_deductions(baseline_now)
            parts = []
            parts.append(f"✅ Feldolgozott új rendelések: {processed_orders} db")
            if mosolap_used:
                parts.append(f"🧺 Mosólap levonás: {mosolap_used} db")
            env_msg = ", ".join([f"{k}:{v}" for k, v in env_counts.items() if v > 0]) or "nincs"
            parts.append(f"✉️ Boríték levonás: {env_msg}")
            st.success(" | ".join(parts))
        except Exception as e:
            st.error(f"Szinkron hiba: {e}")

    st.markdown("---")

    # Aktuális készlet
    st.subheader("📦 Aktuális készlet (Google Sheet)")
    stock_df = load_stock_df()

    # Kiemelt készletkártyák
    mos_qty = int(stock_df.loc[stock_df["item_name"] == "mosolap", "quantity"].values[0]) if (stock_df["item_name"] == "mosolap").any() else 0
    f16 = int(stock_df.loc[stock_df["item_name"] == "F16", "quantity"].values[0]) if (stock_df["item_name"] == "F16").any() else 0
    h18 = int(stock_df.loc[stock_df["item_name"] == "H18", "quantity"].values[0]) if (stock_df["item_name"] == "H18").any() else 0
    i19 = int(stock_df.loc[stock_df["item_name"] == "I19", "quantity"].values[0]) if (stock_df["item_name"] == "I19").any() else 0
    k20 = int(stock_df.loc[stock_df["item_name"] == "K20", "quantity"].values[0]) if (stock_df["item_name"] == "K20").any() else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Mosólap", mos_qty)
    m2.metric("F16", f16)
    m3.metric("H18", h18)
    m4.metric("I19", i19)
    m5.metric("K20", k20)

    st.dataframe(stock_df, use_container_width=True)

    st.markdown("---")

    # Manuális készletmódosítás + változtatás dátuma (baseline)
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

    a1, a2 = st.columns(2)

    def apply_manual_delta(delta: int):
        nonlocal stock_df
        if not reason.strip():
            st.error("Megjegyzés kötelező!")
            return

        # készlet frissítés
        idx = stock_df.index[stock_df["item_name"] == item].tolist()
        if not idx:
            st.error("Ismeretlen tétel a stock_current-ben.")
            return
        i = idx[0]
        new_qty = int(stock_df.at[i, "quantity"]) + int(delta)
        stock_df.at[i, "quantity"] = new_qty
        write_stock_df(stock_df)

        # log
        ts = datetime.combine(change_date, change_time).replace(tzinfo=timezone.utc).replace(microsecond=0).isoformat()
        append_log(ts, item, delta, reason, "manual", "")

        # baseline beállítás ugyanarra az időpontra + azonnali sync
        set_setting("baseline_datetime", ts)
        st.cache_data.clear()

        with st.spinner("Baseline mentve, Shopify szinkron fut..."):
            processed_orders, mosolap_used, env_counts = apply_shopify_deductions(ts)

        env_msg = ", ".join([f"{k}:{v}" for k, v in env_counts.items() if v > 0]) or "nincs"
        st.success(f"Mentve ({item} {delta:+d}). Baseline: {ts}. Új rendelések: {processed_orders}. Mosólap levonás: {mosolap_used}. Boríték: {env_msg}")
        st.rerun()

    with a1:
        if st.button("➕ Feltöltés", use_container_width=True):
            apply_manual_delta(int(amount))

    with a2:
        if st.button("➖ Levonás", use_container_width=True):
            # opcionális: ne menjen mínuszba
            current_qty = int(stock_df.loc[stock_df["item_name"] == item, "quantity"].values[0])
            if amount > current_qty:
                st.error("Nincs ennyi készleten!")
            else:
                apply_manual_delta(-int(amount))

    st.markdown("---")

    # Log megjelenítés
    st.subheader("🧾 Készletmozgások (log)")
    log_df = load_log_df()
    if log_df.empty:
        st.info("Még nincs log bejegyzés.")
    else:
        # legutóbbi 200 sor
        view = log_df.tail(200).copy()
        st.dataframe(view, use_container_width=True)

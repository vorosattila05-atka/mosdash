# -*- coding: utf-8 -*-
import streamlit as st
import requests

st.set_page_config(
    page_title="Mosly – Shopify Boríték Dashboard",
    layout="wide"
)

# ================== SECRETS BETÖLTÉS ==================
def must_get_secret(key: str) -> str:
    if key not in st.secrets or not str(st.secrets[key]).strip():
        st.error(f"Hiányzó Secret: {key}")
        st.stop()
    return str(st.secrets[key]).strip()

APP_PASSWORD = must_get_secret("APP_PASSWORD")
SHOPIFY_STORE = must_get_secret("SHOPIFY_STORE")
SHOPIFY_API_KEY = must_get_secret("SHOPIFY_API_KEY")
SHOPIFY_API_PASSWORD = must_get_secret("SHOPIFY_API_PASSWORD")

BASE_URL = (
    f"https://{SHOPIFY_API_KEY}:{SHOPIFY_API_PASSWORD}@"
    f"{SHOPIFY_STORE}/admin/api/2024-10"
)

# ================== JELSZAVAS VÉDELEM ==================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 Bejelentkezés")
    pw = st.text_input("Jelszó", type="password")

    if pw:
        if pw == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Hibás jelszó")

    st.stop()

# ================== SESSION STATE INIT ==================
if "orders_data" not in st.session_state:
    st.session_state.orders_data = []

if "stats" not in st.session_state:
    st.session_state.stats = {}

if "avg_qty" not in st.session_state:
    st.session_state.avg_qty = 0.0

# ================== SEGÉDFÜGGVÉNYEK ==================
@st.cache_data(ttl=60)
def get_orders(start_date: str, end_date: str):
    url = (
        f"{BASE_URL}/orders.json"
        f"?status=any"
        f"&limit=250"
        f"&created_at_min={start_date}T00:00:00-00:00"
        f"&created_at_max={end_date}T23:59:59-00:00"
        f"&order=created_at+desc"
    )
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.json().get("orders", [])

def envelope_type(qty: int) -> str:
    if qty == 1:
        return "F16"
    if qty in (2, 3):
        return "H18"
    if qty == 4:
        return "I19"
    if qty in (5, 6):
        return "K20"
    return "Nincs kategória"

def is_priority_item(title: str) -> bool:
    t = (title or "").lower()
    keywords = [
        "elsőbbségi", "elsobsegi",
        "priority", "express",
        "gyorsított", "gyorsitott"
    ]
    return any(k in t for k in keywords)

# ================== UI ==================
st.title("📦 Mosly – Shopify rendelés & boríték dashboard")
st.caption("Az elsőbbségi / priority szállítási tétel nem számít bele a termékszámba.")

st.markdown("---")

col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    start_date = st.date_input("Kezdő dátum")
with col2:
    end_date = st.date_input("Végdátum")
with col3:
    fetch = st.button("🔄 Rendelések lekérése", use_container_width=True)

# ================== RENDELÉSEK LEKÉRÉSE ==================
if fetch:
    with st.spinner("Shopify adatok lekérése..."):
        try:
            orders = get_orders(str(start_date), str(end_date))
        except Exception as e:
            st.error(f"Shopify API hiba: {e}")
            st.stop()

    st.session_state.orders_data = []
    st.session_state.stats = {}

    if not orders:
        st.warning("Nincs rendelés ebben az időszakban.")
    else:
        for order in orders:
            items = order.get("line_items", [])

            filtered_items = [
                i for i in items
                if not is_priority_item(i.get("title", ""))
            ]

            qty = sum(int(i.get("quantity", 0)) for i in filtered_items)
            env = envelope_type(qty)

            st.session_state.orders_data.append({
                "Rendelés": order.get("name"),
                "Termékszám": qty,
                "Boríték": env
            })

        total_orders = len(st.session_state.orders_data)
        total_qty = sum(o["Termékszám"] for o in st.session_state.orders_data)
        st.session_state.avg_qty = total_qty / total_orders if total_orders else 0

        for o in st.session_state.orders_data:
            st.session_state.stats[o["Boríték"]] = (
                st.session_state.stats.get(o["Boríték"], 0) + 1
            )

# ================== MEGJELENÍTÉS ==================
if st.session_state.orders_data:

    st.subheader("📋 Rendelések")
    st.dataframe(st.session_state.orders_data, use_container_width=True)

    st.subheader("📊 Boríték statisztika")

    total_orders = len(st.session_state.orders_data)
    avg_qty = st.session_state.avg_qty

    stats_sorted = sorted(
        st.session_state.stats.items(),
        key=lambda x: x[1],
        reverse=True
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Összes rendelés", total_orders)
    c2.metric("Átlagos termékszám", f"{avg_qty:.2f}")
    c3.metric("Borítéktípusok", len(stats_sorted))

    for env, count in stats_sorted:
        percent = (count / total_orders * 100) if total_orders else 0
        st.write(f"**{env}** → {count} db ({percent:.1f}%)")

    st.markdown("---")

    # ================== ELŐREJELZÉS ==================
    st.subheader("🔮 Boríték előrejelzés")

    incoming = st.number_input(
        "Beérkező mosólap darabszám",
        min_value=1,
        step=1
    )

    if incoming and avg_qty > 0:
        est_orders = incoming / avg_qty
        st.write(f"**Becsült kiszolgálható rendelések:** {est_orders:.0f} db")

        env_only = {
            k: v for k, v in st.session_state.stats.items()
            if k in ["F16", "H18", "I19", "K20"]
        }

        env_total = sum(env_only.values()) or 1

        st.write("**Várható borítékigény:**")
        for env, count in env_only.items():
            ratio = count / env_total
            need = round(ratio * est_orders)
            st.write(f"- {env}: **{need} db**")

else:
    st.info("ℹ️ Először kérd le a rendeléseket a fenti dátum szűrővel.")

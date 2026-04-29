import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import hashlib
import json
from pathlib import Path
from datetime import datetime

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="NYC Transit Ridership", page_icon="🚇", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; background-color: #0d1117; color: #e6edf3; }
h1, h2, h3 { font-family: 'DM Serif Display', serif; color: #e6edf3; }
.metric-card { background: linear-gradient(135deg, #161b22 0%, #1c2333 100%); border: 1px solid #30363d; border-radius: 12px; padding: 1.2rem 1.5rem; text-align: center; }
.metric-value { font-size: 2rem; font-weight: 600; color: #58a6ff; }
.metric-label { font-size: 0.85rem; color: #8b949e; margin-top: 0.25rem; text-transform: uppercase; letter-spacing: 0.05em; }
</style>
""", unsafe_allow_html=True)

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_db_params():
    return {
        "dbname":   os.getenv("DB_NAME", "NYC"),
        "user":     os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "host":     os.getenv("DB_HOST", "db"),
        "port":     os.getenv("DB_PORT", "5432"),
    }

@st.cache_resource
def get_engine():
    p = get_db_params()
    from urllib.parse import quote_plus
    password = quote_plus(p['password'])
    return create_engine(f"postgresql://{p['user']}:{password}@{p['host']}:{p['port']}/{p['dbname']}")

@st.cache_data(ttl=60)
def load_daily():
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT date, ridership, transport_type, year FROM daily_ridership"), conn)
    df["date"] = pd.to_datetime(df["date"])
    df["ridership"] = pd.to_numeric(df["ridership"], errors="coerce")
    df["year"] = df["year"].astype(int)
    return df

@st.cache_data(ttl=60)
def load_yearly():
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT year, transport_type, total_ridership FROM yearly_ridership"), conn)
    df["year"] = df["year"].astype(int)
    df["total_ridership"] = pd.to_numeric(df["total_ridership"], errors="coerce")
    return df

@st.cache_data(ttl=60)
def load_transport_types():
    with get_engine().connect() as conn:
        df = pd.read_sql(text("SELECT transport_type FROM transport_types"), conn)
    return df["transport_type"].tolist()

# ── Batch update (pulls fresh data from APIs) ────────────────────────────────
def fetch_from_apis():
    """Re-fetch data from NYC APIs, transform, and return a clean dataframe."""
    import requests
    from io import StringIO

    # Subway & Bus
    response = requests.get("https://data.ny.gov/resource/vxuj-8kew.json", params={"$limit": 6000})
    response.raise_for_status()
    d = pd.DataFrame(response.json())
    d.columns = d.columns.str.strip().str.lower().str.replace(" ", "_")
    d = d[["date", "subways_total_estimated_ridership", "buses_total_estimated_ridersip"]].dropna().drop_duplicates()
    d = d.rename(columns={"buses_total_estimated_ridersip": "buses_total_estimated_ridership"})
    d["date"] = pd.to_datetime(d["date"])
    d["year"] = d["date"].dt.year
    d["subways_total_estimated_ridership"] = pd.to_numeric(d["subways_total_estimated_ridership"], errors="coerce")
    d["buses_total_estimated_ridership"] = pd.to_numeric(d["buses_total_estimated_ridership"], errors="coerce")

    subways = d[["date", "subways_total_estimated_ridership", "year"]].copy()
    subways["transport_type"] = "subway"
    subways = subways.rename(columns={"subways_total_estimated_ridership": "ridership"})

    buses = d[["date", "buses_total_estimated_ridership", "year"]].copy()
    buses["transport_type"] = "bus"
    buses = buses.rename(columns={"buses_total_estimated_ridership": "ridership"})

    # Ferry
    response2 = requests.get("https://data.cityofnewyork.us/resource/6eng-46dm.csv",
                              params={"$limit": 6000, "$order": ":id"})
    response2.raise_for_status()
    ferry = pd.read_csv(StringIO(response2.text))
    ferry.columns = ferry.columns.str.strip().str.lower().str.replace(" ", "_")
    ferry["whitehall_terminal"] = pd.to_numeric(ferry["whitehall_terminal"], errors="coerce").fillna(0)
    ferry["stgeorge_terminal"] = pd.to_numeric(ferry["stgeorge_terminal"], errors="coerce").fillna(0)
    ferry["ridership"] = ferry["whitehall_terminal"] + ferry["stgeorge_terminal"]
    ferry["date"] = pd.to_datetime(ferry["date"])
    ferry["year"] = ferry["date"].dt.year
    ferry["transport_type"] = "ferry"
    ferry = ferry[["date", "ridership", "transport_type", "year"]].dropna().drop_duplicates()
    ferry = ferry[ferry["year"].between(2020, 2024)]

    combined = pd.concat([subways, buses, ferry], ignore_index=True)
    for col in combined.select_dtypes(include="object").columns:
        combined[col] = combined[col].str.lower().str.strip()
    combined = combined[combined["year"].between(2020, 2024)]
    return combined


def run_batch_update():
    try:
        df = fetch_from_apis()
        msg_prefix = f"Fetched {len(df)} rows from APIs."
    except Exception as e:
        return False, f"Failed to fetch from APIs: {e}"

    conn = psycopg2.connect(**get_db_params())
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='daily_ridership_date_transport_type_key') THEN
                    CREATE UNIQUE INDEX daily_ridership_date_transport_type_key ON daily_ridership (date, transport_type); END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='yearly_ridership_year_transport_type_key') THEN
                    CREATE UNIQUE INDEX yearly_ridership_year_transport_type_key ON yearly_ridership (year, transport_type); END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='transport_types_transport_type_key') THEN
                    CREATE UNIQUE INDEX transport_types_transport_type_key ON transport_types (transport_type); END IF;
            END$$
        """)

        unique_types = df["transport_type"].dropna().unique().tolist()
        cursor.executemany("INSERT INTO transport_types (transport_type) VALUES (%s) ON CONFLICT DO NOTHING;",
                           [(t,) for t in unique_types])

        cursor.execute("TRUNCATE TABLE daily_ridership CASCADE;")
        daily_rows = [(row["date"], float(row["ridership"]), row["transport_type"], int(row["year"]))
                      for _, row in df.iterrows()]
        if daily_rows:
            execute_values(cursor, """
                INSERT INTO daily_ridership (date, ridership, transport_type, year)
                VALUES %s ON CONFLICT DO NOTHING;
            """, daily_rows, page_size=1000)

        cursor.execute("TRUNCATE TABLE yearly_ridership;")
        cursor.execute("""
            INSERT INTO yearly_ridership (year, transport_type, total_ridership)
            SELECT year, transport_type, SUM(ridership) FROM daily_ridership
            GROUP BY year, transport_type
            ON CONFLICT (year, transport_type) DO UPDATE SET total_ridership = EXCLUDED.total_ridership;
        """)

        conn.commit()
        return True, f"{msg_prefix} Database updated successfully."
    except Exception as e:
        conn.rollback()
        return False, f"Error updating database: {e}"
    finally:
        cursor.close()
        conn.close()

# ── Load data ─────────────────────────────────────────────────────────────────
try:
    df     = load_daily()
    yearly = load_yearly()
    types  = load_transport_types()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("## 🔎 Filters")
all_years      = sorted(df["year"].unique())
selected_years = st.sidebar.multiselect("Year(s)", all_years, default=all_years)
selected_types = st.sidebar.multiselect("Transport Type(s)", types, default=types)


st.sidebar.markdown("---")
st.sidebar.markdown("## 🔄 Batch Update")
st.sidebar.markdown("Pull new data from APIs into the database.")
if st.sidebar.button("Refresh Data"):
    with st.spinner("Running batch update..."):
        success, message = run_batch_update()
    if success:
        st.sidebar.success(message)
        load_daily.clear()
        load_yearly.clear()
        st.rerun()
    else:
        st.sidebar.info(message)



# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df[df["year"].isin(selected_years) & df["transport_type"].isin(selected_types)]

filtered_yearly = yearly[yearly["year"].isin(selected_years) & yearly["transport_type"].isin(selected_types)]

color_map = {"subway": "#58a6ff", "bus": "#3fb950", "ferry": "#f78166"}

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("# 🚇 NYC Transit Ridership Dashboard")
st.markdown("Daily ridership across **Subways**, **Buses**, and **Ferries** — 2020 to 2024.")
st.markdown("---")

# ── KPI cards ─────────────────────────────────────────────────────────────────
total     = filtered["ridership"].sum()
daily_avg = filtered.groupby("date")["ridership"].sum().mean() if not filtered.empty else 0
peak_row  = filtered.loc[filtered["ridership"].idxmax()] if not filtered.empty else None
top_type  = filtered.groupby("transport_type")["ridership"].sum().idxmax() if not filtered.empty else "N/A"

def metric_card(col, value, label):
    col.markdown(f'<div class="metric-card"><div class="metric-value">{value}</div><div class="metric-label">{label}</div></div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
metric_card(c1, f"{total/1e9:.2f}B", "Total Riders")
metric_card(c2, f"{daily_avg/1e6:.2f}M", "Avg Daily Riders")
metric_card(c3, peak_row["date"].strftime("%b %d, %Y") if peak_row is not None else "—", "Peak Day")
metric_card(c4, top_type.capitalize(), "Top Mode")
st.markdown("---")

# ── Chart 1: Daily ridership line ─────────────────────────────────────────────
st.markdown("### 📈 Daily Ridership Over Time")
fig1 = px.line(filtered.groupby(["date","transport_type"])["ridership"].sum().reset_index(),
               x="date", y="ridership", color="transport_type", color_discrete_map=color_map,
               template="plotly_dark", labels={"ridership":"Daily Riders","date":"Date","transport_type":"Mode"})
fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", hovermode="x unified")
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Yearly totals (from yearly_ridership table) ──────────────────────
st.markdown("### 📊 Yearly Total Ridership by Mode")
fig2 = px.bar(filtered_yearly, x="year", y="total_ridership", color="transport_type",
              barmode="group", color_discrete_map=color_map, template="plotly_dark",
              labels={"total_ridership":"Total Riders","year":"Year","transport_type":"Mode"})
fig2.for_each_trace(lambda t: t.update(
    text=[f"{v/1e9:.2f}B" if v >= 1e9 else f"{v/1e6:.0f}M" for v in t.y],
    textposition="outside"
))
fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(tickmode="linear"))
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3 & 4 side by side ──────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.markdown("### 🥧 Ridership Share by Mode")
    share = filtered.groupby("transport_type")["ridership"].sum().reset_index()
    fig3 = px.pie(share, names="transport_type", values="ridership", color="transport_type",
                  color_discrete_map=color_map, hole=0.45, template="plotly_dark")
    fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig3, use_container_width=True)

with col_b:
    st.markdown("### 📅 Avg Ridership by Month")
    monthly = filtered.copy()
    monthly["month"] = monthly["date"].dt.month
    monthly_avg = monthly.groupby(["month","transport_type"])["ridership"].mean().reset_index()
    month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    monthly_avg["month_name"] = monthly_avg["month"].map(month_labels)
    fig4 = px.bar(monthly_avg, x="month_name", y="ridership", color="transport_type", barmode="group",
                  color_discrete_map=color_map, template="plotly_dark",
                  labels={"ridership":"Avg Riders","month_name":"Month","transport_type":"Mode"},
                  category_orders={"month_name": list(month_labels.values())})
    fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)

# ── Chart 5: YoY % change ─────────────────────────────────────────────────────
st.markdown("### 📉 Year-over-Year Ridership Change (%)")
yoy = filtered_yearly.sort_values(["transport_type","year"]).copy()
yoy["pct_change"] = yoy.groupby("transport_type")["total_ridership"].pct_change() * 100
fig5 = px.line(yoy.dropna(subset=["pct_change"]), x="year", y="pct_change", color="transport_type",
               markers=True, color_discrete_map=color_map, template="plotly_dark",
               labels={"pct_change":"YoY Change (%)","year":"Year","transport_type":"Mode"})
fig5.add_hline(y=0, line_dash="dot", line_color="#8b949e")
fig5.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(tickmode="linear"))
st.plotly_chart(fig5, use_container_width=True)

# ── Raw data table ────────────────────────────────────────────────────────────
st.markdown("---")
with st.expander("🗃️ View Raw Data"):
    search = st.text_input("Search transport type", "")
    display_df = filtered[filtered["transport_type"].str.contains(search, case=False)] if search else filtered
    st.dataframe(display_df.sort_values("date", ascending=False).reset_index(drop=True), use_container_width=True)
    st.caption(f"{len(display_df):,} rows shown")

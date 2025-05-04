import streamlit as st
import pandas as pd
import plotly.express as px
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Título
st.title("🚲 Cycle World Dashboard")

# Verificación de credenciales
if not all([os.getenv("SNOWFLAKE_ACCOUNT"), os.getenv("SNOWFLAKE_USER"), os.getenv("SNOWFLAKE_PASSWORD")]):
    st.error("❌ Faltan credenciales para conectarse a Snowflake. Verifica tus secrets en Streamlit Cloud.")
    st.stop()

# Conexión a Snowflake
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CYCLE_WORLD",
    "schema": "PUBLIC"
}
session = Session.builder.configs(connection_parameters).create()

@st.cache_data
def cargar_tabla(nombre):
    return session.table(nombre).to_pandas()

# Cargar datos
viajes_df = cargar_tabla("RESUMEN_VIAJES")
top_estaciones_df = cargar_tabla("ESTACIONES_MAS_CONCURRIDAS")
colores_df = cargar_tabla("USO_COLORES_BICICLETAS")
lluvia = cargar_tabla("PORCENTAJE_VIAJES_LLUVIOSOS").iloc[0, 0]
duracion = cargar_tabla("DURACION_PROMEDIO_DIAS_DESPEJADOS").iloc[0, 0]
estaciones_mapa = cargar_tabla("ESTACIONES_COORDENADAS")  # Supone tabla con columnas STATION_NAME, LAT, LON

# Formateo de fechas
viajes_df["FECHA_INICIO"] = pd.to_datetime(viajes_df["FECHA_INICIO"], format="%d/%m/%Y")
viajes_df["FECHA_FIN"] = pd.to_datetime(viajes_df["FECHA_FIN"], format="%d/%m/%Y")

# Sidebar - Filtros
st.sidebar.header("Filtros")
min_fecha = viajes_df["FECHA_INICIO"].min().date()
max_fecha = viajes_df["FECHA_INICIO"].max().date()

fecha_inicio = st.sidebar.date_input("Fecha desde", value=min_fecha, min_value=min_fecha, max_value=max_fecha, format="DD/MM/YYYY")
fecha_fin = st.sidebar.date_input("Fecha hasta", value=max_fecha, min_value=min_fecha, max_value=max_fecha, format="DD/MM/YYYY")
sectores = sorted(viajes_df["SECTOR_ESTACION"].dropna().unique())
sector = st.sidebar.selectbox("Selecciona sector", ["Todos"] + sectores)

# Aplicar filtros
mask = (viajes_df["FECHA_INICIO"].dt.date >= fecha_inicio) & (viajes_df["FECHA_INICIO"].dt.date <= fecha_fin)
if sector != "Todos":
    mask &= viajes_df["SECTOR_ESTACION"] == sector
viajes_filtrados = viajes_df.loc[mask].copy()

# Tabs
with st.tabs(["Resumen", "Estaciones", "Clima", "Mapa"]):
    
    # Resumen
    with st.container():
        st.subheader("📋 Viajes Filtrados")
        st.write(f"Se encontraron **{len(viajes_filtrados)}** viajes en el periodo seleccionado.")
        st.dataframe(viajes_filtrados)

    # Estaciones
    with st.container():
        st.markdown("---")
        st.subheader("🏙️ Top Estaciones Más Concurridas")
        fig1 = px.bar(top_estaciones_df, x="STATION_NAME", y="TOTAL_USOS", title="Top Estaciones")
        st.plotly_chart(fig1)

        st.subheader("🎨 Uso por Color de Bicicleta")
        fig2 = px.bar(colores_df, x="BIKE_COLOR", y="USAGE_COUNT", title="Colores de Bicicletas")
        st.plotly_chart(fig2)

    # Clima
    with st.container():
        st.markdown("---")
        st.subheader("🌦️ Clima y Viajes")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🌧️ % de viajes con lluvia", f"{lluvia}%")
        with col2:
            st.metric("🌤️ Duración promedio (min)", f"{duracion} min")

    # Mapa
    with st.container():
        st.markdown("---")
        st.subheader("🗺️ Ubicación de Estaciones")
        st.map(estaciones_mapa.rename(columns={"LAT": "lat", "LON": "lon"}))

# Cierre conexión
session.close()

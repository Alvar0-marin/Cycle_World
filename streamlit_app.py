import streamlit as st
import pandas as pd
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import datetime as dt

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

# 🔹 Cargar y parsear datos de viajes
viajes_df = session.table("RESUMEN_VIAJES").to_pandas()
viajes_df["FECHA_INICIO"] = pd.to_datetime(viajes_df["FECHA_INICIO"], format="%d/%m/%Y")
viajes_df["FECHA_FIN"]    = pd.to_datetime(viajes_df["FECHA_FIN"],    format="%d/%m/%Y")

# Determinar el rango de fechas disponible
min_fecha = viajes_df["FECHA_INICIO"].min().date()
max_fecha = viajes_df["FECHA_INICIO"].max().date()

# Sidebar: Filtros con rango dinámico y formato día/mes/año
st.sidebar.header("Filtros")
fecha_inicio = st.sidebar.date_input(
    "Fecha desde",
    value=min_fecha,
    min_value=min_fecha,
    max_value=max_fecha,
    format="DD/MM/YYYY"
)
fecha_fin = st.sidebar.date_input(
    "Fecha hasta",
    value=max_fecha,
    min_value=min_fecha,
    max_value=max_fecha,
    format="DD/MM/YYYY"
)
sector = st.sidebar.text_input("Filtrar por sector (ej: Marylebone)", "")

# Filtrar DataFrame
mask = (
    (viajes_df["FECHA_INICIO"].dt.date >= fecha_inicio) &
    (viajes_df["FECHA_INICIO"].dt.date <= fecha_fin)
)
if sector:
    mask &= viajes_df["SECTOR_ESTACION"].str.contains(sector, case=False)

viajes_filtrados = viajes_df.loc[mask].copy()

# Formatear fechas para mostrar
viajes_filtrados["FECHA_INICIO"] = viajes_filtrados["FECHA_INICIO"].dt.strftime("%d/%m/%Y")
viajes_filtrados["FECHA_FIN"]    = viajes_filtrados["FECHA_FIN"].dt.strftime("%d/%m/%Y")

# Mostrar tabla de viajes filtrados
st.subheader("📋 Viajes Filtrados")
st.dataframe(viajes_filtrados)

# 🔹 ESTACIONES MÁS CONCURRIDAS
st.subheader("🏙️ Top Estaciones Más Concurridas")
top_estaciones_df = session.table("ESTACIONES_MAS_CONCURRIDAS").to_pandas()
st.bar_chart(top_estaciones_df.set_index("STATION_NAME"))

# 🔹 USO DE COLORES
st.subheader("🎨 Colores de Bicicletas")
colores_df = session.table("USO_COLORES_BICICLETAS").to_pandas()
st.bar_chart(colores_df.set_index("BIKE_COLOR"))

# 🔹 PORCENTAJE LLUVIOSOS / DURACIÓN DESPEJADOS
lluvia   = session.table("PORCENTAJE_VIAJES_LLUVIOSOS").to_pandas().iloc[0, 0]
duracion = session.table("DURACION_PROMEDIO_DIAS_DESPEJADOS").to_pandas().iloc[0, 0]
st.metric("🌧️ % de viajes con lluvia",     f"{lluvia}%")
st.metric("🌤️ Duración promedio (min)",    f"{duracion} min")

# Cierre de sesión
session.close()

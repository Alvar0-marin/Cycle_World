from pathlib import Path
import os

# Código de la app Streamlit corregido
streamlit_code = """
import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Título
st.title("🚲 Cycle World Dashboard")

# Conexión a Snowflake (reemplaza con tu configuración)
connection_parameters = {
    "account": os.getenv("NXB97474"),
    "user": os.getenv("ALVARO"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "CYCLE_WORLD",
    "schema": "PUBLIC"
}
session = Session.builder.configs(connection_parameters).create()

# Sidebar: Filtros
st.sidebar.header("Filtros")
fecha_inicio = st.sidebar.date_input("Fecha desde")
fecha_fin = st.sidebar.date_input("Fecha hasta")
sector = st.sidebar.text_input("Filtrar por sector (ej: Marylebone)", "")

# 🔹 RESUMEN DE VIAJES
viajes_df = session.table("RESUMEN_VIAJES").to_pandas()
if sector:
    viajes_df = viajes_df[viajes_df["SECTOR_ESTACION"].str.contains(sector, case=False)]
viajes_df["FECHA_INICIO"] = pd.to_datetime(viajes_df["FECHA_INICIO"], format="%d/%m/%Y")
viajes_df["FECHA_FIN"] = pd.to_datetime(viajes_df["FECHA_FIN"], format="%d/%m/%Y")
viajes_filtrados = viajes_df[(viajes_df["FECHA_INICIO"] >= pd.to_datetime(fecha_inicio)) & 
                              (viajes_df["FECHA_INICIO"] <= pd.to_datetime(fecha_fin))]

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
lluvia = session.table("PORCENTAJE_VIAJES_LLUVIOSOS").to_pandas().iloc[0,0]
duracion = session.table("DURACION_PROMEDIO_DIAS_DESPEJADOS").to_pandas().iloc[0,0]
st.metric("🌧️ % de viajes con lluvia", f"{lluvia}%")
st.metric("🌤️ Duración promedio (min)", f"{duracion} min")
"""

# Guardar archivo
file_path = Path("/mnt/data/streamlit_app.py")
file_path.write_text(streamlit_code)

str(file_path)

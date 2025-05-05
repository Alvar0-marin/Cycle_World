import streamlit as st
import pandas as pd
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import datetime as dt
import plotly.express as px

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

# Cargar y parsear datos
with st.spinner("Cargando datos de viajes..."):
    viajes_df = session.table("RESUMEN_VIAJES").to_pandas()

viajes_df["FECHA_INICIO"] = pd.to_datetime(viajes_df["FECHA_INICIO"], format="%d/%m/%Y")
viajes_df["FECHA_FIN"]    = pd.to_datetime(viajes_df["FECHA_FIN"], format="%d/%m/%Y")

# Filtros
st.sidebar.header("📅 Filtros por Fecha y Sector")

min_fecha = viajes_df["FECHA_INICIO"].min().date()
max_fecha = viajes_df["FECHA_INICIO"].max().date()

fecha_inicio = st.sidebar.date_input("Fecha desde", min_value=min_fecha, max_value=max_fecha, value=min_fecha, format="DD/MM/YYYY")
fecha_fin    = st.sidebar.date_input("Fecha hasta", min_value=min_fecha, max_value=max_fecha, value=max_fecha, format="DD/MM/YYYY")
sector = st.sidebar.text_input("Filtrar por sector (ej: Marylebone)", "")

# Validar fechas
if fecha_inicio > fecha_fin:
    st.warning("⚠️ La fecha de inicio no puede ser posterior a la fecha de fin.")
    st.stop()

# Aplicar filtros
mask = (
    (viajes_df["FECHA_INICIO"].dt.date >= fecha_inicio) &
    (viajes_df["FECHA_INICIO"].dt.date <= fecha_fin)
)
if sector:
    mask &= viajes_df["SECTOR_ESTACION"].str.contains(sector, case=False)

viajes_filtrados = viajes_df.loc[mask].copy()
viajes_filtrados["FECHA_INICIO"] = viajes_filtrados["FECHA_INICIO"].dt.strftime("%d/%m/%Y")
viajes_filtrados["FECHA_FIN"]    = viajes_filtrados["FECHA_FIN"].dt.strftime("%d/%m/%Y")

# Mostrar cantidad de viajes
st.metric("🧾 Total de viajes filtrados", len(viajes_filtrados))

# Tabla con viajes
with st.expander("📋 Detalles de viajes filtrados"):
    st.dataframe(viajes_filtrados)

# Top estaciones más concurridas
st.subheader("🏙️ Estaciones más concurridas")
top_estaciones_df = session.table("ESTACIONES_MAS_CONCURRIDAS").to_pandas()
st.write("Columnas disponibles:", top_estaciones_df.columns.tolist())
columnas = [col.upper().replace(" ", "_") for col in top_estaciones_df.columns]
top_estaciones_df.columns = columnas

# Usar los nombres reales directamente
if "STATION_NAME" in top_estaciones_df.columns and "TOTAL_MOVIMIENTOS" in top_estaciones_df.columns:
    fig_estaciones = px.bar(top_estaciones_df, x="STATION_NAME", y="TOTAL_MOVIMIENTOS", title="Top Estaciones Más Concurridas")
    st.plotly_chart(fig_estaciones)
else:
    st.warning("❌ No se encontraron las columnas esperadas en la tabla.")
    st.dataframe(top_estaciones_df)

# Uso de colores de bicicletas
st.subheader("🎨 Uso de bicicletas por color")
colores_df = session.table("USO_COLORES_BICICLETAS").to_pandas()
fig_colores = px.bar(colores_df, x=colores_df.columns[0], y=colores_df.columns[1], title="Uso por Color de Bicicleta")
st.plotly_chart(fig_colores)

# Destacar color más y menos usado
color_mas_usado = colores_df.loc[colores_df.iloc[:,1].idxmax()]
color_menos_usado = colores_df.loc[colores_df.iloc[:,1].idxmin()]
col1, col2 = st.columns(2)
col1.metric("🎯 Color más usado", color_mas_usado[0])
col2.metric("🚫 Menos usado", color_menos_usado[0])

# Métricas clima
st.subheader("🌦️ Viajes y Clima")
lluvia = session.table("PORCENTAJE_VIAJES_LLUVIOSOS").to_pandas().iloc[0, 0]
duracion = session.table("DURACION_PROMEDIO_DIAS_DESPEJADOS").to_pandas().iloc[0, 0]

col3, col4 = st.columns(2)
col3.metric("🌧️ % de viajes con lluvia", f"{lluvia}%")
col4.metric("🌤️ Duración promedio (min)", f"{duracion} min")

# Estaciones sin bicicletas
st.subheader("❗ Estaciones sin bicicletas disponibles")
try:
    sin_bicicletas_df = session.table("ESTACIONES_SIN_BICICLETAS").to_pandas()
    if sin_bicicletas_df.empty:
        st.info("No hubo días con estaciones completamente vacías.")
    else:
        st.dataframe(sin_bicicletas_df)
except:
    st.warning("No se encontró la vista ESTACIONES_SIN_BICICLETAS.")

# Reporte de franja horaria
st.subheader("📊 Movimientos por Franja Horaria")

# Filtrar REPORTE_MOVIMIENTOS por fecha usando strings seguros
try:
    reporte_df = (
        session.table("REPORTE_MOVIMIENTOS")
        .filter((col("FECHA_VIAJE") >= lit(fecha_inicio_str)) & (col("FECHA_VIAJE") <= lit(fecha_fin_str)))
        .to_pandas()
    )

    with st.expander("Ver detalles del reporte por franja horaria"):
        st.dataframe(reporte_df)

    fig_franja = px.bar(
        reporte_df.groupby(["FRANJA_HORARIA", "TIPO_MOVIMIENTO"])["TOTAL"].sum().reset_index(),
        x="FRANJA_HORARIA",
        y="TOTAL",
        color="TIPO_MOVIMIENTO",
        barmode="group",
        title="Total de Movimientos por Franja Horaria"
    )
    st.plotly_chart(fig_franja)
except Exception as e:
    st.warning(f"No se pudo cargar REPORTE_MOVIMIENTOS: {e}")

# Cierre de sesión
session.close()

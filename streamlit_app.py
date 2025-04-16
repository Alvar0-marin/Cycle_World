import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os

# --- Configuración de la conexión a Snowflake --- #
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

# --- Sidebar con filtros --- #
st.sidebar.title("Filtros de Tiempo")
fecha_inicio = st.sidebar.date_input("Fecha de inicio", value=pd.to_datetime("2011-01-01"))
fecha_fin = st.sidebar.date_input("Fecha de fin", value=pd.to_datetime("2011-02-28"))
horario = st.sidebar.selectbox("Horario", options=["Todos", "Mañana", "Valle", "Tarde"])

# --- Cargar datos desde Snowflake --- #
df_journeys = session.table("JOURNEYS").to_pandas()
df_bikes = session.table("BIKES").to_pandas()
df_stations = session.table("STATIONS").to_pandas()

# --- Función para aplicar filtros --- #
def aplicar_filtros(df):
    df['start_datetime'] = pd.to_datetime(
        df[['START_YEAR', 'START_MONTH', 'START_DATE', 'START_HOUR', 'START_MINUTE']]
        .rename(columns={
            'START_YEAR': 'year', 'START_MONTH': 'month', 'START_DATE': 'day',
            'START_HOUR': 'hour', 'START_MINUTE': 'minute'
        })
    )

    df = df[(df['start_datetime'].dt.date >= fecha_inicio) &
            (df['start_datetime'].dt.date <= fecha_fin)]

    if horario != "Todos":
        horarios = {
            "Mañana": (7, 9),
            "Valle": (10, 16),
            "Tarde": (17, 23)
        }
        df = df[df['start_datetime'].dt.hour.between(*horarios[horario])]

    return df

# --- Aplicar filtros --- #
df_filtrado = aplicar_filtros(df_journeys)

# --- Merge para reporte resumen --- #
df_resumen = df_filtrado.merge(df_bikes, on="BIKE_ID", how="left")
df_resumen = df_resumen.merge(df_stations, left_on="START_STATION_ID", right_on="STATION_ID", how="left")

df_resumen["FECHA_INICIO"] = df_resumen["start_datetime"].dt.strftime("%d/%m/%Y")
df_resumen["FECHA_FIN"] = pd.to_datetime(df_resumen[["END_YEAR", "END_MONTH", "END_DATE"]].rename(columns={
    "END_YEAR": "year", "END_MONTH": "month", "END_DATE": "day"
})).dt.strftime("%d/%m/%Y")

# --- Mostrar reporte resumen --- #
st.title("🚲 Cycle World Dashboard")
st.subheader("📋 Reporte resumen de viajes")
st.dataframe(df_resumen[[
    "JOURNEY_ID", "FECHA_INICIO", "FECHA_FIN", "STATION_NAME", "BIKE_COLOR"
]])

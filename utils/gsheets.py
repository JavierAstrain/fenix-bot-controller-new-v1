import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

@st.cache_data(ttl=600, show_spinner=False)
def load_sheets(sheet_id: str, allow_sheets=("MODELO_BOT","FINANZAS")) -> dict:
    svc_info = st.secrets.get("GOOGLE_SERVICE_ACCOUNT", None)
    if not svc_info:
        raise RuntimeError("Faltan credenciales GOOGLE_SERVICE_ACCOUNT en st.secrets")
    creds = Credentials.from_service_account_info(svc_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)

    data = {}
    for ws in sh.worksheets():
        name = ws.title.strip()
        if name.upper() not in [s.upper() for s in allow_sheets]:
            continue
        values = ws.get_all_records()
        if not values:
            continue
        df = pd.DataFrame(values, dtype=str)
        df = df.loc[:, ~df.columns.duplicated()]
        data[name] = df
    if not data:
        raise RuntimeError("No se pudieron cargar hojas permitidas. Verifica nombres y permisos.")
    return data

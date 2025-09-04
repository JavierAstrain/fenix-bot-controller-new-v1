import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import streamlit as st

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _get_service_info():
    svc = st.secrets.get("GOOGLE_SERVICE_ACCOUNT", None)
    if svc is None:
        raise RuntimeError("Faltan credenciales GOOGLE_SERVICE_ACCOUNT en st.secrets")
    if isinstance(svc, str):
        try:
            svc = json.loads(svc)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT debe ser JSON válido. Detalle: {e}")
    return svc

@st.cache_data(ttl=600, show_spinner=False)
def load_sheets(sheet_id: str, allow_sheets=("MODELO_BOT","FINANZAS")) -> dict:
    svc_info = _get_service_info()
    creds = Credentials.from_service_account_info(svc_info, scopes=SCOPES)
    client = gspread.authorize(creds)

    try:
        sh = client.open_by_key(sheet_id)
    except Exception as e:
        correo = svc_info.get("client_email","(sin email)")
        raise RuntimeError(
            "No se encontró la planilla o no hay acceso.\n"
            f"- Verifica SHEET_ID.\n"
            f"- Comparte el archivo con: {correo} (Viewer)."
        )

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

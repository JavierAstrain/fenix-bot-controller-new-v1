# utils/skills.py  — SOLO MODELO_BOT (estricto por bandera)
import os, re, yaml, unicodedata
import pandas as pd
import numpy as np
from .nlp import find_col, ilike

# ----------------- column map -----------------
COLUMN_MAP = {}
try:
    if os.path.exists("column_map.yaml"):
        with open("column_map.yaml", "r", encoding="utf-8") as f:
            COLUMN_MAP = yaml.safe_load(f) or {}
except Exception:
    COLUMN_MAP = {}

# ----------------- helpers -----------------
def _norm_text(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.category(c).startswith("M"))  # sin acentos
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _parse_date_col(series):
    if series is None: return pd.NaT
    return pd.to_datetime(series, errors="coerce", dayfirst=True, infer_datetime_format=True)

def _to_number(series):
    if series is None: return np.nan
    s = series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def _get(df, header_fallback: str, field: str):
    """
    Obtiene la columna según column_map.yaml; si falta, intenta por un sinónimo leve.
    """
    name = (COLUMN_MAP.get("MODELO_BOT", {}) or {}).get(field, "")
    if name and name in df.columns:
        return df[name]
    col = find_col(df, [header_fallback, header_fallback.replace("_", " "), header_fallback.upper()])
    return df.get(col)

# ----------------- vista MODELO_BOT normalizada -----------------
def _build_mb(df: pd.DataFrame) -> pd.DataFrame:
    MB = pd.DataFrame(index=df.index)

    # Identificadores y cliente/vehículo
    MB["OT"]       = _get(df, "OT", "ot")
    MB["PATENTE"]  = _get(df, "PATENTE", "patente")
    MB["MARCA"]    = _get(df, "MARCA", "marca")
    MB["MODELO"]   = _get(df, "MODELO", "modelo")
    MB["TIPO_CLIENTE"]   = _get(df, "TIPO CLIENTE", "tipo_cliente")
    MB["NOMBRE_CLIENTE"] = _get(df, "NOMBRE CLIENTE", "nombre_cliente")
    MB["TIPO_VEHICULO"]  = _get(df, "TIPO VEHÍCULO", "tipo_vehiculo")
    MB["SUCURSAL"]       = _get(df, "SUCURSAL", "sucursal")

    # Estados
    MB["ESTADO_SERVICIO"]    = _get(df, "ESTADO SERVICIO", "estado_servicio")
    MB["ESTADO_PRESUPUESTO"] = _get(df, "ESTADO PRESUPUESTO", "estado_presupuesto")

    # Fechas
    MB["FECHA_INGRESO_PLANTA"] = _parse_date_col(_get(df, "FECHA INGRESO PLANTA", "fecha_ingreso_planta"))
    MB["FECHA_SALIDA_PLANTA"]  = _parse_date_col(_get(df, "FECHA SALIDA PLANTA", "fecha_salida_planta"))
    MB["FECHA_INSPECCION"]     = _parse_date_col(_get(df, "FECHA INSPECCIÓN", "fecha_inspeccion"))
    MB["FECHA_RECEPCION"]      = _parse_date_col(_get(df, "FECHA RECEPCION", "fecha_recepcion"))
    MB["FECHA_ENTREGA"]        = _parse_date_col(_get(df, "FECHA ENTREGA", "fecha_entrega"))

    # Facturación
    MB["NUMERO_FACTURA"]     = _get(df, "NUMERO DE FACTURA", "numero_factura")
    MB["FECHA_FACTURACION"]  = _parse_date_col(_get(df, "FECHA DE FACTURACION", "fecha_facturacion"))
    MB["FECHA_PAGO_FACTURA"] = _parse_date_col(_get(df, "FECHA DE PAGO FACTURA", "fecha_pago_factura"))
    MB["FACTURADO_FLAG"]     = _get(df, "FACTURADO", "facturado_flag")  # SI / NO

    # Montos / KPIs
    MB["MONTO_NETO"]    = _to_number(_get(df, "MONTO PRINCIPAL NETO", "monto_neto"))
    MB["IVA_F"]         = _to_number(_get(df, "IVA PRINCIPAL [F]", "iva_f"))
    MB["MONTO_BRUTO_F"] = _to_number(_get(df, "MONTO PRINCIPAL BRUTO [F]", "monto_bruto_f"))

    MB["NUMERO_DIAS_EN_PLANTA"] = _to_number(_get(df, "NUMERO DE DIAS EN PLANTA", "numero_dias_en_planta"))
    MB["DIAS_EN_DOMINIO"]       = _to_number(_get(df, "DIAS EN DOMINIO", "dias_en_dominio"))
    MB["CANTIDAD_VEHICULO"]     = _to_number(_get(df, "CANTIDAD DE VEHICULO", "cantidad_vehiculo"))
    MB["DIAS_PAGO_FACTURA"]     = _to_number(_get(df, "DIAS DE PAGO DE FACTURA", "dias_pago_factura"))

    # --------- Booleans ESTRICTOS por bandera ----------
    estado_norm = MB["ESTADO_SERVICIO"].map(_norm_text)
    fact_norm   = MB["FACTURADO_FLAG"].map(_norm_text)

    # ENTREGADO: SOLO si ESTADO SERVICIO contiene "entreg"
    MB["entregado_bool"] = estado_norm.str.contains("entreg", na=False)

    # FACTURADO: SOLO si FACTURADO_FLAG ∈ truthy; NO FACTURADO si ∈ falsy o vacío.
    FACTURADO_TRUE  = {"si","sí","si.","si !","si ok","sí ok","facturado","facturada","emitida","emitido","ok","con factura"}
    FACTURADO_FALSE = {"no","no.","no !","pendiente","por facturar","sin factura","no emitida","no emitido","0","false",""}

    MB["facturado_bool"]    = fact_norm.isin(FACTURADO_TRUE)
    MB["no_facturado_bool"] = fact_norm.isin(FACTURADO_FALSE) | (fact_norm == "")

    # --------- Derivados útiles ----------
    MB["fecha_op"] = MB["FECHA_FACTURACION"].combine_first(MB["FECHA_ENTREGA"]).combine_first(MB["FECHA_RECEPCION"])

    if MB["NUMERO_DIAS_EN_PLANTA"].isna().all():
        fini = MB["FECHA_INGRESO_PLANTA"].combine_first(MB["FECHA_RECEPCION"])
        fend = MB["FECHA_SALIDA_PLANTA"].combine_first(MB["FECHA_ENTREGA"]).fillna(pd.Timestamp.today())
        MB["NUMERO_DIAS_EN_PLANTA"] = (fend - fini).dt.days

    MB["dias_desde_entrega"] = (pd.Timestamp.today() - MB["FECHA_ENTREGA"]).dt.days
    MB["id"] = MB["PATENTE"].replace("", np.nan).fillna(MB["OT"])

    # diagnóstico para pestaña Calibración
    MB["_estado_servicio_norm"] = estado_norm
    MB["_facturado_flag_norm"]  = fact_norm
    return MB

def _with_id_first(df):
    cols = list(df.columns)
    idc = next((c for c in cols if c.lower() in ("id","patente","placa","ot")), None)
    if idc and idc in df.columns:
        return df[[idc] + [c for c in cols if c != idc]]
    return df

# ----------------- SKILLS -----------------
def skill_entregados_sin_factura(df_raw, **f):
    MB = _build_mb(df_raw)
    t = MB[MB["entregado_bool"] & MB["no_facturado_bool"]].copy()

    if v:=f.get("cliente"):      t = t[ilike(t["NOMBRE_CLIENTE"], v)]
    if v:=f.get("tipo_cliente"): t = t[t["TIPO_CLIENTE"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("marca"):        t = t[t["MARCA"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("sucursal") and "SUCURSAL" in t.columns: t = t[t["SUCURSAL"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("desde"):        t = t[t["FECHA_ENTREGA"]>=pd.to_datetime(v, errors="coerce")]
    if v:=f.get("hasta"):        t = t[t["FECHA_ENTREGA"]<=pd.to_datetime(v, errors="coerce")]

    cols = [c for c in ["id","NOMBRE_CLIENTE","FECHA_ENTREGA","NUMERO_FACTURA","FECHA_FACTURACION",
                        "MONTO_NETO","NUMERO_DIAS_EN_PLANTA"] if c in t.columns]
    return _with_id_first(t[cols].sort_values("FECHA_ENTREGA", ascending=False).head(200)), None

def skill_entregados_facturados(df_raw, **f):
    MB = _build_mb(df_raw)
    t = MB[MB["entregado_bool"] & MB["facturado_bool"]].copy()

    if v:=f.get("cliente"):      t = t[ilike(t["NOMBRE_CLIENTE"], v)]
    if v:=f.get("tipo_cliente"): t = t[t["TIPO_CLIENTE"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("marca"):        t = t[t["MARCA"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("sucursal") and "SUCURSAL" in t.columns: t = t[t["SUCURSAL"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("desde"):        t = t[t["FECHA_ENTREGA"]>=pd.to_datetime(v, errors="coerce")]
    if v:=f.get("hasta"):        t = t[t["FECHA_ENTREGA"]<=pd.to_datetime(v, errors="coerce")]

    cols = [c for c in ["id","NOMBRE_CLIENTE","NUMERO_FACTURA","FECHA_FACTURACION",
                        "FECHA_ENTREGA","MONTO_NETO","NUMERO_DIAS_EN_PLANTA"] if c in t.columns]
    return _with_id_first(t[cols].sort_values("FECHA_ENTREGA", ascending=False).head(200)), None

def skill_top_en_taller(df_raw, topn=10, **f):
    MB = _build_mb(df_raw)
    t = MB[~MB["entregado_bool"]].copy()
    if v:=f.get("marca"):        t = t[t["MARCA"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("tipo_cliente"): t = t[t["TIPO_CLIENTE"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("sucursal") and "SUCURSAL" in t.columns: t = t[t["SUCURSAL"].astype(str).str.lower()==str(v).lower()]
    cols = [c for c in ["id","NOMBRE_CLIENTE","FECHA_RECEPCION","NUMERO_DIAS_EN_PLANTA"] if c in t.columns]
    t = t[cols].sort_values("NUMERO_DIAS_EN_PLANTA", ascending=False).head(int(topn))
    return _with_id_first(t), None

def skill_facturacion_por_mes_tipo(df_raw, mes:int, anio:int):
    MB = _build_mb(df_raw)
    fecha = pd.to_datetime(MB["fecha_op"], errors="coerce")
    t = MB[(fecha.dt.month==int(mes)) & (fecha.dt.year==int(anio))].copy()
    if t.empty:
        return pd.DataFrame(columns=["TIPO_CLIENTE","MONTO_NETO"]), None
    t = t.groupby("TIPO_CLIENTE", dropna=False, as_index=False)["MONTO_NETO"].sum().sort_values("MONTO_NETO", ascending=False)
    return t, None

def skill_entregas_proximos_dias_sin_factura(df_raw, horizonte_dias:int=7):
    MB = _build_mb(df_raw)
    hoy = pd.Timestamp.today()
    lim = hoy + pd.Timedelta(days=int(horizonte_dias))
    t = MB[(MB["entregado_bool"]) & (MB["no_facturado_bool"]) & MB["FECHA_ENTREGA"].between(hoy, lim)].copy()
    cols = [c for c in ["id","NOMBRE_CLIENTE","FECHA_ENTREGA","dias_desde_entrega"] if c in t.columns]
    t = t[cols].sort_values("FECHA_ENTREGA", ascending=True).head(200)
    return _with_id_first(t), None

def skill_sin_aprobacion(df_raw):
    MB = _build_mb(df_raw)
    t = MB[(~MB["entregado_bool"]) & (MB["no_facturado_bool"])].copy()
    cols = [c for c in ["id","NOMBRE_CLIENTE","FECHA_RECEPCION","NUMERO_DIAS_EN_PLANTA"] if c in t.columns]
    t = t[cols].sort_values("NUMERO_DIAS_EN_PLANTA", ascending=False).head(200)
    return _with_id_first(t), None

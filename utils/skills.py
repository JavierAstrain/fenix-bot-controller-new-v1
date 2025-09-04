# utils/skills.py
import os, yaml
import pandas as pd
import numpy as np
from .nlp import find_col, ilike

# ---- carga column_map.yaml si existe ----
COLUMN_MAP = {}
try:
    if os.path.exists("column_map.yaml"):
        with open("column_map.yaml", "r", encoding="utf-8") as f:
            COLUMN_MAP = yaml.safe_load(f) or {}
except Exception:
    COLUMN_MAP = {}

def _parse_date_col(series):
    if series is None: return pd.NaT
    return pd.to_datetime(series, errors="coerce", dayfirst=True, infer_datetime_format=True)

def _to_number(series):
    if series is None: return np.nan
    s = series.astype(str).str.replace(".","", regex=False).str.replace(",",".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def _pick(df, sheet_key: str, field_key: str, synonyms: list[str]):
    """Prioriza mapeo YAML; si no, heurística por sinónimos."""
    m = (COLUMN_MAP.get(sheet_key, {}) or {}).get(field_key)
    if m and m in df.columns: 
        return df.get(m)
    col = find_col(df, synonyms)
    return df.get(col)

# ----------------- MODELO_BOT -----------------
def _build_mb(df):
    out = pd.DataFrame(index=df.index)

    out["patente"] = _pick(df,"MODELO_BOT","patente",["patente","placa","matricula"])
    out["ot"]      = _pick(df,"MODELO_BOT","ot",["n° ot","no ot","ot","orden de trabajo"])
    out["cliente"] = _pick(df,"MODELO_BOT","cliente",["cliente","razon social","razón social"])

    out["fecha_recepcion"] = _parse_date_col(_pick(df,"MODELO_BOT","fecha_recepcion",["fecha recep","recepcion"]))
    out["fecha_entrega"]   = _parse_date_col(_pick(df,"MODELO_BOT","fecha_entrega",["fecha entrega","f. entre","entrega"]))
    out["factura_fecha"]   = _parse_date_col(_pick(df,"MODELO_BOT","factura_fecha",["fecha factura","fecha fact"]))

    out["factura_num"]   = _pick(df,"MODELO_BOT","factura_num",["folio factura","n° factura","factura"])
    out["facturado_flag"]= _pick(df,"MODELO_BOT","facturado_flag",["facturado","estado fact","estado facturacion"])
    out["estado_entrega"]= _pick(df,"MODELO_BOT","estado_entrega",["estado entrega","estado servicio","entrega"])
    out["tipo_cliente"]  = _pick(df,"MODELO_BOT","tipo_cliente",["tipo cliente","segmento"])
    out["marca"]         = _pick(df,"MODELO_BOT","marca",["marca"])
    out["sucursal"]      = _pick(df,"MODELO_BOT","sucursal",["sucursal","sede"])
    out["asesor"]        = _pick(df,"MODELO_BOT","asesor",["asesor","ejecutivo","vendedor"])
    out["monto"]         = _to_number(_pick(df,"MODELO_BOT","monto",["monto","venta neta","total","valor","importe"]))

    # Derivados robustos
    out["entregado_bool"] = (
        out["estado_entrega"].astype(str).str.lower().str.contains("entreg", na=False) |
        (~out["fecha_entrega"].isna())
    )

    out["no_facturado_bool"] = (
        (out["factura_num"].astype(str).str.strip()=="") |
        (out["factura_fecha"].isna()) |
        (out["facturado_flag"].astype(str).str.lower().isin(["no","pendiente","por facturar","sin factura","0","false"]))
    )

    out["dias_en_taller"]     = (out["fecha_entrega"].fillna(pd.Timestamp.today()) - out["fecha_recepcion"]).dt.days
    out["dias_desde_entrega"] = (pd.Timestamp.today() - out["fecha_entrega"]).dt.days
    out["fecha_op"]           = out["factura_fecha"].fillna(out["fecha_entrega"])

    out["id"] = out["patente"].replace("", np.nan).fillna(out["ot"])
    return out

# ----------------- FINANZAS -----------------
def _build_fin(df):
    out = pd.DataFrame(index=df.index)
    out["factura_num"] = _pick(df,"FINANZAS","factura_num",["folio","folio factura","n° factura"])
    out["proveedor"]   = _pick(df,"FINANZAS","proveedor",["proveedor","razon social","cliente"])
    out["estado_pago"] = _pick(df,"FINANZAS","estado_pago",["estado pago","pagado","pago","estado"])
    out["vencimiento"] = _parse_date_col(_pick(df,"FINANZAS","vencimiento",["vencimiento","fecha venc"]))
    out["monto"]       = _to_number(_pick(df,"FINANZAS","monto",["monto","total","neto","importe","valor"]))

    out["por_pagar_bool"] = (
        out["estado_pago"].astype(str).str.lower().isin(["pendiente","por pagar","no","impago","abierta","abierto","sin pago"]) |
        out["estado_pago"].isna() |
        (out["estado_pago"].astype(str).str.strip()=="")
    )
    out["dias_para_vencer"] = (out["vencimiento"] - pd.Timestamp.today()).dt.days
    return out

def _with_id_first(df):
    cols = list(df.columns)
    idc = next((c for c in cols if c.lower() in ("id","patente","placa","ot")), None)
    if idc and idc in df.columns:
        return df[[idc] + [c for c in cols if c != idc]]
    return df

# ---------- Skills ----------
def skill_entregados_sin_factura(df_raw, **f):
    df = _build_mb(df_raw)
    t = df[(df["entregado_bool"]==True) & (df["no_facturado_bool"]==True)].copy()
    # multifiltros
    if v:=f.get("cliente"):      t = t[ilike(t["cliente"], v)]
    if v:=f.get("tipo_cliente"): t = t[t["tipo_cliente"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("marca"):        t = t[t["marca"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("sucursal"):     t = t[t["sucursal"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("asesor"):       t = t[ilike(t["asesor"], v)]
    if v:=f.get("desde"):        t = t[t["fecha_entrega"]>=pd.to_datetime(v, errors="coerce")]
    if v:=f.get("hasta"):        t = t[t["fecha_entrega"]<=pd.to_datetime(v, errors="coerce")]
    t = t[["id","cliente","fecha_entrega","dias_desde_entrega"]].sort_values("fecha_entrega", ascending=False).head(200)
    return _with_id_first(t), None

def skill_facturas_por_pagar(df_fin_raw, horizonte_dias=7, proveedor=None):
    df = _build_fin(df_fin_raw)
    hoy = pd.Timestamp.today()
    t = df[(df["por_pagar_bool"]==True) & (df["vencimiento"].between(hoy, hoy+pd.Timedelta(days=int(horizonte_dias))))].copy()
    if proveedor: t = t[ilike(t["proveedor"], proveedor)]
    t = t[["factura_num","proveedor","vencimiento","monto"]].sort_values("vencimiento", ascending=True).head(200)
    return t, None

def skill_top_en_taller(df_raw, topn=10, **f):
    df = _build_mb(df_raw)
    t = df[df["entregado_bool"]==False].copy()
    if v:=f.get("marca"):        t = t[t["marca"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("asesor"):       t = t[ilike(t["asesor"], v)]
    if v:=f.get("tipo_cliente"): t = t[t["tipo_cliente"].astype(str).str.lower()==str(v).lower()]
    if v:=f.get("sucursal"):     t = t[t["sucursal"].astype(str).str.lower()==str(v).lower()]
    t = t[["id","cliente","fecha_recepcion","dias_en_taller"]].sort_values("dias_en_taller", ascending=False).head(int(topn))
    return _with_id_first(t), None

def skill_facturacion_por_mes_tipo(df_raw, mes:int, anio:int):
    df = _build_mb(df_raw)
    fecha = pd.to_datetime(df["fecha_op"], errors="coerce")
    t = df[(fecha.dt.month==int(mes)) & (fecha.dt.year==int(anio))].copy()
    if t.empty: return pd.DataFrame(columns=["tipo_cliente","monto"]), None
    t = t.groupby("tipo_cliente", dropna=False, as_index=False)["monto"].sum().sort_values("monto", ascending=False)
    return t, None

def skill_entregas_proximos_dias_sin_factura(df_raw, horizonte_dias:int=7):
    df = _build_mb(df_raw)
    hoy = pd.Timestamp.today()
    limite = hoy + pd.Timedelta(days=int(horizonte_dias))
    t = df[(df["entregado_bool"]==True) & (df["no_facturado_bool"]==True) & df["fecha_entrega"].between(hoy, limite)].copy()
    t = t[["id","cliente","fecha_entrega","dias_desde_entrega"]].sort_values("fecha_entrega", ascending=True).head(200)
    return _with_id_first(t), None

def skill_sin_aprobacion(df_raw):
    df = _build_mb(df_raw)
    t = df[(df["entregado_bool"]==False) & (df["no_facturado_bool"]==True)].copy()
    t = t[["id","cliente","fecha_recepcion","dias_en_taller"]].sort_values("dias_en_taller", ascending=False).head(200)
    return _with_id_first(t), None

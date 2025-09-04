import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from .nlp import find_col, ilike

# --- Helpers semánticos (pandas) ---

def _parse_date_col(series):
    if series is None: return pd.NaT
    return pd.to_datetime(series, errors="coerce", dayfirst=True, infer_datetime_format=True)

def _to_number(series):
    if series is None: return np.nan
    s = series.astype(str).str.replace(".","", regex=False).str.replace(",",".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def _build_mb(df):
    out = pd.DataFrame(index=df.index)
    out["patente"] = df.get(find_col(df, ["patente","placa"]), "")
    out["ot"] = df.get(find_col(df, ["# ot","n° ot","ot","orden de trabajo","orden trabajo"]), "")
    out["cliente"] = df.get(find_col(df, ["cliente","razon social","razón social","r. social"]), "")
    out["fecha_recepcion"] = _parse_date_col(df.get(find_col(df, ["fecha recep","f. recep","recepcion"])))
    out["fecha_entrega"]   = _parse_date_col(df.get(find_col(df, ["fecha entrega","f. entre","entrega"])))
    out["factura_fecha"]   = _parse_date_col(df.get(find_col(df, ["fecha fact","fecha de fact","fecha factura"])))
    out["factura_num"]     = df.get(find_col(df, ["n° factura","numero factura","nro factura","folio factura","factura"]), "")
    out["facturado_flag"]  = df.get(find_col(df, ["facturado","estado fact","facturacion"]), "")
    out["estado_entrega"]  = df.get(find_col(df, ["estado entrega","entrega","estado servicio"]), "")
    out["tipo_cliente"]    = df.get(find_col(df, ["tipo cliente","segmento","categoria cliente"]), "")
    out["marca"]           = df.get(find_col(df, ["marca"]), "")
    out["sucursal"]        = df.get(find_col(df, ["sucursal"]), "")
    out["asesor"]          = df.get(find_col(df, ["asesor"]), "")
    out["monto"]           = _to_number(df.get(find_col(df, ["monto","total","neto","bruto","venta","valor","importe"])))
    # Derivados
    out["entregado_bool"] = (out["estado_entrega"].astype(str).str.lower().str.contains("entregad")) | \
                            (~out["fecha_entrega"].isna() & (out["fecha_entrega"] <= pd.Timestamp.today()))
    out["no_facturado_bool"] = (
        (out["factura_num"].astype(str).str.strip()=="") |
        (out["factura_fecha"].isna()) |
        (out["facturado_flag"].astype(str).str.lower().isin(["no","pendiente","por facturar","sin factura","0","false"]))
    )
    out["dias_en_taller"] = (out["fecha_entrega"].fillna(pd.Timestamp.today()) - out["fecha_recepcion"]).dt.days
    out["dias_desde_entrega"] = (pd.Timestamp.today() - out["fecha_entrega"]).dt.days
    out["fecha_op"] = out["factura_fecha"].fillna(out["fecha_entrega"])
    return out

def _build_fin(df):
    out = pd.DataFrame(index=df.index)
    out["factura_num"] = df.get(find_col(df, ["n° factura","folio","folio factura","numero factura","nro factura"]), "")
    out["proveedor"]   = df.get(find_col(df, ["proveedor","nombre proveedor","razon social","cliente"]), "")
    out["estado_pago"] = df.get(find_col(df, ["estado pago","pagado","pago","estado"]), "")
    out["vencimiento"] = _parse_date_col(df.get(find_col(df, ["vencimiento","f. venc","fecha venc","fecha de venc"])))
    out["monto"]       = _to_number(df.get(find_col(df, ["monto","total","neto","importe","valor"])))
    out["por_pagar_bool"] = (
        out["estado_pago"].astype(str).str.lower().isin(["pendiente","por pagar","no","impago","abierta","abierto","sin pago"])) | \
        (out["estado_pago"].isna()) | (out["estado_pago"].astype(str).str.strip()=="")
    out["dias_para_vencer"] = (out["vencimiento"] - pd.Timestamp.today()).dt.days
    return out

def _id_first(df, prefer=("patente","ot")):
    if df is None or df.empty: return df
    cols = list(df.columns)
    idc = None
    for p in prefer:
        for c in cols:
            if c.lower() == p:
                idc = c; break
        if idc: break
    if not idc:
        for c in cols:
            if "patente" in c.lower() or c.lower() in ("ot","orden de trabajo"):
                idc = c; break
    if idc and idc in df.columns:
        rest = [c for c in cols if c != idc]
        return df[[idc]+rest].drop_duplicates(subset=[idc])
    return df

# --- Skills deterministas (con multifiltros) ---

def skill_entregados_sin_factura(df_raw, cliente=None, tipo_cliente=None, marca=None, sucursal=None, asesor=None, fecha_desde=None, fecha_hasta=None):
    try:
        df = _build_mb(df_raw)
        out = df[(df["entregado_bool"]==True) & (df["no_facturado_bool"]==True)].copy()
        if cliente: out = out[ilike(out["cliente"], cliente)]
        if tipo_cliente: out = out[out["tipo_cliente"].astype(str).str.lower()==tipo_cliente.lower()]
        if marca: out = out[out["marca"].astype(str).str.lower()==marca.lower()]
        if sucursal: out = out[out["sucursal"].astype(str).str.lower()==sucursal.lower()]
        if asesor: out = out[ilike(out["asesor"], asesor)]
        if fecha_desde and fecha_hasta:
            d1 = pd.to_datetime(fecha_desde, errors="coerce")
            d2 = pd.to_datetime(fecha_hasta, errors="coerce")
            out = out[(out["fecha_entrega"]>=d1) & (out["fecha_entrega"]<=d2)]
        res = out.loc[:, ["patente","ot","cliente","fecha_entrega","dias_desde_entrega"]]
        res["id"] = res["patente"].replace("", np.nan).fillna(res["ot"])
        res = res[["id","cliente","fecha_entrega","dias_desde_entrega"]]
        res = res.sort_values("fecha_entrega", ascending=False)
        return _id_first(res, ("id",)), None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

def skill_facturas_por_pagar(df_raw, horizonte_dias=7, proveedor=None):
    try:
        df = _build_fin(df_raw)
        today = pd.Timestamp.today().normalize()
        out = df[(df["por_pagar_bool"]==True) & (~df["vencimiento"].isna())].copy()
        out = out[(out["vencimiento"]>=today) & (out["vencimiento"]<=today + pd.Timedelta(days=int(horizonte_dias)))]
        if proveedor: out = out[ilike(out["proveedor"], proveedor)]
        res = out.loc[:, ["factura_num","proveedor","vencimiento","monto"]].sort_values("vencimiento", ascending=True)
        return res, None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

def skill_top_en_taller(df_raw, top_n=10, marca=None, asesor=None, tipo_cliente=None, sucursal=None):
    try:
        df = _build_mb(df_raw)
        out = df[df["entregado_bool"]==False].copy()
        if marca: out = out[out["marca"].astype(str).str.lower()==marca.lower()]
        if asesor: out = out[ilike(out["asesor"], asesor)]
        if tipo_cliente: out = out[out["tipo_cliente"].astype(str).str.lower()==tipo_cliente.lower()]
        if sucursal: out = out[out["sucursal"].astype(str).str.lower()==sucursal.lower()]
        res = out.loc[:, ["patente","ot","cliente","fecha_recepcion","dias_en_taller"]]
        res["id"] = res["patente"].replace("", np.nan).fillna(res["ot"])
        res = res[["id","cliente","fecha_recepcion","dias_en_taller"]]
        res = res.sort_values("dias_en_taller", ascending=False).head(int(top_n))
        res = res.rename(columns={"dias_en_taller":"Días en taller"})
        return _id_first(res, ("id",)), None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

def skill_facturacion_por_mes_tipo(df_raw, mes: int, anio: int|None=None):
    try:
        df = _build_mb(df_raw)
        base = df[df["fecha_op"].notna()].copy()
        base = base[base["fecha_op"].dt.month == int(mes)]
        if anio:
            base = base[base["fecha_op"].dt.year == int(anio)]
        res = base.groupby("tipo_cliente", dropna=False)["monto"].sum().reset_index().fillna({"tipo_cliente": "(Sin tipo)"})
        res = res.sort_values("monto", ascending=False).rename(columns={"tipo_cliente":"Tipo de cliente"})
        return res, None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

def skill_entregas_proximos_dias_sin_factura(df_raw, horizonte_dias=7):
    try:
        df = _build_mb(df_raw)
        today = pd.Timestamp.today().normalize()
        out = df[(df["no_facturado_bool"]==True) & (df["fecha_entrega"].notna())].copy()
        out = out[(out["fecha_entrega"]>=today) & (out["fecha_entrega"]<=today + pd.Timedelta(days=int(horizonte_dias)))]
        res = out.loc[:, ["patente","ot","cliente","fecha_entrega"]]
        res["id"] = res["patente"].replace("", np.nan).fillna(res["ot"])
        res = res[["id","cliente","fecha_entrega"]].sort_values("fecha_entrega", ascending=True)
        return _id_first(res, ("id",)), None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

def skill_sin_aprobacion(df_raw):
    try:
        df = _build_mb(df_raw)
        # Heurística: "sin aprobación" = presupuesto enviado y no perdido (si existen columnas relacionadas),
        # aquí usamos no entregado como proxy mínimo
        out = df[(df["entregado_bool"]==False)].copy()
        res = out.loc[:, ["patente","ot","cliente","fecha_recepcion","dias_en_taller"]]
        res["id"] = res["patente"].replace("", np.nan).fillna(res["ot"])
        res = res[["id","cliente","fecha_recepcion","dias_en_taller"]].sort_values("dias_en_taller", ascending=False)
        return _id_first(res, ("id",)), None
    except Exception as e:
        return pd.DataFrame(), f"Error: {e}"

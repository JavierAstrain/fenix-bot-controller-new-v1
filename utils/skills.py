# utils/skills.py
import pandas as pd
import numpy as np
from datetime import datetime
from .nlp import find_col, ilike

# ---------- Helpers de normalización ----------
def _parse_date_col(series):
    if series is None:
        return pd.NaT
    return pd.to_datetime(series, errors="coerce", dayfirst=True, infer_datetime_format=True)

def _to_number(series):
    if series is None:
        return np.nan
    s = series.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

# ---------- Construcción de vistas MB / FIN ----------
def _build_mb(df):
    out = pd.DataFrame(index=df.index)

    out["patente"] = df.get(find_col(df, ["patente", "placa", "placa patente"]), "")
    out["ot"] = df.get(find_col(df, ["# ot", "n° ot", "ot", "orden de trabajo", "orden trabajo"]), "")
    out["cliente"] = df.get(find_col(df, ["cliente", "razon social", "razón social", "r. social"]), "")

    out["fecha_recepcion"] = _parse_date_col(df.get(find_col(df, [
        "fecha recep", "f. recep", "recepcion", "fecha de recep"
    ])))
    out["fecha_entrega"] = _parse_date_col(df.get(find_col(df, [
        "fecha entrega", "f. entre", "entrega", "fecha de entrega"
    ])))
    out["factura_fecha"] = _parse_date_col(df.get(find_col(df, [
        "fecha fact", "fecha de fact", "fecha factura"
    ])))

    out["factura_num"] = df.get(find_col(df, ["n° factura", "numero factura", "nro factura", "folio factura", "factura"]), "")
    out["facturado_flag"] = df.get(find_col(df, ["facturado", "estado fact", "estado facturacion", "facturacion"]), "")
    out["estado_entrega"] = df.get(find_col(df, ["estado entrega", "entrega", "estado servicio", "estado del servicio"]), "")
    out["tipo_cliente"] = df.get(find_col(df, ["tipo cliente", "segmento", "categoria cliente", "clase cliente"]), "")
    out["marca"] = df.get(find_col(df, ["marca"]), "")
    out["sucursal"] = df.get(find_col(df, ["sucursal", "sede"]), "")
    out["asesor"] = df.get(find_col(df, ["asesor", "ejecutivo", "vendedor"]), "")
    out["monto"] = _to_number(df.get(find_col(df, ["monto", "total", "monto neto", "total neto", "venta neta", "valor", "importe"])))

    # Derivados
    out["entregado_bool"] = (
        out["estado_entrega"].astype(str).str.lower().str.contains("entreg")
        | (~out["fecha_entrega"].isna() & (out["fecha_entrega"] <= pd.Timestamp.today()))
    )

    out["no_facturado_bool"] = (
        (out["factura_num"].astype(str).str.strip() == "")
        | (out["factura_fecha"].isna())
        | (out["facturado_flag"].astype(str).str.lower().isin(
            ["no", "pendiente", "por facturar", "sin factura", "0", "false"]
        ))
    )

    out["dias_en_taller"] = (out["fecha_entrega"].fillna(pd.Timestamp.today()) - out["fecha_recepcion"]).dt.days
    out["dias_desde_entrega"] = (pd.Timestamp.today() - out["fecha_entrega"]).dt.days
    out["fecha_op"] = out["factura_fecha"].fillna(out["fecha_entrega"])

    # ID visible primero
    out["id"] = out["patente"].replace("", np.nan).fillna(out["ot"])
    return out

def _build_fin(df):
    out = pd.DataFrame(index=df.index)
    out["factura_num"] = df.get(find_col(df, ["n° factura", "folio", "folio factura", "numero factura", "nro factura"]), "")
    out["proveedor"] = df.get(find_col(df, ["proveedor", "nombre proveedor", "razon social", "cliente"]), "")
    out["estado_pago"] = df.get(find_col(df, ["estado pago", "estado de pago", "pagado", "pago", "estado"]), "")
    out["vencimiento"] = _parse_date_col(df.get(find_col(df, ["vencimiento", "f. venc", "fecha venc", "fecha de vencimiento"])))
    out["monto"] = _to_number(df.get(find_col(df, ["monto", "total", "neto", "importe", "valor"])))

    out["por_pagar_bool"] = (
        out["estado_pago"].astype(str).str.lower().isin(["pendiente", "por pagar", "no", "impago", "abierta", "abierto", "sin pago"])
        | out["estado_pago"].isna()
        | (out["estado_pago"].astype(str).str.strip() == "")
    )
    out["dias_para_vencer"] = (out["vencimiento"] - pd.Timestamp.today()).dt.days
    return out

def _with_id_first(df):
    cols = list(df.columns)
    idc = next((c for c in cols if c.lower() in ("id", "patente", "placa", "ot")), None)
    if idc and idc in df.columns:
        return df[[idc] + [c for c in cols if c != idc]]
    return df

# ---------- Skills deterministas con multifiltros ----------
def skill_entregados_sin_factura(df_raw, cliente=None, tipo_cliente=None, marca=None, sucursal=None, asesor=None, desde=None, hasta=None):
    df = _build_mb(df_raw)
    out = df[(df["entregado_bool"] == True) & (df["no_facturado_bool"] == True)].copy()

    if cliente:      out = out[ilike(out["cliente"], cliente)]
    if tipo_cliente: out = out[out["tipo_cliente"].astype(str).str.lower() == str(tipo_cliente).lower()]
    if marca:        out = out[out["marca"].astype(str).str.lower() == str(marca).lower()]
    if sucursal:     out = out[out["sucursal"].astype(str).str.lower() == str(sucursal).lower()]
    if asesor:       out = out[ilike(out["asesor"], asesor)]
    if desde:        out = out[out["fecha_entrega"] >= pd.to_datetime(desde, errors="coerce")]
    if hasta:        out = out[out["fecha_entrega"] <= pd.to_datetime(hasta, errors="coerce")]

    out = out[["id", "cliente", "fecha_entrega", "dias_desde_entrega"]].sort_values("fecha_entrega", ascending=False).head(200)
    return _with_id_first(out), None

def skill_facturas_por_pagar(df_fin_raw, horizonte_dias=7, proveedor=None):
    df = _build_fin(df_fin_raw)
    out = df[df["por_pagar_bool"] == True].copy()
    hoy = pd.Timestamp.today()
    out = out[(out["vencimiento"] >= hoy) & (out["vencimiento"] <= hoy + pd.Timedelta(days=int(horizonte_dias)))]

    if proveedor:
        out = out[ilike(out["proveedor"], proveedor)]

    out = out[["factura_num", "proveedor", "vencimiento", "monto"]].sort_values("vencimiento", ascending=True).head(200)
    return out, None

def skill_top_en_taller(df_raw, topn=10, marca=None, asesor=None, tipo_cliente=None, sucursal=None):
    df = _build_mb(df_raw)
    out = df[df["entregado_bool"] == False].copy()

    if marca:        out = out[out["marca"].astype(str).str.lower() == str(marca).lower()]
    if asesor:       out = out[ilike(out["asesor"], asesor)]
    if tipo_cliente: out = out[out["tipo_cliente"].astype(str).str.lower() == str(tipo_cliente).lower()]
    if sucursal:     out = out[out["sucursal"].astype(str).str.lower() == str(sucursal).lower()]

    out = out[["id", "cliente", "fecha_recepcion", "dias_en_taller"]].sort_values("dias_en_taller", ascending=False).head(int(topn))
    return _with_id_first(out), None

def skill_facturacion_por_mes_tipo(df_raw, mes: int, anio: int):
    df = _build_mb(df_raw)
    fecha = pd.to_datetime(df["fecha_op"], errors="coerce")
    out = df[(fecha.dt.month == int(mes)) & (fecha.dt.year == int(anio))].copy()

    if out.empty:
        return pd.DataFrame(columns=["tipo_cliente", "monto"]), None

    out = out.groupby("tipo_cliente", dropna=False, as_index=False)["monto"].sum().sort_values("monto", ascending=False)
    return out, None

def skill_entregas_proximos_dias_sin_factura(df_raw, horizonte_dias: int = 7):
    df = _build_mb(df_raw)
    out = df[(df["entregado_bool"] == True) & (df["no_facturado_bool"] == True)].copy()
    hoy = pd.Timestamp.today()
    limite = hoy + pd.Timedelta(days=int(horizonte_dias))
    out = out[(out["fecha_entrega"] >= hoy) & (out["fecha_entrega"] <= limite)]

    out = out[["id", "cliente", "fecha_entrega", "dias_desde_entrega"]].sort_values("fecha_entrega", ascending=True).head(200)
    return _with_id_first(out), None

def skill_sin_aprobacion(df_raw):
    df = _build_mb(df_raw)
    # Proxy: no entregados y no facturados
    out = df[(df["entregado_bool"] == False) & (df["no_facturado_bool"] == True)].copy()
    out = out[["id", "cliente", "fecha_recepcion", "dias_en_taller"]].sort_values("dias_en_taller", ascending=False).head(200)
    return _with_id_first(out), None

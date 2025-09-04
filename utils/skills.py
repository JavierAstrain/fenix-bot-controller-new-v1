import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from .nlp import norm, find_col

def entregados_sin_factura(df: pd.DataFrame):
    estado = find_col(df, ["estado entrega","estado de entrega","entrega","estado servicio","estado del servicio"])
    if not estado:
        return pd.DataFrame(), "No se encontró la columna de estado de entrega."
    entregado = df[estado].astype(str).str.lower().str.contains("entregad", na=False)

    numf = find_col(df, ["n° factura","n°factura","numero factura","num factura","nro factura","folio factura","factura"])
    fecf = find_col(df, ["fecha facturacion","fecha de facturacion","fecha factura"])
    flag = find_col(df, ["facturado","estado facturacion","estado de facturacion"])

    conds = []
    if numf:
        v = df[numf].astype(str).str.strip().str.lower()
        conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if fecf:
        v = df[fecf].astype(str).str.strip().str.lower()
        conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if flag:
        v = df[flag].astype(str).str.strip().str.lower()
        conds.append(v.isin({"no","pendiente","por facturar","sin factura","0"}))

    if not conds:
        return pd.DataFrame(), "No hay columnas de facturación para evaluar."

    nofac = np.logical_and.reduce(conds)
    out = df[entregado & nofac].copy()

    fec_ent = find_col(df, ["fecha entrega","fecha de entrega"])
    if fec_ent:
        fechas = pd.to_datetime(out[fec_ent], errors="coerce", dayfirst=True)
        out["Días desde la entrega"] = (pd.Timestamp(datetime.now().date()) - fechas).dt.days
        out = out.sort_values(by=fec_ent, key=lambda s: pd.to_datetime(s, errors="coerce", dayfirst=True))

    prefer = ["# ot","n° ot","ot","orden de trabajo","patente","placa","nombre cliente","cliente","marca","modelo"]
    cols = []
    for k in prefer + ([fec_ent] if fec_ent else []) + [estado, numf, fecf, flag]:
        if k and k in out.columns and k not in cols:
            cols.append(k)
    if "Días desde la entrega" in out.columns: cols.append("Días desde la entrega")
    if not cols and not out.empty: cols = list(out.columns)[:12]
    return out[cols] if not out.empty else out, None

def facturas_por_pagar(df: pd.DataFrame, horizonte_dias: int = 7):
    venc = find_col(df, ["fecha vencimiento","vencimiento","fecha de vencimiento","f. vencimiento"])
    estado = find_col(df, ["estado pago","estado de pago","pagado","pago"])
    monto = find_col(df, ["monto","total","monto neto","monto principal neto","importe","valor"])
    proveedor = find_col(df, ["proveedor","nombre proveedor","razon social","cliente"])
    numf = find_col(df, ["n° factura","folio","folio factura","numero factura","nro factura"])

    if not venc:
        return pd.DataFrame(), "No encontré la columna de vencimiento en FINANZAS."
    fechas = pd.to_datetime(df[venc], errors="coerce", dayfirst=True)
    hoy = pd.Timestamp(datetime.now().date())
    lim = hoy + pd.Timedelta(days=horizonte_dias)

    if estado:
        pendiente = ~df[estado].astype(str).str.lower().isin({"pagado","cancelado","ok","pagada"})
    else:
        pendiente = pd.Series(True, index=df.index)

    filt = pendiente & fechas.between(hoy, lim, inclusive="both")
    out = df.loc[filt].copy()

    cols = [c for c in [numf, proveedor, venc, monto, estado] if c]
    if not cols: cols = list(out.columns)[:8]
    out = out[cols]
    out = out.sort_values(by=venc, key=lambda s: pd.to_datetime(s, errors="coerce", dayfirst=True))
    return out, None

def top_en_taller(df: pd.DataFrame, top_n: int = 10):
    frec = find_col(df, ["fecha recepcion","fecha de recepcion"])
    fent = find_col(df, ["fecha entrega","fecha de entrega"])
    estado = find_col(df, ["estado entrega","entrega","estado servicio","estado del servicio"])
    if not frec:
        return pd.DataFrame(), "Falta fecha de recepción."
    rec = pd.to_datetime(df[frec], errors="coerce", dayfirst=True)
    hoy = pd.Timestamp(datetime.now().date())

    if fent:
        ent = pd.to_datetime(df[fent], errors="coerce", dayfirst=True)
        no_entregado = ent.isna() | (ent > hoy)
    elif estado:
        no_entregado = ~df[estado].astype(str).str.lower().str.contains("entregad", na=False)
    else:
        no_entregado = pd.Series(True, index=df.index)

    dias = (hoy - rec).dt.days
    out = df.loc[no_entregado].copy()
    out["Días en taller"] = dias[no_entregado]
    out = out.sort_values("Días en taller", ascending=False).head(top_n)

    prefer = ["# ot","n° ot","ot","orden de trabajo","patente","placa","nombre cliente","cliente","marca","modelo", frec, estado]
    cols = [c for c in prefer if c in out.columns]
    if "Días en taller" in out.columns: cols.append("Días en taller")
    if not cols: cols = list(out.columns)[:8]
    return out[cols], None

def facturacion_por_mes_tipo(df: pd.DataFrame, mes: int, anio: int):
    fecf = find_col(df, ["fecha facturacion","fecha de facturacion","fecha factura"])
    fent = find_col(df, ["fecha entrega","fecha de entrega"])
    fecha = fecf or fent
    if not fecha:
        return pd.DataFrame(), "No hay fecha de facturación/entrega para agrupar."
    tipo = find_col(df, ["tipo cliente","segmento","categoria cliente","clase cliente"])
    monto = find_col(df, ["monto","monto neto","venta neta","total neto","monto principal neto","total"])
    if not tipo or not monto:
        return pd.DataFrame(), "Faltan columnas de tipo de cliente o monto."

    f = pd.to_datetime(df[fecha], errors="coerce", dayfirst=True)
    sel = (f.dt.month == mes) & (f.dt.year == anio)
    s = pd.to_numeric(df[monto].astype(str).str.replace(".","").str.replace(",","."), errors="coerce")
    out = (pd.DataFrame({tipo: df[tipo], "monto": s, "sel": sel})
           .query("sel")
           .groupby(tipo, dropna=False)["monto"].sum()
           .reset_index()
           .sort_values("monto", ascending=False))
    return out, None

def entregas_proximos_dias_sin_factura(df: pd.DataFrame, horizonte_dias: int = 7):
    fent = find_col(df, ["fecha entrega","fecha de entrega"])
    if not fent:
        return pd.DataFrame(), "No encuentro fecha de entrega."
    fechas = pd.to_datetime(df[fent], errors="coerce", dayfirst=True)
    hoy = pd.Timestamp(datetime.now().date())
    lim = hoy + pd.Timedelta(days=horizonte_dias)

    numf = find_col(df, ["n° factura","n°factura","numero factura","num factura","nro factura","folio factura","factura"])
    fecf = find_col(df, ["fecha facturacion","fecha de facturacion","fecha factura"])
    flag = find_col(df, ["facturado","estado facturacion","estado de facturacion"])

    conds = []
    if numf:
        v = df[numf].astype(str).str.strip().str.lower()
        conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if fecf:
        v = df[fecf].astype(str).str.strip().str.lower()
        conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if flag:
        v = df[flag].astype(str).str.strip().str.lower()
        conds.append(v.isin({"no","pendiente","por facturar","sin factura","0"}))
    nofac = (pd.Series(True, index=df.index) if not conds else pd.Series(conds[0], index=df.index))
    for c in conds[1:]:
        nofac = nofac & c

    out = df.loc[nofac & fechas.between(hoy, lim, inclusive="both")].copy()
    out = out.sort_values(by=fent, key=lambda s: pd.to_datetime(s, errors="coerce", dayfirst=True))

    prefer = ["# ot","n° ot","ot","orden de trabajo","patente","placa","nombre cliente","cliente","marca","modelo", fent]
    cols = [c for c in prefer if c in out.columns]
    return out[cols] if not out.empty else out, None

def sin_aprobacion(df: pd.DataFrame):
    est_pres = find_col(df, ["estado presupuesto","presupuesto","estado del presupuesto"])
    estado = find_col(df, ["estado servicio","estado del servicio","estado entrega","entrega"])
    if not est_pres and not estado:
        return pd.DataFrame(), "No encuentro columna de estado/presupuesto."
    cond_pres = pd.Series(True, index=df.index)
    if est_pres:
        v = df[est_pres].astype(str).str.lower()
        cond_pres = v.str.contains("enviad", na=False) & ~v.str.contains("perdid", na=False)

    numf = find_col(df, ["n° factura","folio factura","numero factura","nro factura"])
    fecf = find_col(df, ["fecha facturacion","fecha de facturacion","fecha factura"])
    flag = find_col(df, ["facturado","estado facturacion","estado de facturacion"])
    conds = []
    if numf:
        v = df[numf].astype(str).str.strip().str.lower(); conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if fecf:
        v = df[fecf].astype(str).str.strip().str.lower(); conds.append(v.eq("") | v.isin({"nan","none","-"}))
    if flag:
        v = df[flag].astype(str).str.strip().str.lower(); conds.append(v.isin({"no","pendiente","por facturar","sin factura","0"}))
    cond_nofac = pd.Series(True, index=df.index) if not conds else conds[0]
    for c in conds[1:]:
        cond_nofac = cond_nofac & c

    out = df.loc[cond_pres & cond_nofac].copy()
    prefer = ["# ot","n° ot","ot","orden de trabajo","patente","placa","nombre cliente","cliente","marca","modelo", est_pres]
    cols = [c for c in prefer if c in out.columns]
    return out[cols] if not out.empty else out, None

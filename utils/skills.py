# ... dentro de _build_mb(df) después de crear las columnas MB[...] ...

estado_norm = MB["ESTADO_SERVICIO"].map(_norm_text)
fact_norm   = MB["FACTURADO_FLAG"].map(_norm_text)

# --- MODO ESTRICTO POR BANDERA ---
# Entregado: SOLO si el ESTADO SERVICIO contiene "entreg"
MB["entregado_bool"] = estado_norm.str.contains("entreg", na=False)

# Facturado: SOLO si FACTURADO == "SI" (y variantes)
FACTURADO_TRUE  = {"si","sí","si.","si !","si ok","sí ok","facturado","facturada","emitida","emitido","ok","con factura"}
FACTURADO_FALSE = {"no","no.","no !","pendiente","por facturar","sin factura","no emitida","no emitido","0","false",""}

MB["facturado_bool"]    = fact_norm.isin(FACTURADO_TRUE)
MB["no_facturado_bool"] = fact_norm.isin(FACTURADO_FALSE) | (fact_norm == "")

# Derivados que ya tenías (no cambian)
MB["fecha_op"] = MB["FECHA_FACTURACION"].combine_first(MB["FECHA_ENTREGA"]).combine_first(MB["FECHA_RECEPCION"])
if MB["NUMERO_DIAS_EN_PLANTA"].isna().all():
    fini = MB["FECHA_INGRESO_PLANTA"].combine_first(MB["FECHA_RECEPCION"])
    fend = MB["FECHA_SALIDA_PLANTA"].combine_first(MB["FECHA_ENTREGA"]).fillna(pd.Timestamp.today())
    MB["NUMERO_DIAS_EN_PLANTA"] = (fend - fini).dt.days

MB["dias_desde_entrega"] = (pd.Timestamp.today() - MB["FECHA_ENTREGA"]).dt.days
MB["id"] = MB["PATENTE"].replace("", np.nan).fillna(MB["OT"])

# (opcional para diagnosticar en la pestaña Calibración)
MB["_estado_servicio_norm"] = estado_norm
MB["_facturado_flag_norm"]  = fact_norm

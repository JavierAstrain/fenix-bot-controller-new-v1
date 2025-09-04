import re
import pandas as pd
from .nlp import find_col

# --- sinónimos canónicos (MODELO_BOT) ---
MB_KEYS = {
    "patente": ["patente","placa","placa patente"],
    "ot": ["# ot","n° ot","ot","orden de trabajo","orden trabajo"],
    "cliente": ["cliente","nombre cliente","razon social","r. social"],
    "estado_entrega": ["estado entrega","entrega","estado servicio","estado del servicio"],
    "fecha_recepcion": ["fecha recepcion","fecha de recepcion"],
    "fecha_entrega": ["fecha entrega","fecha de entrega"],
    "factura_num": ["n° factura","numero factura","nro factura","folio factura","factura"],
    "factura_fecha": ["fecha facturacion","fecha de facturacion","fecha factura"],
    "facturado_flag": ["facturado","estado facturacion","estado de facturacion"],
    "tipo_cliente": ["tipo cliente","segmento","categoria cliente","clase cliente"],
    "monto": ["monto","total","monto neto","total neto","venta neta","monto principal neto","valor"],
    "presupuesto_estado": ["estado presupuesto","presupuesto","estado del presupuesto"],
}

# --- sinónimos canónicos (FINANZAS) ---
FIN_KEYS = {
    "factura_num": ["n° factura","folio","folio factura","numero factura","nro factura"],
    "proveedor": ["proveedor","nombre proveedor","razon social","cliente"],
    "vencimiento": ["fecha vencimiento","vencimiento","fecha de vencimiento","f. vencimiento"],
    "estado_pago": ["estado pago","estado de pago","pagado","pago"],
    "monto": ["monto","total","monto neto","monto principal neto","importe","valor"],
}

def map_cols(df, keys):
    m = {}
    for k, syns in keys.items():
        m[k] = find_col(df, syns)
    return m

def _q(col: str | None) -> str:
    # quote ident si tiene espacios o caracteres raros
    if not col:
        return "NULL"
    return f'"{col}"' if re.search(r'[^a-zA-Z0-9_]', col) else col

# --- helpers SQL ---

def parse_date_expr(col_sql: str) -> str:
    """
    Devuelve una expresión SQL (DuckDB) que intenta parsear varias variantes de fecha comunes:
    DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, YYYY/MM/DD, MM/DD/YYYY.
    Resultado final: DATE o NULL.
    """
    # strptime -> TIMESTAMP; lo casteamos a DATE
    # TRY_STRPTIME devuelve NULL si falla
    tries = [
        f"TRY_STRPTIME({col_sql}, '%d/%m/%Y')",
        f"TRY_STRPTIME({col_sql}, '%d-%m-%Y')",
        f"TRY_STRPTIME({col_sql}, '%Y-%m-%d')",
        f"TRY_STRPTIME({col_sql}, '%Y/%m/%d')",
        f"TRY_STRPTIME({col_sql}, '%m/%d/%Y')",
    ]
    return "CAST(COALESCE(" + ", ".join(tries) + ") AS DATE)"

def parse_number_expr(col_sql: str) -> str:
    """
    Convierte números que vienen como texto con separador de miles '.' y decimales ',' o '.'.
    """
    # Primero quitamos separadores de miles (.) y normalizamos coma a punto.
    return f"TRY_CAST(REPLACE(REPLACE({col_sql}, '.', ''), ',', '.') AS DOUBLE)"

def build_mb_view_sql(src_name: str, df: pd.DataFrame) -> tuple[str, str]:
    m = map_cols(df, MB_KEYS)

    # columnas base
    selects = []

    # Textuales directas
    for ckey, alias in [
        ("patente","patente"),("ot","ot"),("cliente","cliente"),
        ("factura_num","factura_num"),("facturado_flag","facturado_flag"),
        ("tipo_cliente","tipo_cliente"),("estado_entrega","estado_entrega"),
        ("presupuesto_estado","presupuesto_estado"),
    ]:
        col = m.get(ckey)
        selects.append(f"{_q(col)} AS {alias}" if col else f"NULL AS {alias}")

    # Fechas con parse robusto
    for ckey, alias in [
        ("fecha_recepcion","fecha_recepcion"),
        ("fecha_entrega","fecha_entrega"),
        ("factura_fecha","factura_fecha"),
    ]:
        col = m.get(ckey)
        if col:
            selects.append(f"{parse_date_expr(_q(col))} AS {alias}")
        else:
            selects.append(f"NULL AS {alias}")

    # Montos numéricos
    col_monto = m.get("monto")
    if col_monto:
        selects.append(f"{parse_number_expr(_q(col_monto))} AS monto")
    else:
        selects.append("NULL AS monto")

    # Derivados
    selects.append(
        "CASE WHEN LOWER(COALESCE(estado_entrega,'')) LIKE '%entregad%' "
        "     OR (fecha_entrega IS NOT NULL AND fecha_entrega <= CURRENT_DATE) "
        "THEN TRUE ELSE FALSE END AS entregado_bool"
    )
    selects.append(
        "CASE WHEN (factura_num IS NULL OR TRIM(factura_num)='') "
        "   OR (factura_fecha IS NULL) "
        "   OR LOWER(COALESCE(facturado_flag,'')) IN ('no','pendiente','por facturar','sin factura','0','false') "
        "THEN TRUE ELSE FALSE END AS no_facturado_bool"
    )
    selects.append(
        "CASE WHEN fecha_recepcion IS NULL THEN NULL "
        "ELSE DATEDIFF('day', fecha_recepcion, COALESCE(fecha_entrega, CURRENT_DATE)) END AS dias_en_taller"
    )
    selects.append(
        "CASE WHEN fecha_entrega IS NULL THEN NULL "
        "ELSE DATEDIFF('day', fecha_entrega, CURRENT_DATE) END AS dias_desde_entrega"
    )

    sql = f'CREATE OR REPLACE VIEW MB AS SELECT {", ".join(selects)} FROM "{src_name}";'
    hint = (
        "View MB(patente TEXT, ot TEXT, cliente TEXT, factura_num TEXT, facturado_flag TEXT, "
        "tipo_cliente TEXT, estado_entrega TEXT, presupuesto_estado TEXT, "
        "fecha_recepcion DATE, fecha_entrega DATE, factura_fecha DATE, monto DOUBLE, "
        "entregado_bool BOOLEAN, no_facturado_bool BOOLEAN, "
        "dias_en_taller INTEGER, dias_desde_entrega INTEGER)"
    )
    return sql, hint

def build_fin_view_sql(src_name: str, df: pd.DataFrame) -> tuple[str, str]:
    m = map_cols(df, FIN_KEYS)
    selects = []

    # Textuales
    for ckey, alias in [
        ("factura_num","factura_num"),
        ("proveedor","proveedor"),
        ("estado_pago","estado_pago"),
    ]:
        col = m.get(ckey)
        selects.append(f"{_q(col)} AS {alias}" if col else f"NULL AS {alias}")

    # Fecha de vencimiento
    col_venc = m.get("vencimiento")
    if col_venc:
        selects.append(f"{parse_date_expr(_q(col_venc))} AS vencimiento")
    else:
        selects.append("NULL AS vencimiento")

    # Monto
    col_monto = m.get("monto")
    if col_monto:
        selects.append(f"{parse_number_expr(_q(col_monto))} AS monto")
    else:
        selects.append("NULL AS monto")

    # Derivados
    selects.append(
        "CASE WHEN LOWER(COALESCE(estado_pago,'')) IN "
        "('pendiente','por pagar','no','impago','abierta','abierto','sin pago') "
        "      OR estado_pago IS NULL OR TRIM(COALESCE(estado_pago,''))='' "
        "THEN TRUE ELSE FALSE END AS por_pagar_bool"
    )
    selects.append("DATEDIFF('day', CURRENT_DATE, vencimiento) AS dias_para_vencer")

    sql = f'CREATE OR REPLACE VIEW FIN AS SELECT {", ".join(selects)} FROM "{src_name}";'
    hint = (
        "View FIN(factura_num TEXT, proveedor TEXT, estado_pago TEXT, "
        "vencimiento DATE, monto DOUBLE, por_pagar_bool BOOLEAN, dias_para_vencer INTEGER)"
    )
    return sql, hint

def build_duckdb_prelude_and_schema(tables: dict[str, pd.DataFrame]) -> tuple[str, str]:
    # detecta los nombres reales tal como llegan desde Sheets
    name_mb = next((n for n in tables if n.upper() == "MODELO_BOT"), list(tables.keys())[0])
    name_fin = next((n for n in tables if n.upper() == "FINANZAS"), None)
    pre_sql = []
    hints = []
    sql, hint = build_mb_view_sql(name_mb, tables[name_mb]); pre_sql.append(sql); hints.append(hint)
    if name_fin:
        sql, hint = build_fin_view_sql(name_fin, tables[name_fin]); pre_sql.append(sql); hints.append(hint)
    return ";\n".join(pre_sql), "\n".join(hints)

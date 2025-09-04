import os, re, duckdb, pandas as pd

def has_openai():
    return bool(os.environ.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"))

def summarize_markdown(table_md: str, question: str) -> str:
    if not has_openai():
        return "Resumen: (sin OPENAI_API_KEY) Se muestran los resultados solicitados."
    try:
        from openai import OpenAI
        client = OpenAI()
        prompt = (
            "Eres un analista. Resume y prioriza para gestión la siguiente tabla "
            f"respecto a la pregunta: \"{question}\". Sé claro y accionable.\n\n{table_md}"
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"(Error LLM: {e})"

def safe_sql(sql: str) -> bool:
    s = sql.strip().lower()
    # Permitimos solo SELECT; los ; ya se limpian en normalize_sql
    return s.startswith("select ") and all(kw not in s for kw in [" insert ", " update ", " delete ", " drop ", " alter "])

def _normalize_sql(sql: str) -> str:
    """Quita fences, ; finales, espacios extra y evita LIMIT duplicado. Si no hay LIMIT, agrega uno."""
    if not isinstance(sql, str):
        return ""
    # Extraer bloque ```sql``` si viene con fences
    m = re.search(r"```sql(.*?)```", sql, flags=re.S | re.I)
    if m:
        sql = m.group(1)

    sql = sql.strip()
    # Eliminar ; finales
    sql = re.sub(r";+\s*$", "", sql)
    # Normalizar espacios
    sql = re.sub(r"\s+", " ", sql).strip()

    # Si el modelo ya puso LIMIT, no añadir otro.
    # Primero, borrar LIMIT final si hay más de uno (quedarnos con uno solo)
    # (Hacemos simple: quitamos el LIMIT final y luego lo volvemos a poner si no hay ninguno)
    sql = re.sub(r"\s+limit\s+\d+\s*$", "", sql, flags=re.I)

    # Si en otro lado existe LIMIT (p.ej., subquery), no añadimos uno global.
    has_any_limit = re.search(r"\blimit\b", sql, flags=re.I) is not None
    if not has_any_limit:
        sql += " LIMIT 200"

    return sql

def nl2sql(question: str, schema_hint: str, params: dict | None = None) -> str | None:
    """
    NL -> SQL (DuckDB) sobre vistas canónicas:
      - MB: datos de MODELO_BOT con derivados (entregado_bool, no_facturado_bool, etc.)
      - FIN: datos de FINANZAS con por_pagar_bool, etc.

    Reglas:
      - Siempre proyectar primero la ID: COALESCE(MB.patente, MB.ot) AS id (cuando use MB).
      - Evitar SELECT *.
      - Usar CURRENT_DATE para fechas relativas.
      - 'entregados' = MB.entregado_bool = TRUE
      - 'no facturado' = MB.no_facturado_bool = TRUE
      - 'en taller' = MB.entregado_bool = FALSE
      - 'por pagar' = FIN.por_pagar_bool = TRUE
      - Si no hay LIMIT, añadir LIMIT 200 (evitando duplicados).
    """
    if not has_openai():
        return None

    p = params or {}
    H = p.get("HORIZONTE_DIAS", 7)
    MES = p.get("MES", None)
    ANIO = p.get("ANIO", None)

    examples = f"""
# Esquema disponible
{schema_hint}

# Reglas adicionales
- Cuando la consulta use MB, selecciona primero COALESCE(MB.patente, MB.ot) AS id si corresponde.
- No repitas LIMIT si ya está presente. No agregues ';' al final.

# Equivalencias semánticas
- entregados -> MB.entregado_bool = TRUE
- no facturado -> MB.no_facturado_bool = TRUE
- en taller -> MB.entregado_bool = FALSE
- por pagar -> FIN.por_pagar_bool = TRUE

# Parámetros útiles
- HORIZONTE_DIAS = {H}
- MES_SELECCIONADO = {MES if MES else "(si no se especifica, no filtrar por mes)"}
- ANIO_SELECCIONADO = {ANIO if ANIO else "(si no se especifica, no filtrar por año)"}

# Ejemplos
Q: ¿Cuáles son los vehículos entregados que aún no han sido facturas?
A:
SELECT DISTINCT COALESCE(MB.patente, MB.ot) AS id, MB.cliente, MB.fecha_entrega, MB.dias_desde_entrega
FROM MB
WHERE MB.entregado_bool = TRUE AND MB.no_facturado_bool = TRUE
ORDER BY MB.fecha_entrega DESC
LIMIT 200

Q: ¿Cuántos días tienen los vehículos en el taller?
A:
SELECT COALESCE(MB.patente, MB.ot) AS id, MB.cliente, MB.fecha_recepcion, MB.dias_en_taller
FROM MB
WHERE MB.entregado_bool = FALSE
ORDER BY MB.dias_en_taller DESC
LIMIT 200

Q: Facturación de marzo por tipo de cliente
A:
SELECT MB.tipo_cliente, SUM(MB.monto) AS monto
FROM MB
WHERE EXTRACT(month FROM MB.fecha_entrega) = 3
GROUP BY MB.tipo_cliente
ORDER BY monto DESC
LIMIT 200

Q: ¿Cuáles son las facturas que se deben pagar en los próximos días?
A:
SELECT FIN.factura_num, FIN.proveedor, FIN.vencimiento, FIN.monto
FROM FIN
WHERE FIN.por_pagar_bool = TRUE
  AND FIN.vencimiento BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL {H} DAY
ORDER BY FIN.vencimiento ASC
LIMIT 200
"""
    try:
        from openai import OpenAI
        client = OpenAI()
        system = "Devuelve SOLO una consulta SQL DuckDB válida y segura (sin explicaciones)."
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": examples + f"\n\nAhora responde con la SQL para:\n{question}"},
            ],
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        sql = _normalize_sql(raw)
        if safe_sql(sql):
            return sql
        return None
    except Exception:
        return None

def run_duckdb(sql: str, tables: dict[str, pd.DataFrame], prelude_sql: str | None = None) -> pd.DataFrame:
    con = duckdb.connect()
    # Registrar tablas crudas
    for name, df in tables.items():
        con.register(name, df)
    # Crear vistas (prelude con MB/FIN)
    if prelude_sql:
        con.execute(prelude_sql)
    # Ejecutar consulta normalizada
    return con.execute(sql).df()


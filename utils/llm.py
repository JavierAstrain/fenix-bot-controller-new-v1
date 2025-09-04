import os, re, duckdb, pandas as pd
from datetime import datetime

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
    return s.startswith("select ") and all(kw not in s for kw in [" insert ", " update ", " delete ", " drop ", " alter "])

def _normalize_sql(sql: str, params: dict | None = None) -> str:
    """Quita fences/; extras, reemplaza placeholders y garantiza un solo LIMIT al final."""
    if not isinstance(sql, str):
        return ""

    # 1) Extraer bloque ```sql``` si vino con fences
    m = re.search(r"```sql(.*?)```", sql, flags=re.S | re.I)
    if m:
        sql = m.group(1)
    sql = sql.strip()

    # 2) Reemplazar placeholders típicos si el modelo los emite
    p = params or {}
    mes  = str(p.get("MES", "")) or ""
    anio = str(p.get("ANIO", "")) or ""
    hor  = str(p.get("HORIZONTE_DIAS", 7))
    # MES/ANIO solo si fueron provistos; si no, deja tal cual
    if mes:
        sql = re.sub(r"\bMES_SELECCIONADO\b", mes, sql, flags=re.I)
    if anio:
        sql = re.sub(r"\bANIO_SELECCIONADO\b", anio, sql, flags=re.I)
        sql = re.sub(r"\bAÑO_SELECCIONADO\b", anio, sql, flags=re.I)
    sql = re.sub(r"\bHORIZONTE_DIAS\b", hor, sql, flags=re.I)

    # 3) Quitar ; finales y normalizar espacios
    sql = re.sub(r";+\s*$", "", sql)
    sql = re.sub(r"\s+", " ", sql).strip()

    # 4) Garantizar un único LIMIT al final
    #    - elimina todos los LIMIT del final (si hay varios)
    sql = re.sub(r"(?:\s+limit\s+\d+\s*)+$", "", sql, flags=re.I)
    #    - si ya contiene LIMIT en alguna parte (p.ej. subquery), respetamos y no añadimos otro
    has_any_limit = re.search(r"\blimit\b", sql, flags=re.I) is not None
    if not has_any_limit:
        sql += " LIMIT 200"

    return sql.strip()

def nl2sql(question: str, schema_hint: str, params: dict | None = None) -> str | None:
    """
    NL -> SQL (DuckDB) sobre vistas canónicas:
      - MB: datos de MODELO_BOT con derivados (entregado_bool, no_facturado_bool, etc.)
      - FIN: FINANZAS con por_pagar_bool, etc.

    Reglas:
      - Si usas MB, proyecta primero COALESCE(MB.patente, MB.ot) AS id cuando tenga sentido.
      - Evita SELECT *.
      - Usa CURRENT_DATE para fechas relativas.
      - 'entregados' = MB.entregado_bool = TRUE
      - 'no facturado' = MB.no_facturado_bool = TRUE
      - 'en taller' = MB.entregado_bool = FALSE
      - 'por pagar' = FIN.por_pagar_bool = TRUE
      - Un solo LIMIT (200) si no se especifica.
    """
    if not has_openai():
        return None

    p = params or {}
    H = p.get("HORIZONTE_DIAS", 7)

    examples = f"""
# Esquema disponible
{schema_hint}

# Instrucciones
- Devuelve SOLO una consulta SQL DuckDB válida y segura (sin comentarios ni explicación).
- No uses placeholders textuales; usa números/expresiones literales.
- Si hay mes en la pregunta (ej. 'marzo'), usa el número (marzo=3).
- Si no se menciona el año, no filtres por año.
- Si se habla de 'próximos X días', usa CURRENT_DATE + INTERVAL X DAY; por defecto X={H}.
- Si la consulta usa MB, selecciona primero COALESCE(MB.patente, MB.ot) AS id si corresponde.
- No agregues ';' al final. Un único LIMIT si es necesario.

# Equivalencias
- 'entregados' -> MB.entregado_bool = TRUE
- 'no facturado' -> MB.no_facturado_bool = TRUE
- 'en taller' -> MB.entregado_bool = FALSE
- 'por pagar' -> FIN.por_pagar_bool = TRUE

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
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Devuelve SOLO SQL DuckDB seguro; nada de texto extra."},
                {"role": "user", "content": examples + f"\n\nAhora devuelve la SQL para:\n{question}"},
            ],
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        sql = _normalize_sql(raw, params=p)
        if safe_sql(sql):
            return sql
        return None
    except Exception:
        return None

def run_duckdb(sql: str, tables: dict[str, pd.DataFrame], prelude_sql: str | None = None) -> pd.DataFrame:
    con = duckdb.connect()
    for name, df in tables.items():
        con.register(name, df)
    if prelude_sql:
        con.execute(prelude_sql)
    return con.execute(sql).df()

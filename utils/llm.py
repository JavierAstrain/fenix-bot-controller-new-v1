# utils/llm.py
import os, re
import pandas as pd

_LAST_LLM_ERROR: str | None = None

def has_openai() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))

def llm_debug_info() -> str:
    """Texto corto con el estado del LLM para mostrar en UI."""
    global _LAST_LLM_ERROR
    parts = []
    parts.append(f"OPENAI_API_KEY presente: {'sí' if has_openai() else 'no'}")
    try:
        import openai  # noqa
        parts.append("openai import: ok")
    except Exception as e:
        parts.append(f"openai import: ERROR ({e})")
    if _LAST_LLM_ERROR:
        parts.append(f"último error LLM: { _LAST_LLM_ERROR }")
    return " | ".join(parts)

def _import_duckdb():
    try:
        import duckdb
        return duckdb, None
    except Exception as e:
        return None, e

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
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as e:
        global _LAST_LLM_ERROR
        _LAST_LLM_ERROR = f"LLM summarize error: {e}"
        return f"(Error LLM: {e})"

def _normalize_sql(sql: str, params: dict | None = None) -> str:
    if not isinstance(sql, str): return ""
    m = re.search(r"```sql(.*?)```", sql, flags=re.S | re.I)
    if m: sql = m.group(1)
    sql = sql.strip()
    p = params or {}
    mes  = str(p.get("MES", "")) or ""
    anio = str(p.get("ANIO", "")) or ""
    hor  = str(p.get("HORIZONTE_DIAS", 7))
    if mes:  sql = re.sub(r"\bMES_SELECCIONADO\b", mes, sql, flags=re.I)
    if anio:
        sql = re.sub(r"\bANIO_SELECCIONADO\b|\bAÑO_SELECCIONADO\b", anio, sql, flags=re.I)
    sql = re.sub(r"\bHORIZONTE_DIAS\b", hor, sql, flags=re.I)
    sql = re.sub(r";+\s*$", "", sql)
    sql = re.sub(r"\s+", " ", sql).strip()
    sql = re.sub(r"(?:\s+limit\s+\d+\s*)+$", "", sql, flags=re.I)
    if re.search(r"\blimit\b", sql, flags=re.I) is None:
        sql += " LIMIT 200"
    return sql

def nl2sql(question: str, schema_hint: str, params: dict | None = None) -> str | None:
    global _LAST_LLM_ERROR
    _LAST_LLM_ERROR = None
    if not has_openai():
        _LAST_LLM_ERROR = "OPENAI_API_KEY no presente en entorno"
        return None
    p = params or {}
    H = p.get("HORIZONTE_DIAS", 7)
    examples = f"""
# Esquema disponible
{schema_hint}

# Instrucciones
- Devuelve SOLO una consulta SQL DuckDB válida y segura (sin comentarios ni explicación).
- Usa las vistas MB y FIN. Evita SELECT *.
- Si usas MB, selecciona primero COALESCE(MB.patente, MB.ot) AS id si corresponde.
- Usa CURRENT_DATE para fechas relativas.
- 'entregados' = MB.entregado_bool = TRUE
- 'no facturado' = MB.no_facturado_bool = TRUE
- 'en taller' = MB.entregado_bool = FALSE
- 'por pagar' = FIN.por_pagar_bool = TRUE
- Para facturación mensual usa COALESCE(MB.factura_fecha, MB.fecha_entrega) = MB.fecha_op
- Un solo LIMIT (200) si no se especifica. No agregues ';'.

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
WHERE EXTRACT(month FROM MB.fecha_op) = 3
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
        return _normalize_sql(raw, params=p)
    except Exception as e:
        _LAST_LLM_ERROR = f"LLM nl2sql error: {e}"
        return None

def run_duckdb(sql: str, tables: dict[str, pd.DataFrame], prelude_sql: str | None = None) -> pd.DataFrame:
    duckdb, err = _import_duckdb()
    if err is not None:
        raise RuntimeError(f"DuckDB no disponible: {err}")
    con = duckdb.connect()
    for name, df in tables.items():
        con.register(name, df)
    if prelude_sql:
        con.execute(prelude_sql)
    return con.execute(sql).df()


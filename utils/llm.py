import os, re, duckdb, pandas as pd

def has_openai():
    return bool(os.environ.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY"))

def summarize_markdown(table_md: str, question: str) -> str:
    if not has_openai():
        return "Resumen: (sin OPENAI_API_KEY) Se muestran los resultados solicitados."
    try:
        from openai import OpenAI
        client = OpenAI()
        prompt = f"""Eres un analista. Resume y prioriza para gestión la siguiente tabla (markdown)
respecto a la pregunta: "{question}". Sé claro y accionable."""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"user","content": prompt + "\n\n" + table_md}
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"(Error LLM: {e})"

def safe_sql(sql: str) -> bool:
    s = sql.strip().lower()
    return s.startswith("select ") and all(kw not in s for kw in [" insert ", " update ", " delete ", " drop ", " alter ", ";"])

def nl2sql(question: str, schema_hint: str, params: dict | None = None) -> str | None:
    """
    Convierte lenguaje natural a SQL DuckDB sobre las vistas MB y FIN.
    Reglas fuertes:
    - SIEMPRE seleccionar la ID principal: 'patente' (o 'ot' si no existe) como PRIMERA columna.
    - Usar MB/FIN (no las tablas originales).
    - LIMIT 200 si no hay límite.
    - 'no facturado' = no_facturado_bool=TRUE ; 'entregado' = entregado_bool=TRUE.
    - 'próximos días' = HORIZONTE_DIAS (default 7).
    """
    if not has_openai():
        return None
    p = params or {}
    H = p.get("HORIZONTE_DIAS", 7)
    MES = p.get("MES", None)
    ANIO = p.get("ANIO", None)

    examples = f"""
# Esquema
{schema_hint}

# Reglas
- Siempre seleccionar primero COALESCE(MB.patente, MB.ot) AS id si aplica.
- Evitar SELECT *.
- Usar CURRENT_DATE para fechas relativas.
- Si no se indica límite, usa LIMIT 200.

# Equivalencias
- entregados = MB.entregado_bool = TRUE
- no facturado = MB.no_facturado_bool = TRUE
- en taller = MB.entregado_bool = FALSE
- por pagar = FIN.por_pagar_bool = TRUE

# Parámetros
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
                {"role":"system","content": system},
                {"role":"user","content": examples + f"\n\nAhora responde con la SQL para:\n{question}"}
            ],
            temperature=0.1,
        )
        sql = resp.choices[0].message.content.strip()
        m = re.search(r"```sql(.*?)```", sql, flags=re.S|re.I)
        if m: sql = m.group(1).strip()
        if " limit " not in sql.lower():
            sql += " LIMIT 200"
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
    # Crear vistas enriquecidas
    if prelude_sql:
        con.execute(prelude_sql)
    # Ejecutar consulta
    return con.execute(sql).df()

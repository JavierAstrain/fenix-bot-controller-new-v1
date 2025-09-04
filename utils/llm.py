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
en relación a la pregunta: "{question}". Sé claro y accionable.
Tabla:
{table_md}
"""
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
    return s.startswith("select ") and all(kw not in s for kw in [" insert ", " update ", " delete ", " drop ", " alter ", ";"])

def nl2sql(question: str, schema_hint: str) -> str | None:
    if not has_openai():
        return None
    try:
        from openai import OpenAI
        client = OpenAI()
        system = f"""Convierte la pregunta del usuario en UNA sola consulta SQL (DuckDB) segura.
Reglas:
- Solo SELECT.
- Si no se especifica límite, usa LIMIT 200.
- Usa nombres de tablas y columnas exactamente como en el esquema.
Esquema disponible:
{schema_hint}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role":"system","content": system},
                {"role":"user","content": question},
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

def run_duckdb(sql: str, tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    con = duckdb.connect()
    for name, df in tables.items():
        con.register(name, df)
    return con.execute(sql).df()

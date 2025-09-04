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

    # 2) Ree

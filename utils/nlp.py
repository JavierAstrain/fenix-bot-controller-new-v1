# utils/nlp.py
import re, os, json
import pandas as pd

def find_col(df, synonyms):
    """Busca la primera columna que contenga alguno de los términos (case-insensitive)."""
    cols = list(df.columns)
    lcols = [c.lower() for c in cols]
    for syn in synonyms:
        s = syn.lower()
        for i, lc in enumerate(lcols):
            if s in lc:
                return cols[i]
    return None

def ilike(series, term):
    """Filtro 'contiene' case-insensitive sobre series de texto."""
    if term is None or term == "":
        return series.astype(str).str.len() >= 0
    return series.astype(str).str.contains(re.escape(term), case=False, na=False)

# ---------- Parser NL -> JSON (métrica + filtros) usando OpenAI ----------
def _get_client():
    from openai import OpenAI
    api = os.environ.get("OPENAI_API_KEY")
    if not api:
        return None
    return OpenAI(api_key=api)

INTENT_SCHEMA = {
  "name": "resolve_question",
  "description": "Mapea la pregunta a una métrica del catálogo y filtros. Devuelve solo argumentos JSON.",
  "parameters": {
    "type": "object",
    "properties": {
      "metric": {"type": "string", "description": "Nombre de la métrica del catálogo (ej: entregados_sin_factura)."},
      "filters": {"type": "object", "additionalProperties": True, "description": "Filtros opcionales (cliente, marca, asesor, sucursal, desde, hasta, mes, anio, horizonte)."},
      "hints": {"type": "array", "items": {"type":"string"}}
    },
    "required": ["metric"]
  }
}

def parse_question_to_json(question: str, semantic_text: str) -> dict | None:
    """
    Devuelve un dict con {metric, filters?, hints?} o None si no pudo.
    """
    client = _get_client()
    if client is None:
        return None
    msgs = [
        {"role":"system","content":"Eres un analista de datos. Devuelve SOLO argumentos para la función; nada de texto libre."},
        {"role":"user","content": f"Catálogo de métricas y sinónimos:\n{semantic_text}\n\nPregunta del usuario:\n{question}"}
    ]
    try:
        resp = client.chat.completions.create(
          model="gpt-4o-mini",
          messages=msgs,
          tools=[{"type":"function","function":INTENT_SCHEMA}],
          tool_choice={"type":"function", "function":{"name":"resolve_question"}},
          temperature=0.1,
        )
        args = resp.choices[0].message.tool_calls[0].function.arguments
        return json.loads(args)
    except Exception:
        return None

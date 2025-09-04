# utils/nlp.py
import re, os, json, unicodedata
import pandas as pd

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join([c for c in s if not unicodedata.category(c).startswith("M")])
    return s.lower().strip()

def find_col(df, synonyms):
    """Busca por contiene, sin acentos y case-insensitive."""
    cols = list(df.columns)
    ncols = [_norm(c) for c in cols]
    for syn in synonyms:
        ns = _norm(syn)
        for i, nc in enumerate(ncols):
            if ns in nc:
                return cols[i]
    return None

def ilike(series, term):
    if not term: return series.astype(str).str.len() >= 0
    return series.astype(str).str.contains(re.escape(term), case=False, na=False)

# --------- Parser NL->JSON (si ya lo tienes, déjalo) ----------
def _get_client():
    from openai import OpenAI
    api = os.environ.get("OPENAI_API_KEY")
    if not api: return None
    return OpenAI(api_key=api)

INTENT_SCHEMA = {
  "name": "resolve_question",
  "description": "Mapea la pregunta a una métrica del catálogo y filtros. Devuelve solo argumentos JSON.",
  "parameters": {
    "type": "object",
    "properties": {
      "metric": {"type": "string"},
      "filters": {"type": "object", "additionalProperties": True},
      "hints": {"type":"array","items":{"type":"string"}}
    },
    "required": ["metric"]
  }
}

def parse_question_to_json(question: str, semantic_text: str) -> dict | None:
    try:
        client = _get_client()
        if client is None: return None
        msgs = [
          {"role":"system","content":"Eres un analista. Devuelve SOLO argumentos para la función."},
          {"role":"user","content": f"Catálogo:\n{semantic_text}\n\nPregunta:\n{question}"}
        ]
        resp = client.chat.completions.create(
          model="gpt-4o-mini",
          messages=msgs,
          tools=[{"type":"function","function":INTENT_SCHEMA}],
          tool_choice={"type":"function", "function":{"name":"resolve_question"}},
          temperature=0.1,
        )
        args = resp.choices[0].message.tool_calls[0].function.arguments
        import json
        return json.loads(args)
    except Exception:
        return None

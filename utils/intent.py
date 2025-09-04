# utils/intent.py — IA → JSON (intención) → filtros deterministas con auto-reparación
from __future__ import annotations
import os, re, json, unicodedata
from typing import Any, Dict, List, Optional
import pandas as pd

# Usa el SDK oficial de OpenAI (ya lo tienes en el proyecto)
from openai import OpenAI

# ---------- helpers ----------
def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.category(c).startswith("M"))
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

DATE_FIELDS = ["FECHA_FACTURACION","FECHA_ENTREGA","FECHA_RECEPCION","FECHA_PAGO_FACTURA","fecha_op"]
GROUP_BY_OPTS = ["ninguno","tipo_cliente","marca","estado_servicio"]
METRICS_OPTS  = ["lista","conteo","suma_neto"]

def _distinct(series: pd.Series, limit=80) -> List[str]:
    if series is None: return []
    vals = (
        series.astype(str)
        .map(_norm)
        .replace({"nan": ""})
        .dropna()
        .unique()
        .tolist()
    )
    return [v for v in vals if v][:limit]

def _collect_enums(MB: pd.DataFrame) -> Dict[str, List[str]]:
    return {
        "marcas": _distinct(MB.get("MARCA")),
        "tipo_cliente": _distinct(MB.get("TIPO_CLIENTE")),
        "estado_servicio": _distinct(MB.get("ESTADO_SERVICIO")),
        "sucursal": _distinct(MB.get("SUCURSAL")) if "SUCURSAL" in MB.columns else [],
    }

def _client() -> OpenAI:
    os.environ.setdefault("OPENAI_MODEL", os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    return OpenAI()

# ---------- LLM: pregunta → QuerySpec (JSON cerrado) ----------
def llm_question_to_queryspec(question: str, MB: pd.DataFrame) -> Dict[str, Any]:
    enums = _collect_enums(MB)

    system = (
        "Eres un parser de consultas en español para una planilla de vehículos.\n"
        "Devuelves SOLO un JSON válido con este esquema (sin texto adicional):\n"
        "{\n"
        '  "delivered": true|false|null,                // ENTREGADOS (True), EN TALLER (False) o no especifica (null)\n'
        '  "invoiced":  true|false|null,                // FACTURADOS (True), SIN FACTURA (False) o null\n'
        '  "date_field": "FECHA_FACTURACION|FECHA_ENTREGA|FECHA_RECEPCION|FECHA_PAGO_FACTURA|fecha_op",\n'
        '  "date_range": {"start":"YYYY-MM-DD|null","end":"YYYY-MM-DD|null","proximos_dias":int|null,"ultimos_dias":int|null},\n'
        '  "filters": {\n'
        '     "cliente_contains": "string|null", "patente_contains":"string|null",\n'
        '     "marca_exact":"string|null", "tipo_cliente_exact":"string|null",\n'
        '     "sucursal_exact":"string|null", "estado_servicio_contains":"string|null"\n'
        "  },\n"
        '  "group_by": "ninguno|tipo_cliente|marca|estado_servicio",\n'
        '  "metrics": "lista|conteo|suma_neto",\n'
        '  "top_n": int|null,\n'
        '  "sort_desc": true|false\n'
        "}\n"
        "REGLAS:\n"
        "- Reconoce sinónimos: entregados→delivered=true; en taller/no entregados→delivered=false; con factura→invoiced=true; sin factura/no facturados→invoiced=false.\n"
        "- Si habla de facturación → date_field=FECHA_FACTURACION; entrega→FECHA_ENTREGA; recepción→FECHA_RECEPCION; pago→FECHA_PAGO_FACTURA; si no dice, usa fecha_op.\n"
        "- Usa SOLO valores exactos de estos catálogos (si no hay match, deja null):\n"
        f"  marcas: {enums['marcas']}\n"
        f"  tipo_cli_

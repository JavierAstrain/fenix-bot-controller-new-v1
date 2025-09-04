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
        f"  tipo_cliente: {enums['tipo_cliente']}\n"
        f"  estado_servicio (ejemplos): {enums['estado_servicio'][:10]}\n"
        f"  sucursal: {enums['sucursal']}\n"
        "- group_by ∈ {ninguno,tipo_cliente,marca,estado_servicio}. metrics ∈ {METRICS_OPTS}.\n"
        "- Si el usuario pide una LISTA, usa metrics='lista' y group_by='ninguno'.\n"
        "- Si pide 'cuántos', usa metrics='conteo'. Si pide 'monto' o 'facturación por', usa metrics='suma_neto' (por group_by si aplica).\n"
        "- No inventes campos ni valores fuera del esquema."
    )

    # Pocos ejemplos críticos de referencia (cadenas típicas del negocio)
    examples = [
        ("¿Cuáles son los vehículos entregados sin factura?",
         {"delivered": True, "invoiced": False, "date_field":"fecha_op",
          "date_range":{"start":None,"end":None,"proximos_dias":None,"ultimos_dias":None},
          "filters":{"cliente_contains":None,"patente_contains":None,"marca_exact":None,"tipo_cliente_exact":None,"sucursal_exact":None,"estado_servicio_contains":None},
          "group_by":"ninguno","metrics":"lista","top_n":200,"sort_desc":True}),
        ("En taller sin aprobación (sin factura), últimos 10 días",
         {"delivered": False, "invoiced": False, "date_field":"fecha_op",
          "date_range":{"start":None,"end":None,"proximos_dias":None,"ultimos_dias":10},
          "filters":{"cliente_contains":None,"patente_contains":None,"marca_exact":None,"tipo_cliente_exact":None,"sucursal_exact":None,"estado_servicio_contains":"aprob"},
          "group_by":"ninguno","metrics":"lista","top_n":200,"sort_desc":True}),
        ("Facturación de marzo por tipo de cliente",
         {"delivered": None, "invoiced": True, "date_field":"FECHA_FACTURACION",
          "date_range":{"start":None,"end":None,"proximos_dias":None,"ultimos_dias":None},
          "filters":{"cliente_contains":None,"patente_contains":None,"marca_exact":None,"tipo_cliente_exact":None,"sucursal_exact":None,"estado_servicio_contains":None},
          "group_by":"tipo_cliente","metrics":"suma_neto","top_n":100,"sort_desc":True}),
        ("Próximos 7 días entregas sin facturar",
         {"delivered": True, "invoiced": False, "date_field":"FECHA_ENTREGA",
          "date_range":{"start":None,"end":None,"proximos_dias":7,"ultimos_dias":None},
          "filters":{"cliente_contains":None,"patente_contains":None,"marca_exact":None,"tipo_cliente_exact":None,"sucursal_exact":None,"estado_servicio_contains":None},
          "group_by":"ninguno","metrics":"lista","top_n":200,"sort_desc":False}),
        ("Cuántos vehículos livianos con factura en Toyota",
         {"delivered": None, "invoiced": True, "date_field":"fecha_op",
          "date_range":{"start":None,"end":None,"proximos_dias":None,"ultimos_dias":None},
          "filters":{"cliente_contains":None,"patente_contains":None,"marca_exact":"toyota","tipo_cliente_exact":None,"sucursal_exact":None,"estado_servicio_contains":"liviano"},
          "group_by":"ninguno","metrics":"conteo","top_n":None,"sort_desc":True}),
    ]

    user = "Pregunta del usuario:\n" + question + "\n\n" + "Ejemplos JSON de salida:\n" + "\n".join(
        [json.dumps(e[1], ensure_ascii=False) for e in examples]
    )

    client = _client()
    rsp = client.responses.create(
        model=os.environ["OPENAI_MODEL"],
        temperature=0,
        response_format={"type": "json_object"},
        input=[{"role":"system","content":system},{"role":"user","content":user}],
    )
    try:
        spec = json.loads(rsp.output_text)
    except Exception:
        spec = {}

    return _validate_and_repair_spec(spec)

# ---------- validación + reparación ligera ----------
def _validate_and_repair_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    # defaults
    spec = spec or {}
    spec.setdefault("delivered", None)
    spec.setdefault("invoiced", None)
    spec.setdefault("date_field", "fecha_op")
    dr = spec.get("date_range") or {}
    spec["date_range"] = {
        "start": dr.get("start") if isinstance(dr.get("start"), str) else None,
        "end":   dr.get("end")   if isinstance(dr.get("end"),   str) else None,
        "proximos_dias": int(dr.get("proximos_dias")) if isinstance(dr.get("proximos_dias"), (int,float)) else None,
        "ultimos_dias":  int(dr.get("ultimos_dias"))  if isinstance(dr.get("ultimos_dias"),  (int,float)) else None,
    }
    spec["filters"] = spec.get("filters") or {}
    for k in ["cliente_contains","patente_contains","marca_exact","tipo_cliente_exact","sucursal_exact","estado_servicio_contains"]:
        v = spec["filters"].get(k)
        spec["filters"][k] = v if (isinstance(v,str) and v.strip()) else None
    if spec.get("date_field") not in DATE_FIELDS:
        spec["date_field"] = "fecha_op"
    if spec.get("group_by") not in GROUP_BY_OPTS:
        spec["group_by"] = "ninguno"
    if spec.get("metrics") not in METRICS_OPTS:
        spec["metrics"] = "lista"
    spec["top_n"] = int(spec["top_n"]) if isinstance(spec.get("top_n"), (int,float)) else None
    spec["sort_desc"] = bool(spec.get("sort_desc")) if spec.get("sort_desc") is not None else True
    return spec

# ---------- ejecutar QuerySpec sobre MB ----------
def _apply_state_filters(df: pd.DataFrame, delivered, invoiced) -> pd.DataFrame:
    t = df
    if delivered is True:  t = t[t["entregado_bool"]]
    if delivered is False: t = t[~t["entregado_bool"]]
    if invoiced is True:   t = t[t["facturado_bool"]]
    if invoiced is False:  t = t[t["no_facturado_bool"]]
    return t

def _apply_text_filters(t: pd.DataFrame, f: Dict[str, Any]) -> pd.DataFrame:
    if f.get("cliente_contains"):
        t = t[t["NOMBRE_CLIENTE"].astype(str).str.contains(f["cliente_contains"], case=False, na=False)]
    if f.get("patente_contains"):
        t = t[t["PATENTE"].astype(str).str.contains(f["patente_contains"], case=False, na=False)]
    if f.get("marca_exact"):
        t = t[t["MARCA"].astype(str).str.lower()==f["marca_exact"].lower()]
    if f.get("tipo_cliente_exact"):
        t = t[t["TIPO_CLIENTE"].astype(str).str.lower()==f["tipo_cliente_exact"].lower()]
    if f.get("sucursal_exact") and "SUCURSAL" in t.columns:
        t = t[t["SUCURSAL"].astype(str).str.lower()==f["sucursal_exact"].lower()]
    if f.get("estado_servicio_contains"):
        t = t[t["ESTADO_SERVICIO"].astype(str).str.contains(f["estado_servicio_contains"], case=False, na=False)]
    return t

def _apply_time_filters(t: pd.DataFrame, date_col: str, dr: Dict[str,Any]) -> pd.DataFrame:
    if date_col not in t.columns:
        date_col = "fecha_op"
    prox, last = dr.get("proximos_dias"), dr.get("ultimos_dias")
    start, end = dr.get("start"), dr.get("end")
    if prox:
        hoy = pd.Timestamp.today().normalize(); fin = hoy + pd.Timedelta(days=int(prox))
        return t[t[date_col].between(hoy, fin)]
    if last:
        fin = pd.Timestamp.today().normalize(); ini = fin - pd.Timedelta(days=int(last))
        return t[t[date_col].between(ini, fin)]
    if start: t = t[t[date_col] >= pd.to_datetime(start)]
    if end:   t = t[t[date_col] <= pd.to_datetime(end)]
    return t

def execute_queryspec(MB: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    t = MB.copy()

    # 1) estado (entregado / facturado)
    t = _apply_state_filters(t, spec.get("delivered"), spec.get("invoiced"))

    # 2) filtros de texto
    t = _apply_text_filters(t, spec.get("filters", {}))

    # 3) tiempo
    date_col = spec.get("date_field") or "fecha_op"
    t = _apply_time_filters(t, date_col, spec.get("date_range", {}))

    # Reparación si quedó vacío: quitamos fechas y probamos otra columna de fecha
    if t.empty:
        # 3a) quitar rango de fechas
        t1 = _apply_state_filters(MB, spec.get("delivered"), spec.get("invoiced"))
        t1 = _apply_text_filters(t1, spec.get("filters", {}))
        if not t1.empty:
            t = t1  # sin fechas
        else:
            # 3b) probar fecha alternativa (si preguntó por entrega, prueba facturación, etc.)
            fallback_order = ["FECHA_ENTREGA","FECHA_FACTURACION","FECHA_RECEPCION","fecha_op"]
            for col in fallback_order:
                if col in MB.columns:
                    t2 = _apply_time_filters(t1, col, {"start":None,"end":None,"proximos_dias":None,"ultimos_dias":None})
                    if not t2.empty:
                        t = t2
                        break

    # 4) salida según métrica/agrupación
    metric = spec.get("metrics","lista")
    gb = spec.get("group_by","ninguno")
    topn = spec.get("top_n") or (100 if metric != "lista" else 300)
    sort_desc = bool(spec.get("sort_desc", True))

    if metric == "lista" and gb == "ninguno":
        cols = [c for c in [
            "id","NOMBRE_CLIENTE","PATENTE","MARCA","ESTADO_SERVICIO",
            "FECHA_RECEPCION","FECHA_ENTREGA","NUMERO_FACTURA",
            "FECHA_FACTURACION","MONTO_NETO","NUMERO_DIAS_EN_PLANTA","FACTURADO_FLAG"
        ] if c in t.columns]
        sort_cols = [c for c in ["FECHA_ENTREGA","FECHA_FACTURACION","FECHA_RECEPCION"] if c in t.columns]
        if sort_cols:
            t = t.sort_values(sort_cols, ascending=[not sort_desc]*len(sort_cols), kind="stable")
        return t[cols].head(topn)

    if gb != "ninguno":
        key = {"tipo_cliente":"TIPO_CLIENTE","marca":"MARCA","estado_servicio":"ESTADO_SERVICIO"}[gb]
        if metric == "conteo":
            g = t.groupby(key, dropna=False, as_index=False).size().rename(columns={"size":"CANTIDAD"})
            return g.sort_values("CANTIDAD", ascending=not sort_desc).head(topn)
        if metric == "suma_neto":
            g = t.groupby(key, dropna=False, as_index=False)["MONTO_NETO"].sum()
            return g.sort_values("MONTO_NETO", ascending=not sort_desc).head(topn)

    # fallback
    return t.head(topn)

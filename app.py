# app.py — Agente Fénix (solo MODELO_BOT)
import os, sys, io, base64, traceback
from datetime import datetime
import streamlit as st
import pandas as pd
import plotly.express as px

ICON_PATH = "assets/Isotipo_Nexa.png"
try:
    st.set_page_config(page_title="Agente Fénix", page_icon=ICON_PATH if os.path.exists(ICON_PATH) else None, layout="wide")
except Exception:
    st.set_page_config(page_title="Agente Fénix", layout="wide")

def _debug_on():
    try:
        return str(st.query_params.get("debug", "0")) in ("1","true","True")
    except Exception:
        qp = st.experimental_get_query_params()
        return qp.get("debug", ["0"])[0] in ("1","true","True")

if _debug_on():
    st.sidebar.title("🛠️ Debug")
    try: st.sidebar.write("secrets keys:", list(st.secrets.keys()))
    except Exception as e: st.sidebar.write("secrets error:", e)
    st.sidebar.write("python:", sys.version)

try:
    if "OPENAI_API_KEY" not in os.environ and st.secrets.get("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]
except Exception:
    pass

def safe_import(module_path, names):
    try:
        mod = __import__(module_path, fromlist=names)
        return [getattr(mod, n) for n in names]
    except Exception as e:
        st.error(f"❌ Error importando {module_path}")
        st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)))
        st.stop()

(load_sheets,) = safe_import("utils.gsheets", ["load_sheets"])
(ensure_login,) = safe_import("utils.login", ["ensure_login"])
(format_currency_clp, format_date_ddmmyyyy) = safe_import("utils.formatters", ["format_currency_clp", "format_date_ddmmyyyy"])
(df_to_md,) = safe_import("utils.md", ["df_to_md"])
(build_duckdb_prelude_and_schema,) = safe_import("utils.schema", ["build_duckdb_prelude_and_schema"])
(summarize_markdown, nl2sql, run_duckdb, has_openai, llm_debug_info) = safe_import("utils.llm", ["summarize_markdown","nl2sql","run_duckdb","has_openai","llm_debug_info"])
(parse_question_to_json,) = safe_import("utils.nlp", ["parse_question_to_json"])
(verify_and_refine,) = safe_import("utils.llm_guard", ["verify_and_refine"])
(
    skill_entregados_sin_factura,
    skill_entregados_facturados,
    skill_top_en_taller,
    skill_facturacion_por_mes_tipo,
    skill_entregas_proximos_dias_sin_factura,
    skill_sin_aprobacion,
) = safe_import("utils.skills", [
    "skill_entregados_sin_factura",
    "skill_entregados_facturados",
    "skill_top_en_taller",
    "skill_facturacion_por_mes_tipo",
    "skill_entregas_proximos_dias_sin_factura",
    "skill_sin_aprobacion",
])

ensure_login()

def _b64(path: str) -> str:
    try:
        with open(path, "rb") as f: return base64.b64encode(f.read()).decode("utf-8")
    except Exception: return ""
fenix_b64 = _b64("assets/Fenix_isotipo.png")
if fenix_b64:
    st.markdown(
        f"""<style>.top-right-logo{{position:fixed;top:12px;right:16px;z-index:1000;}}</style>
        <div class="top-right-logo"><img src="data:image/png;base64,{fenix_b64}" height="40"/></div>""",
        unsafe_allow_html=True,
    )

with st.sidebar:
    try: st.image("assets/Nexa_logo.png", use_container_width=True)
    except Exception: pass
    st.markdown("---")

st.title("🛠️ Agente Fénix")
st.caption("Solo MODELO_BOT. Capa semántica + skills deterministas; Auto-SQL de respaldo.")

# Conexión (solo MODELO_BOT)
sheet_id = st.secrets.get("SHEET_ID", "")
with st.sidebar:
    st.subheader("Conexión")
    try:
        data = load_sheets(sheet_id, allow_sheets=("MODELO_BOT",))
        st.success("Google Sheets conectado (solo lectura).")
        st.write("Hoja:", "MODELO_BOT")
    except Exception as e:
        st.error(f"Error al conectar: {e}")
        st.info("Verifica SHEET_ID y comparte con el client_email de la service account (Viewer).")
        st.stop()

    st.subheader("Preferencias")
    mes = st.number_input("Mes para facturación", 1, 12, value=datetime.now().month)
    anio = st.number_input("Año para facturación", 2000, 2100, value=datetime.now().year)

    st.subheader("Estado LLM")
    st.write(llm_debug_info())

    if st.button("Cerrar sesión"):
        st.session_state.authenticated = False
        st.rerun()

def _fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        lc = c.lower()
        if any(k in lc for k in ["monto","total","valor","neto","bruto","importe"]):
            out[c] = out[c].map(format_currency_clp)
        if "fecha" in lc:
            out[c] = out[c].map(format_date_ddmmyyyy)
    return out

def _show(df: pd.DataFrame, name: str):
    st.dataframe(_fmt_df(df), use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"), f"{name}.csv", "text/csv")
    with c2:
        buff = io.BytesIO()
        with pd.ExcelWriter(buff, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name="Datos")
        st.download_button("⬇️ XLSX", buff.getvalue(), f"{name}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@st.cache_data(show_spinner=False)
def load_semantic_yaml():
    with open("semantic.yaml", "r", encoding="utf-8") as f:
        return f.read()
try:
    semantic_text = load_semantic_yaml()
except Exception as e:
    semantic_text = ""
    st.sidebar.warning(f"No se pudo cargar semantic.yaml: {e}")

tabs = st.tabs(["Preguntar (semántico) + Auto-SQL", "Botones rápidos", "Calibración"])

# ---- TAB 1
with tabs[0]:
    st.markdown("### Preguntar (semántico) + Auto-SQL de respaldo")
    q = st.text_input("Pregunta", "¿Cuáles son los vehículos entregados que aún no han sido facturas?")
    if st.button("Responder", key="btn_sem"):
        used_path = None
        if semantic_text and has_openai():
            parsed = parse_question_to_json(q, semantic_text)
            if parsed and parsed.get("metric"):
                metric = parsed["metric"]
                filters = parsed.get("filters", {}) or {}
                used_path = f"Semántico → {metric}"
                try:
                    MB = data.get("MODELO_BOT", next(iter(data.values())))
                    if metric == "entregados_sin_factura":
                        df, err = skill_entregados_sin_factura(MB, **filters)
                    elif metric == "entregados_facturados":
                        df, err = skill_entregados_facturados(MB, **filters)
                    elif metric == "en_taller":
                        df, err = skill_top_en_taller(MB, **filters)
                    elif metric == "facturacion_mensual_tipo_cliente":
                        df, err = skill_facturacion_por_mes_tipo(MB, int(filters.get("mes", mes)), int(filters.get("anio", anio)))
                    elif metric == "entregas_proximas_sin_factura":
                        df, err = skill_entregas_proximos_dias_sin_factura(MB, int(filters.get("horizonte", 7)))
                    elif metric == "sin_aprobacion":
                        df, err = skill_sin_aprobacion(MB)
                    else:
                        df, err = None, f"Métrica no implementada: {metric}"
                except Exception as e:
                    df, err = None, str(e)

                if err:
                    st.warning(err)
                elif df is not None and not df.empty:
                    st.success(f"✓ Ruta: {used_path}")
                    _show(df, "resultado_semantico")
                else:
                    st.info("Ruta semántica devolvió 0 filas; intentaré Auto-SQL.")
                    used_path = None

        if used_path is None:
            try:
                prelude_sql, schema_hint = build_duckdb_prelude_and_schema({"MODELO_BOT": data["MODELO_BOT"]})
                params = {"MES": int(mes), "ANIO": int(anio)}
                sql = nl2sql(q, schema_hint=schema_hint, params=params)
                if not sql:
                    st.warning("No pude generar SQL. Revisa el 'Estado LLM' en la barra lateral.")
                    st.info(llm_debug_info())
                else:
                    st.code(sql, language="sql")
                    df = run_duckdb(sql, {"MODELO_BOT": data["MODELO_BOT"]}, prelude_sql=prelude_sql)
                    if df.empty: st.info("Sin resultados.")
                    else:
                        _show(df, "resultado_autosql")
                        try:
                            st.markdown("### Resumen")
                            st.write(summarize_markdown(df_to_md(df), q))
                        except Exception as e:
                            st.info(f"(Resumen no disponible) {e}")
            except Exception as e:
                st.error("Error en Auto-SQL:")
                st.code("".join(traceback.format_exception(type(e), e, e.__traceback__)))

# ---- TAB 2
with tabs[1]:
    st.markdown("### Botones rápidos (MODELO_BOT)")
    MB = data.get("MODELO_BOT", next(iter(data.values())))

    c = st.columns(2)
    with c[0]:
        st.subheader("Entregados SIN factura")
        cliente = st.text_input("Cliente (contiene)", key="f1_cli")
        tipo_cli = st.text_input("Tipo cliente (exacto)", key="f1_tc")
        marca = st.text_input("Marca (exacto)", key="f1_marca")
        suc = st.text_input("Sucursal (exacto)", key="f1_suc")
        asesor = st.text_input("Asesor (contiene)", key="f1_ase")
        fdesde = st.text_input("Fecha desde (YYYY-MM-DD)", key="f1_fd")
        fhasta = st.text_input("Fecha hasta (YYYY-MM-DD)", key="f1_fh")
        if st.button("Ejecutar 1"):
            table, err = skill_entregados_sin_factura(MB, cliente=cliente or None, tipo_cliente=tipo_cli or None,
                                                      marca=marca or None, sucursal=suc or None, asesor=asesor or None,
                                                      desde=fdesde or None, hasta=fhasta or None)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "entregados_sin_factura")

    with c[1]:
        st.subheader("Entregados CON factura")
        if st.button("Ejecutar 2"):
            table, err = skill_entregados_facturados(MB)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "entregados_facturados")

    with c[0]:
        st.subheader("Top en taller (no entregados)")
        topn = st.number_input("Top N", 1, 100, value=10, key="f3_top")
        if st.button("Ejecutar 3"):
            table, err = skill_top_en_taller(MB, topn=int(topn))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                _show(table, "top_en_taller")
                try: st.plotly_chart(px.bar(table, x=table.columns[0], y="NUMERO_DIAS_EN_PLANTA"), use_container_width=True)
                except Exception: pass

    with c[1]:
        st.subheader("Facturación por mes / tipo cliente")
        if st.button("Ejecutar 4"):
            table, err = skill_facturacion_por_mes_tipo(MB, int(mes), int(anio))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                _show(table, "facturacion_mes_tipo")
                try: st.plotly_chart(px.pie(table, names=table.columns[0], values="MONTO_NETO"), use_container_width=True)
                except Exception: pass

    with c[0]:
        st.subheader("Entregas próximos días SIN facturación")
        horizonte = st.number_input("Días", 1, 60, value=7, key="f5_h")
        if st.button("Ejecutar 5"):
            table, err = skill_entregas_proximos_dias_sin_factura(MB, int(horizonte))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "entregas_proximas_sin_factura")

    with c[1]:
        st.subheader("En taller SIN aprobación (proxy)")
        if st.button("Ejecutar 6"):
            table, err = skill_sin_aprobacion(MB)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "sin_aprobacion")

# ---- TAB 3 (Calibración)
with tabs[2]:
    st.markdown("### Calibración (ver lectura real de columnas)")
    MB = data.get("MODELO_BOT", next(iter(data.values())))
    st.subheader("Encabezados MODELO_BOT")
    st.write(list(MB.columns))

    import yaml, os
    current_map = {}
    if os.path.exists("column_map.yaml"):
        current_map = yaml.safe_load(open("column_map.yaml","r",encoding="utf-8")) or {}
    st.subheader("Mapeo actual (column_map.yaml)")
    st.json(current_map.get("MODELO_BOT", {}))

    from utils.skills import _build_mb
    prev = _build_mb(MB).head(15)
    st.subheader("Preview derivadas (verifica booleans)")
    cols = ["id","NOMBRE_CLIENTE","FECHA_RECEPCION","FECHA_ENTREGA","NUMERO_FACTURA","FECHA_FACTURACION",
            "entregado_bool","facturado_bool","no_facturado_bool","_estado_servicio_norm","_facturado_flag_norm"]
    cols = [c for c in cols if c in prev.columns]
    st.dataframe(prev[cols], use_container_width=True)

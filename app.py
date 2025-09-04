# app.py — Agente Fénix (semántico + Auto-SQL fallback)
import os, sys, io, base64, traceback, yaml
from datetime import datetime
import streamlit as st
import pandas as pd
import plotly.express as px

ICON_PATH = "assets/Isotipo_Nexa.png"
try:
    st.set_page_config(page_title="Agente Fénix",
                       page_icon=ICON_PATH if os.path.exists(ICON_PATH) else None,
                       layout="wide")
except Exception:
    st.set_page_config(page_title="Agente Fénix", layout="wide")

def _debug_on():
    try: return str(st.query_params.get("debug", "0")) in ("1","true","True")
    except Exception:
        qp = st.experimental_get_query_params()
        return qp.get("debug", ["0"])[0] in ("1","true","True")

if _debug_on():
    st.sidebar.title("🛠️ Debug")
    try: st.sidebar.write("secrets keys:", list(st.secrets.keys()))
    except Exception as e: st.sidebar.write("secrets error:", e)
    st.sidebar.write("python:", sys.version)

# export OPENAI_API_KEY si existe
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
# NUEVO: parser JSON y guard
(parse_question_to_json,) = safe_import("utils.nlp", ["parse_question_to_json"])
(verify_and_refine,) = safe_import("utils.llm_guard", ["verify_and_refine"])
# Skills deterministas
(
    skill_entregados_sin_factura,
    skill_facturas_por_pagar,
    skill_top_en_taller,
    skill_facturacion_por_mes_tipo,
    skill_entregas_proximos_dias_sin_factura,
    skill_sin_aprobacion,
) = safe_import("utils.skills",
[
 "skill_entregados_sin_factura",
 "skill_facturas_por_pagar",
 "skill_top_en_taller",
 "skill_facturacion_por_mes_tipo",
 "skill_entregas_proximos_dias_sin_factura",
 "skill_sin_aprobacion",
])

ensure_login()

# Fenix isotipo arriba derecha (opcional)
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
st.caption("Capa semántica + skills deterministas; Auto-SQL como respaldo.")

# Conexión Google Sheets
sheet_id = st.secrets.get("SHEET_ID", "")
with st.sidebar:
    st.subheader("Conexión")
    try:
        data = load_sheets(sheet_id, allow_sheets=("MODELO_BOT","FINANZAS"))
        st.success("Google Sheets conectado (solo lectura).")
        st.write("Hojas:", ", ".join(data.keys()))
    except Exception as e:
        st.error(f"Error al conectar: {e}")
        st.info("Verifica SHEET_ID y comparte con el client_email de la service account (Viewer).")
        st.stop()

    st.subheader("Preferencias")
    horizonte = st.number_input("Días (próximos)", 1, 60, value=7)
    mes = st.number_input("Mes para facturación", 1, 12, value=datetime.now().month)
    anio = st.number_input("Año para facturación", 2000, 2100, value=datetime.now().year)

    st.subheader("Estado LLM")
    st.write(llm_debug_info())

    if st.button("Cerrar sesión"):
        st.session_state.authenticated = False
        st.rerun()

# Utilidad para formatear y descargar
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

# Carga YAML semántico
@st.cache_data(show_spinner=False)
def load_semantic_yaml():
    with open("semantic.yaml", "r", encoding="utf-8") as f:
        return f.read()
semantic_text = ""
try:
    semantic_text = load_semantic_yaml()
except Exception as e:
    st.sidebar.warning(f"No se pudo cargar semantic.yaml: {e}")

tabs = st.tabs(["Preguntar (semántico) + Auto-SQL", "Botones rápidos (6 pruebas)"])

# ------------ TAB 1 ------------
with tabs[0]:
    st.markdown("### Preguntar (semántico) + Auto-SQL de respaldo")
    q = st.text_input("Pregunta", "¿Cuáles son los vehículos entregados que aún no han sido facturas?")
    if st.button("Responder", key="btn_sem"):
        used_path = None
        # 1) Ruta semántica (si hay YAML + API)
        if semantic_text and has_openai():
            parsed = parse_question_to_json(q, semantic_text)
            if parsed and parsed.get("metric"):
                metric = parsed["metric"]
                filters = parsed.get("filters", {}) or {}
                used_path = f"Semántico → {metric}"
                try:
                    if metric == "entregados_sin_factura":
                        df, err = skill_entregados_sin_factura(
                            data.get("MODELO_BOT", next(iter(data.values()))),
                            cliente=filters.get("cliente"),
                            tipo_cliente=filters.get("tipo_cliente"),
                            marca=filters.get("marca"),
                            sucursal=filters.get("sucursal"),
                            asesor=filters.get("asesor"),
                            desde=filters.get("desde"),
                            hasta=filters.get("hasta"),
                        )
                    elif metric == "facturas_por_pagar":
                        df, err = skill_facturas_por_pagar(
                            data.get("FINANZAS"), horizonte_dias=filters.get("horizonte", st.session_state.get("horizonte", 7)),
                            proveedor=filters.get("proveedor")
                        )
                    elif metric == "en_taller":
                        df, err = skill_top_en_taller(
                            data.get("MODELO_BOT", next(iter(data.values()))),
                            topn=filters.get("topn", 10),
                            marca=filters.get("marca"),
                            asesor=filters.get("asesor"),
                            tipo_cliente=filters.get("tipo_cliente"),
                            sucursal=filters.get("sucursal"),
                        )
                    elif metric == "facturacion_mensual_tipo_cliente":
                        df, err = skill_facturacion_por_mes_tipo(
                            data.get("MODELO_BOT", next(iter(data.values()))),
                            int(filters.get("mes", st.session_state.get("mes",  datetime.now().month))),
                            int(filters.get("anio", st.session_state.get("anio", datetime.now().year))),
                        )
                    elif metric == "entregas_proximas_sin_factura":
                        df, err = skill_entregas_proximos_dias_sin_factura(
                            data.get("MODELO_BOT", next(iter(data.values()))),
                            int(filters.get("horizonte", 7)),
                        )
                    elif metric == "sin_aprobacion":
                        df, err = skill_sin_aprobacion(data.get("MODELO_BOT", next(iter(data.values()))))
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
                    used_path = None  # forzar fallback

        # 2) Fallback → Auto-SQL (tu pipeline)
        if used_path is None:
            try:
                prelude_sql, schema_hint = build_duckdb_prelude_and_schema(data)
                params = {"HORIZONTE_DIAS": int(st.session_state.get("horizonte", 7) or 7),
                          "MES": int(st.session_state.get("mes", datetime.now().month)),
                          "ANIO": int(st.session_state.get("anio", datetime.now().year))}
                sql = nl2sql(q, schema_hint=schema_hint, params=params)
                if not sql:
                    st.warning("No pude generar SQL. Revisa el 'Estado LLM' en la barra lateral.")
                    st.info(llm_debug_info())
                else:
                    st.code(sql, language="sql")
                    df = run_duckdb(sql, data, prelude_sql=prelude_sql)
                    if df.empty: st.info("Sin resultados."); 
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

# ------------ TAB 2 ------------
with tabs[1]:
    st.markdown("### Botones rápidos (las 6 preguntas prioritarias)")
    df_main = data.get("MODELO_BOT", next(iter(data.values())))
    df_fin = data.get("FINANZAS", None)

    c = st.columns(2)

    with c[0]:
        st.subheader("1) Entregados sin factura")
        cliente = st.text_input("Cliente (contiene)", key="f1_cli")
        tipo_cli = st.text_input("Tipo cliente (exacto)", key="f1_tc")
        marca = st.text_input("Marca (exacto)", key="f1_marca")
        suc = st.text_input("Sucursal (exacto)", key="f1_suc")
        asesor = st.text_input("Asesor (contiene)", key="f1_ase")
        fdesde = st.text_input("Fecha desde (YYYY-MM-DD)", key="f1_fd")
        fhasta = st.text_input("Fecha hasta (YYYY-MM-DD)", key="f1_fh")
        if st.button("Ejecutar 1"):
            table, err = skill_entregados_sin_factura(df_main, cliente or None, tipo_cli or None, marca or None, suc or None, asesor or None, fdesde or None, fhasta or None)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "entregados_sin_factura")

    with c[1]:
        st.subheader("2) Por pagar próximos días (FINANZAS)")
        prov = st.text_input("Proveedor (contiene)", key="f2_prov")
        if st.button("Ejecutar 2"):
            if df_fin is None: st.warning("No se encontró hoja FINANZAS.")
            else:
                table, err = skill_facturas_por_pagar(df_fin, horizonte_dias=int(horizonte), proveedor=prov or None)
                if err: st.warning(err)
                elif table.empty: st.info("Sin resultados.")
                else: _show(table, "facturas_por_pagar")

    with c[0]:
        st.subheader("3) Top en taller (no entregados)")
        topn = st.number_input("Top N", 1, 100, value=10, key="f3_top")
        marca3 = st.text_input("Marca (exacto)", key="f3_marca")
        asesor3 = st.text_input("Asesor (contiene)", key="f3_ase")
        tipo3 = st.text_input("Tipo cliente (exacto)", key="f3_tc")
        suc3 = st.text_input("Sucursal (exacto)", key="f3_suc")
        if st.button("Ejecutar 3"):
            table, err = skill_top_en_taller(df_main, int(topn), marca3 or None, asesor3 or None, tipo3 or None, suc3 or None)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                _show(table, "top_en_taller")
                try: st.plotly_chart(px.bar(table, x=table.columns[0], y="dias_en_taller"), use_container_width=True)
                except Exception: pass

    with c[1]:
        st.subheader("4) Facturación por mes / tipo cliente")
        if st.button("Ejecutar 4"):
            table, err = skill_facturacion_por_mes_tipo(df_main, int(mes), int(anio))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                _show(table, "facturacion_mes_tipo")
                try: st.plotly_chart(px.pie(table, names=table.columns[0], values="monto"), use_container_width=True)
                except Exception: pass

    with c[0]:
        st.subheader("5) Entregas próximos días SIN facturación")
        if st.button("Ejecutar 5"):
            table, err = skill_entregas_proximos_dias_sin_factura(df_main, int(horizonte))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "entregas_proximas_sin_factura")

    with c[1]:
        st.subheader("6) En taller sin aprobación (proxy)")
        if st.button("Ejecutar 6"):
            table, err = skill_sin_aprobacion(df_main)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else: _show(table, "sin_aprobacion")

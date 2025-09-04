# app.py — Agente Fénix (failsafe)
# ---------------------------------------------------------------------
# Requiere los módulos locales (pueden estar en "fenix_utils" o en "utils"):
#   gsheets.py, login.py, formatters.py, llm.py, md.py, schema.py, skills.py
# Y los assets:
#   assets/Nexa_logo.png, assets/Fenix_isotipo.png, assets/Isotipo_Nexa.png
# ---------------------------------------------------------------------

import os, sys, io, base64
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px

# ============== Page config + debug panel (no crashea por favicon) ==============
ICON_PATH = "assets/Isotipo_Nexa.png"
try:
    st.set_page_config(
        page_title="Agente Fénix",
        page_icon=ICON_PATH if os.path.exists(ICON_PATH) else None,
        layout="wide",
    )
except Exception:
    st.set_page_config(page_title="Agente Fénix", page_icon=None, layout="wide")

def get_debug_flag() -> bool:
    try:
        q = st.query_params
        return str(q.get("debug", "0")) in ("1", "true", "True")
    except Exception:
        q = st.experimental_get_query_params()
        v = q.get("debug", ["0"])
        return (v and v[0] in ("1", "true", "True"))

if get_debug_flag():
    st.sidebar.title("🛠️ Debug")
    st.sidebar.write("cwd:", os.getcwd())
    try: st.sidebar.write("files:", sorted(os.listdir(".")))
    except Exception as e: st.sidebar.write("files error:", e)
    try: st.sidebar.write("assets:", sorted(os.listdir("assets")) if os.path.isdir("assets") else "NO assets/")
    except Exception as e: st.sidebar.write("assets error:", e)
    try: st.sidebar.write("secrets keys:", list(st.secrets.keys()))
    except Exception as e: st.sidebar.write("secrets error:", e)
    st.sidebar.write("python:", sys.version)
    st.sidebar.write("sys.path[0:3]:", sys.path[:3])
    st.sidebar.info("Quita ?debug=1 para ocultar el panel")

# Exporta OPENAI_API_KEY desde secrets si existe
try:
    if "OPENAI_API_KEY" not in os.environ and st.secrets.get("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = st.secrets["OPENAI_API_KEY"]
except Exception:
    pass

# ============== Imports locales (intenta fenix_utils y luego utils) ==============
def import_local():
    """Intenta importar desde fenix_utils/ o utils/ y devuelve el paquete base."""
    err = []
    for pkg in ("fenix_utils", "utils"):
        try:
            base = __import__(pkg)
            return pkg, None
        except Exception as e:
            err.append(f"{pkg}: {e}")
    return None, "\n".join(err)

pkg_base, import_error = import_local()
if not pkg_base:
    st.error("No pude importar los módulos locales. Revisa la carpeta **fenix_utils/** o **utils/**.")
    st.code(import_error or "(sin detalles)")
    st.markdown(
        "- Asegúrate de tener **gsheets.py, login.py, formatters.py, llm.py, md.py, schema.py, skills.py**.\n"
        "- Si la carpeta se llama `utils/`, debe existir **utils/__init__.py** (vacío) para que sea un paquete."
    )
    st.stop()

# Construtor de imports dinámicos (para no duplicar código)
def imp(mod, names):
    m = __import__(f"{pkg_base}.{mod}", fromlist=names)
    return [getattr(m, n) for n in names]

(
    load_sheets,
) = imp("gsheets", ["load_sheets"])
(
    ensure_login,
) = imp("login", ["ensure_login"])
(
    format_currency_clp, format_date_ddmmyyyy,
) = imp("formatters", ["format_currency_clp", "format_date_ddmmyyyy"])
(
    summarize_markdown, nl2sql, run_duckdb,
) = imp("llm", ["summarize_markdown", "nl2sql", "run_duckdb"])
(
    df_to_md,
) = imp("md", ["df_to_md"])
(
    build_duckdb_prelude_and_schema,
) = imp("schema", ["build_duckdb_prelude_and_schema"])
(
    skill_entregados_sin_factura,
    skill_facturas_por_pagar,
    skill_top_en_taller,
    skill_facturacion_por_mes_tipo,
    skill_entregas_proximos_dias_sin_factura,
    skill_sin_aprobacion,
) = imp(
    "skills",
    [
        "skill_entregados_sin_factura",
        "skill_facturas_por_pagar",
        "skill_top_en_taller",
        "skill_facturacion_por_mes_tipo",
        "skill_entregas_proximos_dias_sin_factura",
        "skill_sin_aprobacion",
    ],
)

# ============== Login (si falla la UI del login, es porque no llegó hasta aquí) ==============
ensure_login()

# ============== Logos ==============
def _b64(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return ""

fenix_b64 = _b64("assets/Fenix_isotipo.png")
if fenix_b64:
    st.markdown(
        f"""
        <style>.top-right-logo {{ position: fixed; top: 12px; right: 16px; z-index: 1000; }}</style>
        <div class="top-right-logo"><img src="data:image/png;base64,{fenix_b64}" height="40"/></div>
        """,
        unsafe_allow_html=True,
    )

with st.sidebar:
    try: st.image("assets/Nexa_logo.png", use_container_width=True)
    except Exception: pass
    st.markdown("---")

st.title("🛠️ Agente Fénix")
st.caption("Responde como ChatGPT, con cálculos deterministas sobre MODELO_BOT y FINANZAS.")

# ============== Conexión Google Sheets ==============
sheet_id = st.secrets.get("SHEET_ID", "")
with st.sidebar:
    st.subheader("Conexión")
    allow = ("MODELO_BOT", "FINANZAS")
    try:
        data = load_sheets(sheet_id, allow_sheets=allow)
        st.success("Google Sheets conectado (solo lectura).")
        st.write("Hojas:", ", ".join(data.keys()))
    except Exception as e:
        st.error(f"Error al conectar: {e}")
        st.info("Tips: verifica SHEET_ID, comparte el archivo con el client_email de la service account (Viewer).")
        st.stop()

    st.subheader("Preferencias")
    horizonte = st.number_input("Días (próximos)", min_value=1, max_value=60, value=7, step=1)
    mes = st.number_input("Mes para facturación", 1, 12, value=datetime.now().month)
    anio = st.number_input("Año para facturación", 2000, 2100, value=datetime.now().year)
    if st.button("Cerrar sesión"):
        st.session_state.authenticated = False
        st.rerun()

# ============== Utilidades de formato/descarga ==============
def fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        lc = c.lower()
        if any(k in lc for k in ["monto", "total", "valor", "neto", "bruto", "importe"]):
            out[c] = out[c].map(format_currency_clp)
        if "fecha" in lc:
            out[c] = out[c].map(format_date_ddmmyyyy)
    return out

def show_table_and_download(df: pd.DataFrame, name: str):
    st.dataframe(fmt_df(df), use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"),
                           f"{name}.csv", "text/csv")
    with c2:
        buff = io.BytesIO()
        with pd.ExcelWriter(buff, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Datos")
        st.download_button(
            "⬇️ XLSX",
            buff.getvalue(),
            f"{name}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ============== UI: Tabs ==============
tabs = st.tabs(["Preguntar libre (Auto-SQL)", "Botones rápidos (6 pruebas)"])

# ---------- Tab 1: Auto-SQL ----------
with tabs[0]:
    st.markdown("### Preguntar libre (Auto-SQL)")
    st.caption("Tu pregunta → SQL DuckDB seguro sobre las vistas MB/FIN.")

    q = st.text_input(
        "Pregunta",
        "¿Cuáles son los vehículos entregados que aún no han sido facturas?",
        key="input_free_question",
    )

    if st.button("Responder", key="btn_free_question"):
        try:
            prelude_sql, schema_hint = build_duckdb_prelude_and_schema(data)
        except Exception as e:
            st.error(f"Error generando vistas: {e}")
            st.stop()

        params = {"HORIZONTE_DIAS": int(horizonte), "MES": int(mes), "ANIO": int(anio)}
        sql = nl2sql(q, schema_hint=schema_hint, params=params)

        if not sql:
            st.warning("No pude generar SQL (¿falta OPENAI_API_KEY?). Usa los botones rápidos.")
        else:
            st.code(sql, language="sql")
            try:
                df = run_duckdb(sql, data, prelude_sql=prelude_sql)

                # Mover ID (patente/OT) primero si existe
                cols = list(df.columns)
                id_col = next((c for c in cols if c.lower() in ("id", "patente", "placa")), None)
                if not id_col:
                    id_col = next((c for c in cols if c.lower() in ("ot", "orden de trabajo")), None)
                if id_col:
                    df = df[[id_col] + [c for c in cols if c != id_col]]

                if df.empty:
                    st.info("Sin resultados.")
                else:
                    show_table_and_download(df, "consulta_autosql")
                    # Gráfico simple si hay 1 categórica + 1 numérica
                    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                    cat_cols = [c for c in df.columns if df[c].dtype == object]
                    if num_cols and cat_cols:
                        try:
                            st.plotly_chart(px.bar(df, x=cat_cols[0], y=num_cols[0]), use_container_width=True)
                        except Exception:
                            pass
                    # Resumen tipo ChatGPT (si hay API)
                    try:
                        st.markdown("### Resumen")
                        st.write(summarize_markdown(df_to_md(df), q))
                    except Exception as e:
                        st.info(f"(Resumen no disponible) {e}")
            except Exception as e:
                st.error(f"Error al ejecutar SQL: {e}")

# ---------- Tab 2: Botones rápidos ----------
with tabs[1]:
    st.markdown("### Botones rápidos (las 6 preguntas prioritarias)")
    st.caption("Usan reglas deterministas y soportan multi-filtros.")

    df_main = data.get("MODELO_BOT", next(iter(data.values())))
    df_fin = data.get("FINANZAS", None)

    col = st.columns(2)

    # 1) Entregados sin factura
    with col[0]:
        st.subheader("1) Entregados sin factura")
        cliente = st.text_input("Filtro cliente (contiene)", key="f1_cli")
        tipo_cli = st.text_input("Tipo cliente (exacto)", key="f1_tc")
        marca = st.text_input("Marca (exacto)", key="f1_marca")
        suc = st.text_input("Sucursal (exacto)", key="f1_suc")
        asesor = st.text_input("Asesor (contiene)", key="f1_ase")
        fdesde = st.text_input("Fecha desde (YYYY-MM-DD)", key="f1_fd")
        fhasta = st.text_input("Fecha hasta (YYYY-MM-DD)", key="f1_fh")

        if st.button("Ejecutar 1"):
            table, err = skill_entregados_sin_factura(
                df_main,
                cliente=cliente or None,
                tipo_cliente=tipo_cli or None,
                marca=marca or None,
                sucursal=suc or None,
                asesor=asesor or None,
                fecha_desde=fdesde or None,
                fecha_hasta=fhasta or None,
            )
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "entregados_sin_factura")
                st.write(summarize_markdown(df_to_md(table), "Entregados sin factura"))

    # 2) Facturas a pagar próximos días
    with col[1]:
        st.subheader("2) Facturas a pagar próximos días (FINANZAS)")
        prov = st.text_input("Proveedor (contiene)", key="f2_prov")
        if st.button("Ejecutar 2"):
            if df_fin is None:
                st.warning("No se encontró hoja FINANZAS.")
            else:
                table, err = skill_facturas_por_pagar(df_fin, horizonte_dias=int(horizonte), proveedor=prov or None)
                if err: st.warning(err)
                elif table.empty: st.info("Sin resultados.")
                else:
                    show_table_and_download(table, "facturas_por_pagar")
                    st.write(summarize_markdown(df_to_md(table), "Facturas por pagar"))

    # 3) Top en taller
    with col[0]:
        st.subheader("3) Top en taller (no entregados)")
        topn = st.number_input("Top N", 1, 100, value=10, key="f3_top")
        marca3 = st.text_input("Marca (exacto)", key="f3_marca")
        asesor3 = st.text_input("Asesor (contiene)", key="f3_ase")
        tipo3 = st.text_input("Tipo cliente (exacto)", key="f3_tc")
        suc3 = st.text_input("Sucursal (exacto)", key="f3_suc")
        if st.button("Ejecutar 3"):
            table, err = skill_top_en_taller(
                df_main, top_n=int(topn),
                marca=marca3 or None, asesor=asesor3 or None,
                tipo_cliente=tipo3 or None, sucursal=suc3 or None
            )
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "top_en_taller")
                try:
                    st.plotly_chart(px.bar(table, x=table.columns[0], y="Días en taller"), use_container_width=True)
                except Exception:
                    pass

    # 4) Facturación por mes / tipo cliente
    with col[1]:
        st.subheader("4) Facturación por mes / tipo cliente")
        if st.button("Ejecutar 4"):
            table, err = skill_facturacion_por_mes_tipo(df_main, mes=int(mes), anio=int(anio))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "facturacion_mes_tipo")
                try:
                    st.plotly_chart(px.pie(table, names=table.columns[0], values="monto"), use_container_width=True)
                except Exception:
                    pass

    # 5) Entregas próximos días SIN facturación
    with col[0]:
        st.subheader("5) Entregas próximos días SIN facturación")
        if st.button("Ejecutar 5"):
            table, err = skill_entregas_proximos_dias_sin_factura(df_main, horizonte_dias=int(horizonte))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "entregas_proximas_sin_factura")

    # 6) En taller sin aprobación
    with col[1]:
        st.subheader("6) En taller sin aprobación (proxy)")
        if st.button("Ejecutar 6"):
            table, err = skill_sin_aprobacion(df_main)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "sin_aprobacion")
                st.write(summarize_markdown(df_to_md(table), "En taller sin aprobación"))

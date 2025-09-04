import os, base64, io
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from utils.gsheets import load_sheets
from utils.skills import (
    skill_entregados_sin_factura,
    skill_facturas_por_pagar,
    skill_top_en_taller,
    skill_facturacion_por_mes_tipo,
    skill_entregas_proximos_dias_sin_factura,
    skill_sin_aprobacion,
)
from utils.formatters import format_currency_clp, format_date_ddmmyyyy
from utils.llm import summarize_markdown, nl2sql, run_duckdb
from utils.login import ensure_login
from utils.md import df_to_md
from utils.schema import build_duckdb_prelude_and_schema


# ---- Page config ----
st.set_page_config(
    page_title="Agente Fénix",
    page_icon="assets/Isotipo_Nexa.png",
    layout="wide",
)

# ---- Login ----
ensure_login()

# ---- Fénix isotipo arriba a la derecha ----
def _b64(path: str) -> str:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return ""

FENIX_B64 = _b64("assets/Fenix_isotipo.png")
if FENIX_B64:
    st.markdown(
        f"""
        <style>
        .top-right-logo {{
            position: fixed; top: 12px; right: 16px; z-index: 1000;
        }}
        </style>
        <div class="top-right-logo">
            <img src="data:image/png;base64,{FENIX_B64}" height="40"/>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---- Sidebar: logo Nexa + conexión + preferencias ----
with st.sidebar:
    try:
        st.image("assets/Nexa_logo.png", use_container_width=True)
    except Exception:
        pass
    st.markdown("---")

st.title("🛠️ Agente Fénix")
st.caption("Responde como ChatGPT, pero con números exactos desde MODELO_BOT y FINANZAS.")

# ---------- Conexión a Google Sheets ----------
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
        st.stop()

    st.subheader("Preferencias")
    horizonte = st.number_input("Días (próximos)", min_value=1, max_value=60, value=7, step=1)
    mes = st.number_input("Mes para facturación", 1, 12, value=datetime.now().month)
    anio = st.number_input("Año para facturación", 2000, 2100, value=datetime.now().year)

    if st.button("Cerrar sesión"):
        st.session_state.authenticated = False
        if hasattr(st, "rerun"):
            st.rerun()
        else:
            st.experimental_rerun()

# ---------- Utilidades de formato ----------
def fmt_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        cn = c.lower()
        if any(k in cn for k in ["monto", "total", "valor", "neto", "bruto", "importe"]):
            out[c] = out[c].map(format_currency_clp)
        if "fecha" in cn:
            out[c] = out[c].map(format_date_ddmmyyyy)
    return out

def show_table_and_download(df: pd.DataFrame, name: str):
    st.dataframe(fmt_df(df), use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇️ CSV", df.to_csv(index=False).encode("utf-8"), f"{name}.csv", "text/csv")
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

# ---------- Pestañas ----------
tabs = st.tabs([
    "Preguntar libre (Auto-SQL)",
    "Botones rápidos (6 pruebas)",
])

# ---------- Tab 1: Auto-SQL ----------
with tabs[0]:
    st.markdown("### Preguntar libre (Auto-SQL)")
    st.caption("El modelo traduce tu pregunta a SQL seguro (solo SELECT) sobre vistas MB/FIN.")

    q = st.text_input("Pregunta", "¿Cuáles son los vehículos entregados que aún no han sido facturas?")
    if st.button("Responder", key="free_q"):
        # 1) Construye MB/FIN como vistas en DuckDB + schema para el LLM
        prelude_sql, schema_hint = build_duckdb_prelude_and_schema(data)
        params = {"HORIZONTE_DIAS": int(horizonte), "MES": int(mes), "ANIO": int(anio)}

        # 2) NL -> SQL
        sql = nl2sql(q, schema_hint=schema_hint, params=params)
        if not sql:
            st.warning("No pude generar SQL automático (o falta OPENAI_API_KEY). Usa los botones rápidos.")
        else:
            st.code(sql, language="sql")
            try:
                df = run_duckdb(sql, data, prelude_sql=prelude_sql)

                # Reordenar: ID (patente/placa u OT) primero si existe
                cols = list(df.columns)
                id_col = next((c for c in cols if c.lower() in ("id", "patente", "placa")), None)
                if not id_col:
                    id_col = next((c for c in cols if c.lower() in ("ot", "orden de trabajo")), None)
                if id_col:
                    cols = [id_col] + [c for c in cols if c != id_col]
                    df = df[cols]

                if df.empty:
                    st.info("Sin resultados.")
                else:
                    show_table_and_download(df, "consulta_autosql")
                    # Gráfico básico si hay 1 categórica + 1 numérica
                    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                    cat_cols = [c for c in df.columns if df[c].dtype == object]
                    if num_cols and cat_cols:
                        fig = px.bar(df, x=cat_cols[0], y=num_cols[0])
                        st.plotly_chart(fig, use_container_width=True)
                    st.markdown("### Resumen")
                    st.write(summarize_markdown(df_to_md(df), q))
            except Exception as e:
                st.error(f"Error al ejecutar SQL: {e}")

# ---------- Tab 2: Botones rápidos ----------
with tabs[1]:
    st.markdown("### Botones rápidos (las 6 preguntas prioritarias)")
    st.caption("Usan reglas deterministas y soportan multi-filtros.")

    df_main = data.get("MODELO_BOT", next(iter(data.values())))
    df_fin = data.get("FINANZAS", None)

    col = st.columns(2)

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
                fig = px.bar(table, x=table.columns[0], y="Días en taller")
                st.plotly_chart(fig, use_container_width=True)

    with col[1]:
        st.subheader("4) Facturación por mes / tipo cliente")
        if st.button("Ejecutar 4"):
            table, err = skill_facturacion_por_mes_tipo(df_main, mes=int(mes), anio=int(anio))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "facturacion_mes_tipo")
                fig = px.pie(table, names=table.columns[0], values="monto")
                st.plotly_chart(fig, use_container_width=True)

    with col[0]:
        st.subheader("5) Entregas próximos días SIN facturación")
        if st.button("Ejecutar 5"):
            table, err = skill_entregas_proximos_dias_sin_factura(df_main, horizonte_dias=int(horizonte))
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "entregas_proximas_sin_factura")

    with col[1]:
        st.subheader("6) En taller sin aprobación (presupuesto enviado ≠ perdido)")
        if st.button("Ejecutar 6"):
            table, err = skill_sin_aprobacion(df_main)
            if err: st.warning(err)
            elif table.empty: st.info("Sin resultados.")
            else:
                show_table_and_download(table, "sin_aprobacion")
                st.write(summarize_markdown(df_to_md(table), "En taller sin aprobación"))

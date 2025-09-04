import os, base64
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from utils.gsheets import load_sheets
from utils.skills import (
    entregados_sin_factura,
    facturas_por_pagar,
    top_en_taller,
    facturacion_por_mes_tipo,
    entregas_proximos_dias_sin_factura,
    sin_aprobacion,
)
from utils.formatters import format_currency_clp, format_date_ddmmyyyy
from utils.llm import summarize_markdown, nl2sql, run_duckdb
from utils.login import ensure_login


# ---- Page config con favicon (isotipo Nexa) ----
st.set_page_config(
    page_title="Agente Fénix",
    page_icon="assets/Isotipo_Nexa.png",
    layout="wide",
)

# ---- Login ----
ensure_login()

# ---- Isotipo Fénix arriba a la derecha (flotante) ----
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

# ---- Sidebar con logo Nexa y controles ----
with st.sidebar:
    try:
        st.image("assets/Nexa_logo.png", use_column_width=True)
    except Exception:
        pass
    st.markdown("---")

st.title("🛠️ Agente Fénix")
st.caption("Responde como ChatGPT, con cálculos deterministas sobre MODELO_BOT y FINANZAS.")

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
        if any(k in cn for k in ["monto", "total", "valor", "neto"]):
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
        import io
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
    st.caption("El modelo traduce tu pregunta a SQL seguro (solo SELECT) sobre MODELO_BOT y FINANZAS.")

    q = st.text_input("Pregunta", "Facturación de marzo por tipo de cliente")
    if st.button("Responder", key="free_q"):
        # Esquema textual para el LLM
        schema_lines = []
        for name, df in data.items():
            cols = ", ".join([f"`{c}`" for c in df.columns])
            schema_lines.append(f"Tabla {name}({cols})")
        schema = "\n".join(schema_lines)

        sql = nl2sql(q, schema_hint=schema)
        if not sql:
            st.warning("No pude generar SQL automático (o falta OPENAI_API_KEY). Usa los botones rápidos.")
        else:
            st.code(sql, language="sql")
            try:
                df = run_duckdb(sql, data)
                if df.empty:
                    st.info("Sin resultados.")
                else:
                    show_table_and_download(df, "consulta_autosql")
                    # Gráfico básico si hay 1 cat + 1 num
                    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                    cat_cols = [c for c in df.columns if df[c].dtype == object]
                    if num_cols and cat_cols:
                        fig = px.bar(df, x=cat_cols[0], y=num_cols[0])
                        st.plotly_chart(fig, use_container_width=True)
                    st.markdown("### Resumen")
                    st.write(summarize_markdown(df.head(50).to_markdown(index=False), q))
            except Exception as e:
                st.error(f"Error al ejecutar SQL: {e}")

# ---------- Tab 2: Botones rápidos ----------
with tabs[1]:
    st.markdown("### Botones rápidos (las 6 preguntas prioritarias)")
    st.caption("Usan reglas deterministas y son más robustas que el Auto-SQL.")

    df_main = data.get("MODELO_BOT", next(iter(data.values())))
    df_fin = data.get("FINANZAS", None)

    col = st.columns(2)

    with col[0]:
        st.subheader("1) Entregados sin factura")
        if st.button("Ejecutar 1"):
            table, err = entregados_sin_factura(df_main)
            if err:
                st.warning(err)
            elif table.empty:
                st.info("Sin resultados.")
            else:
                show_table_and_download(table, "entregados_sin_factura")
                st.write(summarize_markdown(table.head(50).to_markdown(index=False), "Entregados sin factura"))

    with col[1]:
        st.subheader("2) Facturas a pagar próximos días")
        if st.button("Ejecutar 2"):
            if df_fin is None:
                st.warning("No se encontró hoja FINANZAS.")
            else:
                table, err = facturas_por_pagar(df_fin, horizonte_dias=st.session_state.get("horizonte", 7) or 7)
                if err:
                    st.warning(err)
                elif table.empty:
                    st.info("Sin resultados.")
                else:
                    show_table_and_download(table, "facturas_por_pagar")
                    st.write(summarize_markdown(table.head(50).to_markdown(index=False), "Facturas por pagar"))

    with col[0]:
        st.subheader("3) Top 10 en taller (no entregados)")
        if st.button("Ejecutar 3"):
            table, err = top_en_taller(df_main, top_n=10)
            if err:
                st.warning(err)
            elif table.empty:
                st.info("Sin resultados.")
            else:
                show_table_and_download(table, "top_en_taller")
                fig = px.bar(table, x=table.columns[0], y="Días en taller")
                st.plotly_chart(fig, use_container_width=True)

    with col[1]:
        st.subheader("4) Facturación por mes / tipo cliente")
        if st.button("Ejecutar 4"):
            table, err = facturacion_por_mes_tipo(df_main, mes=int(st.session_state.get("mes", datetime.now().month) or datetime.now().month), anio=int(st.session_state.get("anio", datetime.now().year) or datetime.now().year))
            if err:
                st.warning(err)
            elif table.empty:
                st.info("Sin resultados.")
            else:
                show_table_and_download(table, "facturacion_mes_tipo")
                fig = px.pie(table, names=table.columns[0], values="monto")
                st.plotly_chart(fig, use_container_width=True)

    with col[0]:
        st.subheader("5) Entregas próximos días SIN facturación")
        if st.button("Ejecutar 5"):
            table, err = entregas_proximos_dias_sin_factura(df_main, horizonte_dias=st.session_state.get("horizonte", 7) or 7)
            if err:
                st.warning(err)
            elif table.empty:
                st.info("Sin resultados.")
            else:
                show_table_and_download(table, "entregas_proximas_sin_factura")

    with col[1]:
        st.subheader("6) En taller sin aprobación (presupuesto enviado ≠ perdido)")
        if st.button("Ejecutar 6"):
            table, err = sin_aprobacion(df_main)
            if err:
                st.warning(err)
            elif table.empty:
                st.info("Sin resultados.")
            else:
                show_table_and_download(table, "sin_aprobacion")
                st.write(summarize_markdown(table.head(50).to_markdown(index=False), "En taller sin aprobación"))

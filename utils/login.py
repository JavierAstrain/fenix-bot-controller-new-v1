import streamlit as st

def ensure_login():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    # --- Login UI ---
    st.markdown("<div style='text-align:center'>", unsafe_allow_html=True)
    st.image("assets/Isotipo_Nexa.png", width=96)
    st.image("assets/Fenix_isotipo.png", width=96)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### 🔐 Iniciar sesión")
    u = st.text_input("Usuario", value="", key="login_user")
    p = st.text_input("Contraseña", value="", type="password", key="login_pass")
    if st.button("Ingresar", type="primary"):
        user = st.secrets.get("APP_USERNAME", "admin")
        pwd = st.secrets.get("APP_PASSWORD", "admin")
        if u == user and p == pwd:
            st.session_state.authenticated = True
            st.success("Autenticado.")
            if hasattr(st, "rerun"):
    st.rerun()
else:
    st.experimental_rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")
    st.stop()

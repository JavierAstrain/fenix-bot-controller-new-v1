import streamlit as st

def ensure_login():
    if st.session_state.get("authenticated"):
        return
    st.markdown(
        """
        <style>
        .login-card {
            max-width: 420px; margin: 8vh auto; padding: 28px;
            border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,.08);
            background: white;
        }
        .brand-logos { display:flex; justify-content:space-between; align-items:center; margin-bottom: 12px; }
        </style>
        """, unsafe_allow_html=True
    )
    with st.container():
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        col1, col2 = st.columns([1,1])
        with col1: st.image("assets/Isotipo_Nexa.png", width=56)
        with col2: st.image("assets/Fenix_isotipo.png", width=56)
        st.markdown("### Agente Fénix — Iniciar sesión")
        user = st.text_input("Usuario")
        pwd = st.text_input("Contraseña", type="password")
        if st.button("Ingresar"):
            if user == st.secrets.get("APP_USERNAME") and pwd == st.secrets.get("APP_PASSWORD"):
                st.session_state.authenticated = True
                st.experimental_rerun()
            else:
                st.error("Credenciales inválidas.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

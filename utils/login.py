import streamlit as st


def ensure_login():
    """
    Login básico con logos (isotipos) y credenciales tomadas de st.secrets.
    Claves aceptadas:
      - APP_USERNAME / APP_PASSWORD  (recomendado)
      - USER / PASSWORD              (fallback)
    """
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    # --- Login UI (ambos isotipos) ---
    st.markdown("<div style='text-align:center'>", unsafe_allow_html=True)
    try:
        st.image("assets/Isotipo_Nexa.png", width=96)
    except Exception:
        pass
    try:
        st.image("assets/Fenix_isotipo.png", width=96)
    except Exception:
        pass
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### 🔐 Iniciar sesión")
    u = st.text_input("Usuario", value="", key="login_user")
    p = st.text_input("Contraseña", value="", type="password", key="login_pass")

    if st.button("Ingresar", type="primary"):
        # Lee primero APP_USERNAME/APP_PASSWORD y, si no existen, intenta USER/PASSWORD.
        user = st.secrets.get("APP_USERNAME", st.secrets.get("USER", "admin"))
        pwd = st.secrets.get("APP_PASSWORD", st.secrets.get("PASSWORD", "admin"))

        if u == user and p == pwd:
            st.session_state.authenticated = True
            st.success("Autenticado.")
            # Streamlit moderno usa st.rerun(); en versiones antiguas queda experimental_rerun
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")

    # Bloquea el resto de la app hasta iniciar sesión
    st.stop()

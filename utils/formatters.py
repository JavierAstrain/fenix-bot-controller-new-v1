import pandas as pd

def format_currency_clp(x):
    try:
        v = pd.to_numeric(str(x).replace(".","").replace(",","."), errors="coerce")
    except Exception:
        return x
    if pd.isna(v):
        return ""
    return f"$ {int(round(v,0)):,}".replace(",", ".")

def format_date_ddmmyyyy(s):
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(d):
        return ""
    return d.strftime("%d-%m-%Y")

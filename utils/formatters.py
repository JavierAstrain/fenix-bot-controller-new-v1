from datetime import datetime

def format_currency_clp(x):
    if x is None: return ""
    try:
        v = float(str(x).replace(".", "").replace(",", "."))
    except:
        return str(x)
    # Formato: $ 1.234.567 (sin decimales)
    return f"$ {int(round(v)):n}".replace(",", ".")
    
def format_date_ddmmyyyy(x):
    if not x: return ""
    s = str(x)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime("%d-%m-%Y")
        except:
            continue
    return s

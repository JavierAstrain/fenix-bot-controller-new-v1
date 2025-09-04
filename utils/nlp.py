import re
import pandas as pd

def norm(s: str) -> str:
    if s is None: return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = (s.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ñ","n"))
    return s

def find_col(df: pd.DataFrame, keys: list[str]) -> str | None:
    cols = {norm(c): c for c in df.columns}
    keysn = [norm(k) for k in keys]
    for k in keysn:
        if k in cols: return cols[k]
    for k in keysn:
        for n, c in cols.items():
            if k in n: return c
    return None

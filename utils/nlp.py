import re

def find_col(df, synonyms):
    """Busca la primera columna que contenga alguno de los términos (case-insensitive)."""
    cols = list(df.columns)
    lcols = [c.lower() for c in cols]
    for syn in synonyms:
        s = syn.lower()
        for i, lc in enumerate(lcols):
            if s in lc:
                return cols[i]
    return None

def ilike(series, term):
    """Filtro 'contiene' case-insensitive sobre series de texto."""
    if term is None or term == "": return series.astype(str).str.len() >= 0
    return series.astype(str).str.contains(re.escape(term), case=False, na=False)

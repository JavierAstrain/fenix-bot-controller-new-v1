import pandas as pd

def df_to_md(df: pd.DataFrame) -> str:
    """Devuelve una vista markdown (o CSV si no hay tabulate)."""
    try:
        # pandas usa 'tabulate' por debajo
        return df.head(50).to_markdown(index=False)
    except Exception:
        # Fallback sin tabulate
        return df.head(50).to_csv(index=False)

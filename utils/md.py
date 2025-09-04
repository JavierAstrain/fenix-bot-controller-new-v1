import pandas as pd

def df_to_md(df: pd.DataFrame) -> str:
    try:
        return df.head(50).to_markdown(index=False)
    except Exception:
        return df.head(50).to_csv(index=False)

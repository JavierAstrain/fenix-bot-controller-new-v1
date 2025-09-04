# utils/llm_guard.py
import pandas as pd
from typing import Callable

def verify_and_refine(
    df: pd.DataFrame | None,
    expected_cols: list[str],
    refine_fn: Callable[[dict], pd.DataFrame | None] | None,
    max_retry: int = 1
) -> pd.DataFrame | None:
    """
    Reglas simples: columnas esperadas y filas >= 0.
    Si falla, ejecuta refine_fn(feedback) una vez.
    """
    if isinstance(df, pd.DataFrame) and set(expected_cols).issubset(df.columns):
        return df
    if max_retry <= 0 or refine_fn is None:
        return df
    fb = {
        "have_cols": [] if df is None else list(df.columns),
        "expected": expected_cols,
        "rows": 0 if df is None else len(df),
    }
    return refine_fn(fb)

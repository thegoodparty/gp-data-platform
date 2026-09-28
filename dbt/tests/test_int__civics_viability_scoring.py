"""Unit tests for the pure pandas helpers of the civics viability scorer.

These guard the pandas round-trip that feeds `spark.createDataFrame(...)`.
The scorer hands Spark a list of dicts rather than a pandas DataFrame, so
Spark infers each column's type from the Python values -- and a NaN in a
string column infers as the literal text "nan" instead of a null.
"""

import numpy as np
import pandas as pd
from dbt.project.models.intermediate.civics.int__civics_viability_scoring import (
    _string_nans_to_none,
)


def test_string_column_nans_become_none():
    """A missing provenance value must reach Spark as None, not the text "nan"."""
    df = pd.DataFrame(
        {
            "log_n_losers_source": pd.Series(["native", np.nan, "roster"]),
            "log_n_losers": [0.5, 1.5, 2.5],
        }
    )

    out = _string_nans_to_none(df)

    assert out["log_n_losers_source"].tolist() == ["native", None, "roster"]


def test_numeric_column_nans_are_left_alone():
    """Score NaNs must stay NaN so Spark still infers a double, not a null column.

    An all-None column gives Spark nothing to infer from; the scorer's
    unscored-model columns are entirely NaN, so converting them would break
    schema inference. The `isnan` whitelist downstream nulls them instead.
    """
    df = pd.DataFrame({"y_score0a": [0.1, np.nan], "gp_candidacy_id": ["a", "b"]})

    out = _string_nans_to_none(df)

    assert pd.api.types.is_numeric_dtype(out["y_score0a"])
    assert out["y_score0a"].isna().tolist() == [False, True]

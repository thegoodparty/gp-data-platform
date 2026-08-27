"""JSON serialization helper shared by the read and write sides.

Both `cli._serialize_array_value` (read side) and
`databricks_io._coerce_to_string_df` (write side) turn array-valued cells into
JSON strings, and both meet numpy types json refuses. The hook lives here so
the two stay in step.

Note this is deliberately not used by `pipeline.save_results.to_json`, which
takes a stricter line on purpose: it sends anything that is not a plain list or
ndarray to an empty-array sentinel so a numpy scalar in a list cell cannot emit
a bare NaN token, which is not valid JSON.
"""

import numpy as np


def json_fallback(o):
    """Unwrap the numpy types json refuses, for use as `json.dumps(default=...)`.

    ndarray.tolist() unwraps only the outer level, so a nested array column
    arrives with inner ndarrays that json cannot serialize on its own.
    """
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, np.generic):
        return o.item()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

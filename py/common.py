# py/common.py

import os

import pandas as pd


def ensure_output_dir():

    out = os.getenv("OUTPUT_DIR", "stage")

    os.makedirs(out, exist_ok=True)

    return out


def format_dates(start: str, end: str = None):

    """

    Return list of YYYY-MM-DD strings from start to end (inclusive).

    If end is None, returns [start].

    """

    start_ts = pd.to_datetime(start)

    if end is None:

        return [start_ts.strftime("%Y-%m-%d")]
    
    end_ts = pd.to_datetime(end)

    days = (end_ts - start_ts).days

    return [

        (start_ts + pd.Timedelta(days=i)).strftime("%Y-%m-%d")

        for i in range(days + 1)

    ]


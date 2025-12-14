from __future__ import annotations
import requests
import pandas as pd

WB_BASE = "https://api.worldbank.org/v2"

def fetch_wdi_indicator(
    countries: list[str],
    indicator: str,
    start: int = 2005,
    end: int = 2030,
) -> pd.DataFrame:
    """Return tidy: iso3c, year, value, indicator."""
    frames = []
    for iso3 in countries:
        url = f"{WB_BASE}/country/{iso3}/indicator/{indicator}"
        params = {"format": "json", "per_page": 20000, "date": f"{start}:{end}"}
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        if not isinstance(data, list) or len(data) < 2 or data[1] is None:
            continue

        rows = []
        for obs in data[1]:
            if not obs:
                continue
            val = obs.get("value", None)
            year = obs.get("date", None)
            if year is None:
                continue
            rows.append(
                {"iso3c": iso3, "year": int(year), "value": val, "indicator": indicator}
            )

        if rows:
            frames.append(pd.DataFrame(rows))

    if not frames:
        return pd.DataFrame(columns=["iso3c", "year", "value", "indicator"])
    return pd.concat(frames, ignore_index=True)

from __future__ import annotations

import pandas as pd
from pandasdmx import Request


def fetch_weo(
    countries: list[str],
    indicators: list[str],
    start: int = 2005,
    end: int = 2030,
) -> pd.DataFrame:
    """
    Fetch IMF WEO-style macro series via IMF SDMX (IFS/other IMF datasets, depending on availability).
    Returns tidy df: iso3c, year, value, indicator

    Notes:
    - IMF datasets and codes differ. This function is a generic SDMX puller.
    - You must ensure `indicators` match the dataset you query.
    """
    # IMF SDMX endpoint
    imf = Request("IMF")

    # You need a dataset id. Common IMF dataset is 'IFS', but WEO is not always exposed the same way.
    # We'll try IFS as a base and you can swap dataset_id once confirmed.
    dataset_id = "IFS"

    frames = []

    # SDMX query keys vary by dataset; for IFS: FREQ, REF_AREA, INDICATOR, etc.
    # We'll attempt annual frequency "A". If your series are quarterly, change to "Q".
    for ind in indicators:
        try:
            # key format: "A.<country>.<indicator>...." varies by dataset.
            # We'll query broadly then filter.
            resp = imf.data(
                resource_id=dataset_id,
                key={"FREQ": "A", "REF_AREA": "+".join(countries), "INDICATOR": ind},
                params={"startPeriod": str(start), "endPeriod": str(end)},
            )
            msg = resp.data
            df = msg.to_pandas()

            # df index typically includes time; handle both Series and DataFrame cases
            if isinstance(df, pd.Series):
                df = df.to_frame("value")

            df = df.reset_index()

            # Try to standardize columns
            # Common columns: 'TIME_PERIOD', 'REF_AREA', maybe 'value'
            time_col = "TIME_PERIOD" if "TIME_PERIOD" in df.columns else "time"
            area_col = "REF_AREA" if "REF_AREA" in df.columns else "REF_AREA.id"

            if time_col not in df.columns:
                # fallback: guess first column that looks like time
                for c in df.columns:
                    if "TIME" in c or "Period" in c:
                        time_col = c
                        break

            if area_col not in df.columns:
                # fallback: guess first column that looks like area
                for c in df.columns:
                    if "AREA" in c or "REF" in c:
                        area_col = c
                        break

            out = pd.DataFrame(
                {
                    "iso3c": df[area_col].astype(str),
                    "year": pd.to_numeric(df[time_col], errors="coerce"),
                    "value": pd.to_numeric(df["value"], errors="coerce"),
                    "indicator": ind,
                }
            ).dropna(subset=["year"])

            out["year"] = out["year"].astype(int)
            frames.append(out)

        except Exception:
            # If a series isn't available under the chosen dataset_id, skip it cleanly
            continue

    if not frames:
        return pd.DataFrame(columns=["iso3c", "year", "value", "indicator"])

    return pd.concat(frames, ignore_index=True)

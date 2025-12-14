from __future__ import annotations
import pandas as pd
from imfpy import IMF

def fetch_weo(
    countries: list[str],
    indicators: list[str],
    start: int = 2005,
    end: int = 2030,
    weo_release: str | None = None,  # optional, e.g. "2024-10"
) -> pd.DataFrame:
    """
    Returns tidy df: iso3c, year, value, indicator
    Notes:
    - WEO indicators are strings like NGDP_RPCH, GGXWDN_NGDP, GGB_NGDP, etc.
    """
    imf = IMF()
    # If your client version supports selecting a release, keep default unless you want a frozen vintage.
    df = imf.weo(countries=countries, indicators=indicators, start=start, end=end)  # type: ignore
    # Normalize to tidy
    out = (
        df.rename(columns={"Country": "iso3c", "Subject Descriptor": "indicator", "Units": "units"})
          .melt(id_vars=["iso3c", "indicator"], var_name="year", value_name="value")
    )
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out = out.dropna(subset=["year"])
    out["year"] = out["year"].astype(int)
    return out[["iso3c", "year", "value", "indicator"]]

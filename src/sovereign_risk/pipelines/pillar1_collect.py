from __future__ import annotations

import os
import pandas as pd
from dotenv import load_dotenv

from sovereign_risk.sources.worldbank import fetch_wdi_indicator
from sovereign_risk.sources.imf_weo import fetch_weo


# -----------------------------
# Pillar 1: data series mapping
# -----------------------------
PILLAR1_WDI: dict[str, str] = {
    # Growth
    "gdp_growth_real_wdi": "NY.GDP.MKTP.KD.ZG",      # GDP growth (annual %)
    "gdp_pc_growth_wdi": "NY.GDP.PCAP.KD.ZG",        # GDP per capita growth (annual %)

    # Structure / diversification proxies
    "agri_share_gdp": "NV.AGR.TOTL.ZS",             # Agriculture, value added (% of GDP)
    "ind_share_gdp": "NV.IND.TOTL.ZS",              # Industry, value added (% of GDP)
    "serv_share_gdp": "NV.SRV.TOTL.ZS",             # Services, value added (% of GDP)
    "resource_rents_gdp": "NY.GDP.TOTL.RT.ZS",       # Total natural resources rents (% of GDP)

    # Buffers / financial depth proxy
    "private_credit_gdp": "FS.AST.PRVT.GD.ZS",       # Domestic credit to private sector (% of GDP)
}

# IMF WEO codes (these are commonly used; availability can differ by release)
PILLAR1_WEO: dict[str, str] = {
    "gdp_growth_real_weo": "NGDP_RPCH",              # Real GDP growth (%)
    "gov_debt_gdp": "GGXWDN_NGDP",                   # General gov gross debt (% GDP)
    "fiscal_balance_gdp": "GGB_NGDP",                # General gov overall balance (% GDP)
    # If your WEO puller supports it and the series exists, keep; otherwise it will be dropped safely
    "primary_balance_gdp": "GGXONLB_NGDP",           # Primary balance (% GDP) (may be missing for some)
}


def run(countries: list[str], start: int = 2005, end: int = 2030) -> pd.DataFrame:
    """
    Collect Pillar 1 raw data from:
      - World Bank (WDI indicators)
      - IMF WEO (macro-fiscal & debt)
    Output:
      data/raw/pillar1_wdi.(csv|parquet)
      data/raw/pillar1_weo.(csv|parquet)
      data/raw/pillar1_combined.(csv|parquet)
    Returns:
      combined tidy dataframe with columns: iso3c, year, value, series_name, source
    """
    load_dotenv()

    os.makedirs("data/raw", exist_ok=True)

    # --------
    # World Bank
    # --------
    wdi_frames: list[pd.DataFrame] = []
    for series_name, ind_code in PILLAR1_WDI.items():
        df = fetch_wdi_indicator(countries, ind_code, start=start, end=end)
        if df.empty:
            continue
        df["series_name"] = series_name
        df["source"] = "WDI"
        wdi_frames.append(df[["iso3c", "year", "value", "series_name", "source"]])

    wdi_out = (
        pd.concat(wdi_frames, ignore_index=True)
        if wdi_frames
        else pd.DataFrame(columns=["iso3c", "year", "value", "series_name", "source"])
    )

    wdi_out.to_parquet("data/raw/pillar1_wdi.parquet", index=False)
    wdi_out.to_csv("data/raw/pillar1_wdi.csv", index=False)

    # ----
    # IMF WEO
    # ----
    weo_raw = fetch_weo(
        countries=countries,
        indicators=list(PILLAR1_WEO.values()),
        start=start,
        end=end,
    )

    if weo_raw.empty:
        weo_out = pd.DataFrame(columns=["iso3c", "year", "value", "series_name", "source"])
    else:
        # Map WEO codes -> our series_name
        code_to_series = {code: name for name, code in PILLAR1_WEO.items()}
        weo_raw["series_name"] = weo_raw["indicator"].map(code_to_series)
        weo_out = (
            weo_raw.dropna(subset=["series_name"])
            .drop(columns=["indicator"])
            .assign(source="WEO")
            [["iso3c", "year", "value", "series_name", "source"]]
        )

    weo_out.to_parquet("data/raw/pillar1_weo.parquet", index=False)
    weo_out.to_csv("data/raw/pillar1_weo.csv", index=False)

    # --------
    # Combine
    # --------
    combined = (
        pd.concat([wdi_out, weo_out], ignore_index=True)
        .sort_values(["iso3c", "series_name", "year"])
        .reset_index(drop=True)
    )

    combined.to_parquet("data/raw/pillar1_combined.parquet", index=False)
    combined.to_csv("data/raw/pillar1_combined.csv", index=False)

    return combined


if __name__ == "__main__":
    # Pilot list (change as needed)
    pilot_countries = ["SEN", "GHA", "KEN"]

    df = run(pilot_countries, start=2005, end=2030)

    print("✅ Saved:")
    print(" - data/raw/pillar1_wdi.(csv|parquet)")
    print(" - data/raw/pillar1_weo.(csv|parquet)")
    print(" - data/raw/pillar1_combined.(csv|parquet)")
    print(df.head(20))

from __future__ import annotations

import os
import pandas as pd
from dotenv import load_dotenv

from ..sources.worldbank import fetch_wdi_indicator
from ..sources.imf_weo import fetch_weo


PILLAR1_WDI: dict[str, str] = {
    "gdp_growth_real_wdi": "NY.GDP.MKTP.KD.ZG",
    "gdp_pc_growth_wdi": "NY.GDP.PCAP.KD.ZG",
    "agri_share_gdp": "NV.AGR.TOTL.ZS",
    "ind_share_gdp": "NV.IND.TOTL.ZS",
    "serv_share_gdp": "NV.SRV.TOTL.ZS",
    "resource_rents_gdp": "NY.GDP.TOTL.RT.ZS",
    "private_credit_gdp": "FS.AST.PRVT.GD.ZS",
}

PILLAR1_WEO: dict[str, str] = {
    "gdp_growth_real_weo": "NGDP_RPCH",
    "gov_debt_gdp": "GGXWDN_NGDP",
    "fiscal_balance_gdp": "GGB_NGDP",
    "primary_balance_gdp": "GGXONLB_NGDP",  # may be missing; handled safely
}


def run(countries: list[str], start: int = 2005, end: int = 2030) -> pd.DataFrame:
    load_dotenv()
    os.makedirs("data/raw", exist_ok=True)

    # --- WDI ---
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

    # --- WEO ---
    weo_raw = fetch_weo(
        countries=countries,
        indicators=list(PILLAR1_WEO.values()),
        start=start,
        end=end,
    )

    if weo_raw.empty:
        weo_out = pd.DataFrame(columns=["iso3c", "year", "value", "series_name", "source"])
    else:
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

    # --- Combine ---
    combined = (
        pd.concat([wdi_out, weo_out], ignore_index=True)
        .sort_values(["iso3c", "series_name", "year"])
        .reset_index(drop=True)
    )

    combined.to_parquet("data/raw/pillar1_combined.parquet", index=False)
    combined.to_csv("data/raw/pillar1_combined.csv", index=False)

    return combined


if __name__ == "__main__":
    pilot_countries = ["SEN", "GHA", "KEN"]
    df = run(pilot_countries, start=2005, end=2030)

    print("✅ Saved:")
    print(" - data/raw/pillar1_wdi.(csv|parquet)")
    print(" - data/raw/pillar1_weo.(csv|parquet)")
    print(" - data/raw/pillar1_combined.(csv|parquet)")
    print(df.head(20))

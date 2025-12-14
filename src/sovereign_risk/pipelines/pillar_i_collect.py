from __future__ import annotations
import os
from dotenv import load_dotenv
import pandas as pd

from sovereign_risk.sources.worldbank import fetch_wdi_indicator

PILLAR1_WDI = {
    # Growth
    "gdp_growth_real": "NY.GDP.MKTP.KD.ZG",
    "gdp_pc_growth": "NY.GDP.PCAP.KD.ZG",
    # Structure / diversification proxies
    "agri_share_gdp": "NV.AGR.TOTL.ZS",
    "ind_share_gdp": "NV.IND.TOTL.ZS",
    "serv_share_gdp": "NV.SRV.TOTL.ZS",
    "resource_rents_gdp": "NY.GDP.TOTL.RT.ZS",
    # Buffers / depth proxy
    "private_credit_gdp": "FS.AST.PRVT.GD.ZS",
}

def run(countries: list[str], start: int = 2005, end: int = 2030) -> pd.DataFrame:
    load_dotenv()

    frames = []
    for name, code in PILLAR1_WDI.items():
        df = fetch_wdi_indicator(countries, code, start=start, end=end)
        df["series_name"] = name
        frames.append(df)

    out = pd.concat(frames, ignore_index=True).sort_values(
        ["iso3c", "series_name", "year"]
    )

    os.makedirs("data/raw", exist_ok=True)
    out.to_parquet("data/raw/pillar1_wdi.parquet", index=False)
    out.to_csv("data/raw/pillar1_wdi.csv", index=False)
    return out

if __name__ == "__main__":
    pilot = ["SEN", "GHA", "KEN"]
    df = run(pilot, 2005, 2030)
    print(df.head(20))
    print("Saved data/raw/pillar1_wdi.(parquet|csv)")

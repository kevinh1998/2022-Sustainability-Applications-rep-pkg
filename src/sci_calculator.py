import pandas as pd
import requests

"""
Calculate SCI Score
https://github.com/Green-Software-Foundation/software_carbon_intensity
"""
class SCICalculator:

    def __init__(self, zone: str):
        self.zone = zone

    def get_carbon_intenisty(self) -> int:
        url = "https://api.co2signal.com/v1/latest"
        params = {"countryCode": self.zone}
        header = {"auth-token": "486x34o3mqGtxNmRkChe17QOxS3Cmk8O"}
        r = requests.get(url, params=params, headers=header).json()
        return r["data"]["carbonIntensity"]

    def calculate_score(self, df: pd.DataFrame) -> pd.DataFrame:
        # carbon_intenisty = self.get_carbon_intenisty()
        # time_span = (df["Time"].max() - df["Time"].min()).total_seconds() / 3600
        carbon_intenisty = 352
        df["SCI"] = (df["Energy kwh"] * carbon_intenisty) / df["Net Trans gbps"]
        df = df.round({"SCI": 1})
        df.sort_values(by=["SCI"], ascending=False, inplace=True)
        return df[["OAR Name", "SCI", "Energy kwh", "Net Trans gbps"]]

if __name__ == "__main__":
    df = pd.read_csv("week31.csv")
    sci = SCICalculator(zone="NL")
    df = sci.calculate_score(df)
    df.to_csv("week31-sci_scores.csv", index=False)
    # print(df)
    # print(type(df))

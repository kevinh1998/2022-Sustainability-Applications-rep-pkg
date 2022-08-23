from email.mime import application
from time import time
import pandas as pd
import numpy as np
from pyparsing import col

class ApplicationCoupler:

    def __init__(self, filepath: str):
        self.app_df = self.importData(filename=filepath)

    def importData(self, filename: str, sep=",", encoding="utf-8") -> pd.DataFrame:
        return pd.read_csv(filename, sep=sep, encoding=encoding)

    def prep_golden_source(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"OAR ID": "OAR"})
        df = df[["OAR", "OAR Name", "Grid", "Block"]]
        df = df.dropna(subset=["OAR"])
        return df.drop_duplicates() 

    def prep_cms_source(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(columns={"Server": "VM"}, inplace=True)
        df = df[["VM", "OAR"]]
        df = df.dropna(subset=["OAR"])
        df = df.drop_duplicates()
        return df.drop_duplicates(subset=["VM"], keep=False)

    def merge_cms_golden(self, cms_df: pd.DataFrame, golden_df: pd.DataFrame) -> pd.DataFrame:
        return cms_df.merge(golden_df, how="left", on="OAR")

    def merge_dataframes_oar(self, df: pd.DataFrame) -> pd.DataFrame:
        df["VM"] = df["VM"].str.upper()
        df["VM"] = df["VM"].str.split("_").str[-1]

        return df.merge(self.app_df, how="inner", on="VM")     

if __name__ == "__main__":
    t1 = time()

    ac = ApplicationCoupler(filepath="data/applications_info_short.csv")
    
    vm_df = ac.importData(filename="data/VM_data.csv")
    vm_df.drop(columns=["ActiveMem Usage KB"], inplace=True)
    vm_df.drop(vm_df[vm_df["VM"] == "DineshTEst.csv"].index, inplace=True)

    vm_df = ac.merge_dataframes_oar(df=vm_df)
    vm_df.to_csv("out/vm_data.csv",index=False)
    
    t2 = time() 
    print(f"Total elapsed time {t2-t1} seconds")
    
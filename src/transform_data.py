import os
from time import time
from datetime import datetime

import pandas as pd

from utils import folder_path

def group_data(path: str, folder: str):
    count = len(os.listdir(path))
    print(f'0/{count}', end='')
    for idx, file in enumerate(os.listdir(path)):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        file_path = f"{path}/{file}"
        df = pd.read_csv(file_path, parse_dates=["Time"])
        if folder == "average_data":
            df = df.groupby(["OAR"], as_index=False).agg({"CPU Usage %": "mean", "CPU Usage mhz": "sum", "Mem Usage %": "mean", "Disk Usage kbps": "sum", "net Usage kbps": "sum"})
        else:
            df = df.groupby("OAR")[["Power Enrg Juole", "Net Trans kbps"]].sum().reset_index()
        df.to_csv(f"{folder}_aggregated/{file}", index=False)
    print("")

def group_data_week(path: str, folder: str):
    df = pd.DataFrame()
    count = len(os.listdir(path))
    print(f'0/{count}', end='')
    week = None
    for idx, file in enumerate(os.listdir(path)):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        file_path = f"{path}/{file}"
        temp_df = pd.read_csv(file_path, parse_dates=["Time"])
        date_string = file.split("-", 1)[0]
        try:
            new_week = datetime.strptime(date_string, '%Y%m%d').date().isocalendar().week
        except ValueError:
            new_week = datetime.strptime(date_string, '%d%m%Y').date().isocalendar().week
        if (week != new_week and idx != 0):
            if folder == "average_data":
                df = df.groupby(["OAR"], as_index=False).agg({"CPU Usage %": "mean", "CPU Usage mhz": "sum", "Mem Usage %": "mean", "Disk Usage kbps": "sum", "net Usage kbps": "sum"})
            else:
                df = df.groupby("OAR")[["Power Enrg Juole", "Net Trans kbps"]].sum().reset_index()
            df.to_csv(f"week_data/week{week}-{folder}.csv", index=False)
            df = temp_df
        else:
            df = pd.concat([df, temp_df], ignore_index=True)
        week = new_week
    df.to_csv(f"week_data/week{week}-{folder}.csv", index=False)
    print("")
    print("All done!")

def concat_data_week(path: str):
    df = pd.DataFrame()
    count = len(os.listdir(path))
    print(f'0/{count}', end='')
    week = None
    for idx, file in enumerate(os.listdir(path)):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        file_path = f"{path}/{file}"
        temp_df = pd.read_csv(file_path)
        date_string = file.split("-", 1)[0]
        date = datetime.strptime(date_string, '%Y%m%d').date()
        temp_df["date"] = date
        new_week = date.isocalendar().week
        if (week != new_week and idx != 0):
            df.sort_values(by=["OAR", "date"], inplace=True)
            df.to_csv(f"day_data/week{week}-data.csv", index=False)
            df = temp_df
        else:
            df = pd.concat([df, temp_df], ignore_index=True)
        week = new_week
    df.to_csv(f"day_data/week{week}-data.csv", index=False)
    print("")
    print("All done!") 

def group_data_manual(path: str):
    files = ["week32-average_data.csv", "week32-realtime_data_v2.csv", "week30-realtime_data.csv"]
    count = len(files)
    print(f'0/{count}', end='')
    for idx, file in enumerate(files):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        file_path = f"{path}/{file}"
        df = pd.read_csv(file_path, parse_dates=["Time"])
        if "average_data" in file:
            df = df.groupby(["OAR"], as_index=False).agg({"CPU Usage %": "mean", "CPU Usage mhz": "sum", "Mem Usage %": "mean", "Disk Usage kbps": "sum", "net Usage kbps": "sum"})
        else:
            df = df.groupby("OAR")[["Power Enrg Juole", "Net Trans kbps"]].sum().reset_index()
        df.to_csv(f"week_data_v2/{file}", index=False)
    print("")
    print("All done!")

def merge_data(path: str):
    files = ["average_data", "realtime_data_v2"]
    df = pd.DataFrame()
    for file in os.listdir(path):
        if any(x in file for x in files):
            file_path = f"{path}/{file}"
            temp_df = pd.read_csv(file_path)
            date_string = file.split("-", 1)[0]
            if df.empty:
                df = temp_df
            else:
                df = temp_df.merge(df, how="inner", on="OAR")
                df.to_csv(f"week_data/{date_string}-data.csv", index=False)
                df = pd.DataFrame()

def merge_day_data(path: str):
    df = pd.DataFrame()
    for file in os.listdir(path):
        file_path = f"{path}/{file}"
        temp_df = pd.read_csv(file_path)
        date_string = file.split("-", 1)[0]
        if df.empty:
            df = temp_df
        else:
            df = temp_df.merge(df, how="inner", on="OAR")
            df.to_csv(f"day_data/{date_string}-data.csv", index=False)
            df = pd.DataFrame()

def add_application_data(path: str):
    df_info = pd.read_csv("data/application.csv")
    for file in os.listdir(path):
        file_path = f"{path}/{file}"
        df = pd.read_csv(file_path)
        df = df.merge(df_info, how="left", on="OAR")
        df.to_csv(f"day_data/{file}", index=False)


def round_data(path: str):
    count = len(os.listdir(path))
    print(f'0/{count}', end='')
    for idx, file in enumerate(os.listdir(path)):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        file_path = f"{path}/{file}"
        df = pd.read_csv(file_path)
        df = df.round(2)
        df.to_csv(f"day_data/{file}", index=False)

# def add_application_data(path: str):
#     files = ["week30-data.csv", "week31-data.csv"]
#     df_info = pd.read_csv("data/application.csv")
#     for file in files:
#         file_path = f"{path}/{file}"
#         df = pd.read_csv(file_path)
#         df = df.merge(df_info, how="left", on="OAR")
#         df.to_csv(f"week_data/{file}", index=False)


def main():
    t1 = time()

    cwd = os.getcwd()
    folders = ["average_data", "realtime_data", "realtime_data_v2"]

    for folder in folders:
        input_path = folder_path(cwd, folder)
        group_data_week(path=input_path, folder=folder)

    print("All done!")

    t2 = time()
    print(f"Total elapsed time {t2-t1} seconds")

if __name__ == "__main__":
    cwd = os.getcwd()
    input_path = folder_path(cwd, "day_data")
    concat_data_week(path=input_path)
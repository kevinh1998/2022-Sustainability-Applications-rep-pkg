from time import time
from datetime import datetime
import pandas as pd
import os
import glob

from file_extractor_mail import FileExtractorMail
from application_coupler import ApplicationCoupler
from sci_calculator import SCICalculator
from utils import folder_path

def read_clean_df(file_name: str, path: str) -> pd.DataFrame:
    file_path = f"{path}/{file_name}"
    df = pd.read_csv(file_path, parse_dates=["Time"])
    # df = pd.read_csv(file_path, parse_dates=["Time"], low_memory=False)
    df.drop(columns=["ActiveMem Usage KB"], inplace=True)
    return df.drop(df[df["VM"] == "DineshTEst.csv"].index)

def drop_zero_values(df: pd.DataFrame) -> pd.DataFrame:
    df_group = df.groupby(['VM'], as_index=False).agg({'Power Enrg Juole': 'sum', 'OAR': 'first'})
    df_group = df_group[df_group["Power Enrg Juole"] == 0]
    oar_list = df_group["OAR"].unique().tolist()
    return df[~df['OAR'].isin(oar_list)]

def main():
    t1 = time()

    cwd = os.getcwd()
    input_path = folder_path(cwd, "extracted_files")

    file_extractor = FileExtractorMail(time_delta = 20)
    # file_extractor = FileExtractorMail(subject = "Average", time_delta = 20)
    file_extractor.open_attachments()
    file_extractor.extract_zip()
    file_extractor.change_filenames()

    print("Start application coupling...")
    application_coupler = ApplicationCoupler(filepath="data/applications_info_short.csv")
    df = pd.DataFrame()
    count = len(os.listdir(input_path))
    print(f'0/{count}', end='')
    date = None
    for idx, file in enumerate(os.listdir(input_path)):
        print(f'\r{idx+1}/{count}', end='', flush=True)
        temp_df = read_clean_df(file_name=file, path=input_path)
        temp_df = application_coupler.merge_dataframes_oar(df=temp_df)
        # temp_df = drop_zero_values(temp_df)

        temp_df.drop('VM', axis=1, inplace=True)
        temp_df.drop('net rcvd kbps', axis=1, inplace=True)
        col = temp_df.pop("OAR")
        temp_df.insert(0, col.name, col)
        temp_df = temp_df.astype({"Power Enrg Juole": "int16", "Net Trans kbps": "int32"})

        date_string = file.split("-", 1)[0]
        new_date = datetime.strptime(date_string, '%Y%d%m').date()
        if (date != new_date and idx != 0):
            df.to_csv(f"realtime_data_v2/{date.strftime('%Y%m%d')}-Application_Realtime.csv", index=False)
            # df.to_csv(f"average_data/{date.strftime('%Y%m%d')}-Application_Average.csv", index=False)
            df = temp_df
        else:
            df = pd.concat([df, temp_df], ignore_index=True)
        date = new_date

    df.to_csv(f"realtime_data_v2/{date.strftime('%Y%m%d')}-Application_Realtime.csv", index=False)
    # df.to_csv(f"average_data/{date.strftime('%Y%m%d')}-Application_Average.csv", index=False)
    print("")
    print("All done!")

    t2 = time()
    print(f"Total elapsed time {t2-t1} seconds")


if __name__ == main():
    main()

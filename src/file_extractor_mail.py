import logging
import os
import shutil
from configparser import ConfigParser
from datetime import datetime, timedelta
from time import time
from zipfile import ZipFile

import pandas as pd
import win32com.client
from win32com.client import CDispatch

from utils import folder_path

class FileExtractorMail:

    def __init__(self, subject: str = "Realtime", time_delta: int = 7, email_list: list[str] = ['ABN-BHT-VMWARE@Kyndryl.com', 'vijay.yadav@ocean.ibm.com', 'Vijay.Yadav11@kyndryl.com']):
        self.cwd = os.getcwd()
        self.subject = subject
        self.time_delta = time_delta
        self.email_list = email_list

    def open_messages(self) -> CDispatch:
        outlook = win32com.client.Dispatch('outlook.application')
        mapi = outlook.GetNamespace("MAPI")
        inbox = mapi.GetDefaultFolder(6)
        messages = inbox.Items
        print("Succesfully retreived all e-mails")
        return messages

    def select_messages(self, messages: CDispatch, e_mail: str) -> list[CDispatch]:
        received_dt = datetime.now() - timedelta(self.time_delta)
        received_dt = received_dt.strftime('%m/%d/%Y %H:%M %p')
        messages = messages.Restrict("[ReceivedTime] >= '" + received_dt + "'")
        messages = messages.Restrict(f"[SenderEmailAddress] = '{e_mail}'")
        messages = list(messages)
        
        message_list = []
        for message in messages:
            if self.subject in message.Subject:
                message_list.append(message)
        print(f"Selected {len(message_list)} e-mails from sender: {e_mail}, with date: {received_dt}")
        return message_list

    def extract_attachments(self, messages, folder: str = "attachments"):
        outputDir = folder_path(self.cwd, folder)
        file_list = os.listdir(outputDir)
        extracted = False
        print("Start extracting attachments...")
        try:
            print(f'0/{len(messages)}', end='')
            for idx, message in enumerate(messages):
                print(f'\r{idx+1}/{len(messages)}', end='', flush=True)
                try:
                    for attachment in message.Attachments:
                        _, extension = os.path.splitext(attachment.FileName)
                        file_name = attachment.FileName
                        if extension == ".zip" and file_name not in file_list:
                            path = os.path.join(outputDir, file_name)
                            attachment.SaveAsFile(path)
                            logging.info(f" attachment {file_name} saved")
                            extracted = True
                except Exception as e:
                    logging.warning("erorr when saving attachment:" + str(e))
            print("")
            print("Extracted attachments from all messages")
        except Exception as e:
            logging.warning("Error when processing emails:" + str(e))
        if not extracted:
            message = "No new mails were found"
            logging.warning(message)

    def extract_zip(self, input_folder: str = "attachments", output_folder: str = "extracted_files"):
        input_path = folder_path(self.cwd, input_folder)
        output_path = folder_path(self.cwd, output_folder)
        os.chdir(input_path)
        output_files = os.listdir(output_path)
        unzipped = False

        print("Start unzipping attachments...")
        count = len(os.listdir(input_path))
        print(f'0/{count}', end='')
        for idx, file in enumerate(os.listdir(input_path)):
            print(f'\r{idx+1}/{count}', end='', flush=True)
            file_name = file.replace(".zip", "")
            if file.endswith(".zip") and file_name not in output_files:
                unzipped = True
                with ZipFile(file, "r") as z:
                    z.extractall(path = output_path)
                    logging.info(f"extracted {file}")
        os.chdir(self.cwd)
        print("")
        print("Unzipped all attachments")
        # shutil.rmtree(input_path)
        if not unzipped:
            logging.warning("No files unzipped")

    def change_filenames(self, folder: str = "extracted_files"):
        path = folder_path(self.cwd, folder)
        os.chdir(folder)
        count = len(os.listdir(path))
        print("Change file names")
        print(f'0/{count}', end='')
        for idx, file in enumerate(os.listdir(path)):
            print(f'\r{idx+1}/{count}', end='', flush=True)
            new_file = file.split(".", 1)[0]
            name_list = new_file.split("-", 1)
            name_list.insert(0, name_list.pop(1))
            new_file = "-".join(name_list)
            new_file += ".csv"
            os.rename(file, new_file)
        print("")
        print("All file names changed")
        os.chdir(self.cwd)


    def open_attachments(self):
        messages = self.open_messages()
        message_list: list[CDispatch] = []
        for e_mail in self.email_list:
            message_list.extend(self.select_messages(messages, e_mail=e_mail))
        self.extract_attachments(message_list)

if __name__ == "__main__":
    file_extractor = FileExtractorMail()
    file_extractor.open_attachments()
    file_extractor.extract_zip()
    file_extractor.change_filenames()

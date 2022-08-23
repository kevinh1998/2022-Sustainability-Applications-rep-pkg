import os
import logging

def folder_path(cwd, folder):
        path = os.path.join(cwd, folder)
        isExist = os.path.exists(path)
        if not isExist:
            os.mkdir(path)
            logging.info(f"{folder} did not exist in {path}")
        return path
import glob
import time
# import chardet
import os
import re
import json
# from common.config_parser import *

flagID = re.I|re.DOTALL
flagI = re.I
flagD = re.DOTALL


class Common:

    def create_directory(self,directory):
        print("inside the main class")
        if not os.path.exists(directory):
            os.mkdir(directory)
            print(f"Directory {directory} created.")
        else:
            print(f"Directory {directory} already exists.")

    def fetch_json(self,single_json):
        print("Json File is fetching")
        print(single_json)
        with open(single_json) as file_content:
            file_contents = file_content.read()

        parsed_json = json.loads(file_contents)
        return parsed_json


common_obj = Common()

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
        # print("Json File is fetching")
        # print(single_json)
        with open(single_json) as file_content:
            file_contents = file_content.read()

        parsed_json = json.loads(file_contents)
        return parsed_json

    def get_normalized_path(self,file_path, os_type):
        """
        Normalize file path based on the operating system type.

        Args:
            file_path (str): Original file path.
            os_type (str): Target OS type ("windows" or "mac").

        Returns:
            str: OS-specific normalized path.
        """
        if os_type.lower() == "mac":
            # Convert Windows-style paths to macOS/Unix-style
            print("need to convert to mac os")
            print(file_path)
            mac_path = file_path.replace("\\", "/")
            # print(os.path.normpath(mac_path))
            print(mac_path)
            return mac_path  # Ensures compatibility
        elif os_type.lower() == "windows":
            # Ensure Windows-compatible path
            return os.path.normpath(file_path)
        else:
            raise ValueError("Unsupported OS type. Use 'windows' or 'mac'.")



common_obj = Common()

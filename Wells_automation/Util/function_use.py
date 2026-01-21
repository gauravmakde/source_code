import os
import sys
import re

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Append 'common' directory to sys.path if not already there
common_path = os.path.join(project_root, "common")
# if common_path not in sys.path:
sys.path.append(common_path)

# Import directly from common_fun.py
from config_parser import *


from common.config_parser import *

def fetch_details_sql_file(file):

    comment = 0

    with open(temp_table_file_location,"r") as tempfileopen:
        temp_table_file=tempfileopen.readline()

    table_temp_location={}
    for single_value in temp_table_file.strip().split("\n"):  # strip to avoid trailing empty lines
        if single_value.strip():
            parts = single_value.split(",")
            if len(parts) == 2:
                paramas_key = parts[0].strip()
                paramas_value = parts[1].strip()
                table_temp_location[paramas_key]=paramas_value

    with open(statics_file_location,"r") as statifileopen:
        static_file=statifileopen.readline()

    full_static_value = {}
    for single_value in static_file.strip().split("\n"):  # strip to avoid trailing empty lines
        if single_value.strip():
            parts = single_value.split(",")
            if len(parts) == 2:
                paramas_key = parts[0].strip()
                paramas_value = parts[1].strip()
                full_static_value[paramas_key] = paramas_value

    with open(file) as fileReopen:
        fileLines = fileReopen.readlines()
        # print(fileLines)
        output_file = ""
        clean_metadata_lines = []
        for fileLine in fileLines:
            cmt = re.findall(r'^[\s]*--', fileLine)
            if "/*" in fileLine:

                if '*/' in fileLine:
                    comment = 0
                    pass
                else:
                    comment = 1
                    pass
            elif comment == 1:
                if "*/" in fileLine:
                    comment = 0
                    pass
                else:
                    pass
            elif cmt:
                pass
            else:
                for key, value in full_static_value.items():
                    input_metadata=re.findall(regex_input_file_metadata,fileLine)
                    for match in input_metadata:
                        if match[0]:  # It's a templated match like: INSERT INTO {{ params... }}
                            clean_line = match[0].strip()
                            clean_metadata_lines.append(clean_line)
                        elif match[4]:  # It's a regular match like: FROM abc.table_name
                            clean_line = match[4].strip()
                            clean_metadata_lines.append(clean_line)
                    if key in fileLine:
                        fileLine = fileLine.replace(key, value)
                    match_metadata=re.search(metadata_regex, fileLine)
                    if match_metadata:
                        metadata_key = match_metadata.group(2)

                        # Replace only if the key exists in the dict
                        if metadata_key in table_temp_location:
                            fileLine = fileLine.replace(metadata_key, table_temp_location[metadata_key])

                output_file = output_file + fileLine

    output_file_location=file.replace("Input","Output")
    os.makedirs(os.path.dirname(output_file_location), exist_ok=True)

    with open(output_file_location,"w") as outputfileopen:
        outputfileopen.write(output_file)
    print(f"The output sql file is created {output_file_location}")
    metadata_file = output_file_location.replace(".sql", "_metadata_cleaned_output.txt")

    with open(metadata_file, "w") as meta_f:
        meta_f.write(f"--- Clean Metadata for file: {file} ---\n")
        for line in clean_metadata_lines:
            meta_f.write(line + ",\n")
    print(f"The output metadata file is created {metadata_file}")
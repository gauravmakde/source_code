import os
import time
import re
import glob
import pandas as pd

# Create date strings for directory and filenames
datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.mkdir(directory)

output_directory = os.path.join(directory, "Output")

# Get list of all .sql files in the specified directory and subdirectories
ParamFolderLst = glob.glob(
    r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-06-26//temp_sp2//**//NA//translated_DL_CMA_CMBR.customer_activation.sql',
    recursive=True)
print(ParamFolderLst)


def file_name_to_sql(location, name_to_insert, sql_file):
    print(location)
    print(name_to_insert)
    with open(sql_file, "r") as f:
        py = f.read()
        sub = re.search(name_to_insert.lower(), py.lower())
        if sub:
            need_to_convert = re.sub(name_to_insert.lower(), name_to_insert, py)
        else:
            need_to_convert = py

    return need_to_convert


if __name__ == "__main__":
    data = []
    for sql_file in ParamFolderLst:
        location = os.path.dirname(sql_file)
        file_name = os.path.basename(sql_file)
        print("Location", location)
        file_pattern = re.compile(r"translated_(\w*\.\w*)\.sql")
        fetch_name_to_insert = file_pattern.search(file_name)
        if fetch_name_to_insert:
            name_to_insert = fetch_name_to_insert.group(1)
            final_response = file_name_to_sql(location, name_to_insert, sql_file)
            print(final_response)

            output_file_path = os.path.join(output_directory, file_name)
            output_folder = os.path.dirname(output_file_path)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)  # Create the directory if it doesn't exist

            with open(output_file_path, 'w') as f_out:
                f_out.write(final_response)

        else:
            print("Translate is not present")
            data.append({
                'location': location,
                'file_name': file_name,
                'comment': "Issue with the file"
            })
            print(data)

    print(data)  # Debugging: print the collected data

    df = pd.DataFrame(data)
    output_csv_path = os.path.join(directory, 'Output', f'file_issue_{timestr}.csv')
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False)

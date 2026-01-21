import os
import glob
import time
import pandas as pd

# Get the current date and time
datestr = time.strftime("%Y-%m-%d") #2024-07-16
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

output_directory = os.path.join(directory, "Output")

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.mkdir(directory)

CSV_Input_Folder_Path = directory
Enable_csv_fetch_file = "Y"
Enable_copy_of_sql = "Y"
csv_location="D://Nordstrom//Automation//reports//SQL_Input_Files.csv"
output_base_location=ion="D://Nordstrom//Automation//reports//"
dag_folder='dag_py'
input_base_location="D://Nordstrom//SPRINT03//ISF_Exports//ISFExport_with_configs//"

if __name__ == "__main__":
    data = []
    error_file = []



    if Enable_csv_fetch_file == "Y":

        print("Enable_csv_file_fetch")
        file_df = pd.read_csv(csv_location)

        ParamFolderLst = []
        # print(file_df['SQL_Name'])
        for single_file in file_df.iterrows():
            # Dag_location = single_file[1]['Dag_location']
            Dag_name = single_file[1]['Dag_name']
            SQL_Location = single_file[1]['SQL_Location']
            SQL_Name = single_file[1]['SQL_Name']
            Milestone = single_file[1]['Milestone']
            final_location=single_file[1]['Location_Dag_Name']
            folder_location=final_location.replace("|","//").split("/")[0]
            sourceProduct = single_file[1]['SourceProduct']

            if Milestone == "Sprint 3" and sourceProduct == "teradata":
                print("File is in Sprint 3")
                last_folder = SQL_Location.split("/")[-2]
                first_folder = SQL_Location.split("/")[0]
                singleFileLst = glob.glob(
                   input_base_location + first_folder+ '//**//'+ last_folder+"//"+ SQL_Name,recursive=True
                )

                print(singleFileLst)
                for sql_file in singleFileLst:
                    location = os.path.dirname(sql_file)
                    file_name = os.path.basename(sql_file)
                    print("\nLocation", location)
                    print("File name:", file_name)
                    print("The length of file is:", len(sql_file))

                    # relative_location = os.path.relpath(location, directory)
                    output_file_path = os.path.join(output_base_location,folder_location,dag_folder,Dag_name , file_name)
                    output_folder = os.path.dirname(output_file_path)
                    if not os.path.exists(output_folder):
                        os.makedirs(output_folder)  # Create the directory if it doesn't exist

                    print(f"length of file ",{len(output_file_path)})
                    try:
                        with open(sql_file, 'r') as src_file:
                            with open(output_file_path, 'w') as dest_file:
                                dest_file.write(src_file.read())
                        print(f"File copy {file_name}")


                    except PermissionError as e:

                        print(f"PermissionError copying SQL file {file_name}: {e}")
                        error_file.append({
                            'location': location,
                            'file_name': file_name,
                            'error': str(e)
                        })

                    except Exception as e:

                        print(f"Error copying SQL file {file_name}: {e}")
                        error_file.append({
                            'location': location,
                            'file_name': file_name,
                            'error': str(e)
                        })
    if error_file:
        df_1 = pd.DataFrame(error_file)
        print(df_1)
        df_1.to_csv(os.path.join(directory, f"error_file_dag_{timestr}.csv"))

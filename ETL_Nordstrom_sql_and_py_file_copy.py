import os
import glob
import time
import re
import pandas as pd

# Get the current date and time
datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

output_directory = os.path.join(directory, "Output")

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.mkdir(directory)

CSV_Input_Folder_Path = directory
Enable_csv_fetch_file = "Y" # fetch the csv which is master csv for dags
Enable_copy_of_sql = "Y" # To copy the sql to another location
Enable_copy_of_dag="Y" # To copy the dag py file to another location

if __name__ == "__main__":
    data = []
    error_file = []
    error_dag_file=[]


    if Enable_csv_fetch_file == "Y":

        print("Enable_csv_file_fetch")
        file_df = pd.read_csv(os.path.join(directory, "SQL_Input_Files.csv"))

        ParamFolderLst = []
        print(file_df['SQL_Name'])
        print(file_df['Dag_name'])
        # print(file_df['SourceProduct'])
        for single_file in file_df.iterrows():
            Dag_location = single_file[1]['Dag_location']
            Dag_name = single_file[1]['Dag_name']
            SQL_Location = single_file[1]['SQL_Location']
            SQL_Name = single_file[1]['SQL_Name']
            Milestone = single_file[1]['Milestone']
            final_location=single_file[1]['Location_Dag_Name']
            folder_location=final_location.replace("|","//").split("/")[0]
            sourceProduct=single_file[1]['SourceProduct']
            last_folder = SQL_Location.split("/")[-2]
            first_folder = SQL_Location.split("/")[0]

            if Milestone == "Sprint 3" and sourceProduct=="teradata" and Enable_copy_of_sql=="Y":
                print("File is in Sprint 3")

                singleFileLst = glob.glob(
                    os.path.join(directory, 'ISF', first_folder, '**', last_folder, SQL_Name),
                    recursive=True
                )

                # if (len(singleFileLst))
                for sql_file in singleFileLst:
                    location = os.path.dirname(sql_file)
                    file_name = os.path.basename(sql_file)
                    print("\nLocation", location)
                    print("File name:", file_name)
                    print("The length of file is:", len(sql_file))

                    relative_location = os.path.relpath(location, directory)

                    output_file_path = os.path.join(output_directory,folder_location,'pypline_py',Dag_name , file_name)
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

            if Milestone == "Sprint 3"  and Enable_copy_of_dag == "Y":

                print(Dag_name)
                print(first_folder)
                file_Status = "Checking"

                if file_Status=="Checking":

                    singleFileLst = glob.glob(
                        os.path.join(directory, 'ISF', first_folder, '**','dags_py', "*.py"),
                        recursive=True
                    )

                    for python_file in singleFileLst:
                        location = os.path.dirname(python_file)
                        file_name = os.path.basename(python_file)
                        print("\nLocation", location)
                        print("File name:", file_name)
                        print("The length of file is:", len(python_file))


                        try:
                            with open(python_file, 'r', encoding='utf-8') as src_file:
                                source_file=(src_file.read())
                                dag_pattern = re.compile("(dag_id\s*=\s*f?'*([^\n]+))'*\s*,*")

                                search_dag_id = dag_pattern.search(source_file)
                                if search_dag_id:
                                    print("Dag_group_1", search_dag_id.group(1))
                                    dag_id = search_dag_id.group(1)
                                else:
                                    dag_pattern = re.compile("dag\s*=\s*DAG\(([^,]*)")
                                    search_dag_id = dag_pattern.search(source_file)
                                    if search_dag_id:
                                        dag_id = search_dag_id.group(1)
                                    else:
                                        dag_id = None

                            # print(f"File copy {dag_id}")
                            dag_value_check=Dag_name+"_"+first_folder

                            dag_id_status=re.search("\'\s*"+dag_value_check,dag_id)

                            if dag_id_status:

                                print(f"File we got for dag {Dag_name} and value of dag_id {dag_id} ")
                                file_Status ="Success"

                                dag_file_path = os.path.join(output_directory, folder_location, 'dags_py', Dag_name,
                                                                file_name)
                                output_dag_folder = os.path.dirname(dag_file_path)

                                if not os.path.exists(output_dag_folder):
                                    os.makedirs(output_dag_folder)  # Create the directory if it doesn't exist

                                # print(f"length of file ", {len(output_dag_folder)})
                                try:
                                    with open(python_file, 'r', encoding='utf-8') as src_file:
                                        with open(dag_file_path, 'w') as dest_file:
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




                        except PermissionError as e:

                            print(f"Permission Error python file  {file_name}: {e}")
                            error_dag_file.append({
                                'location': location,
                                'file_name': file_name,
                                'error': str(e)
                            })

                        except Exception as e:

                            print(f"Error copying python file {file_name}: {e}")
                            error_dag_file.append({
                                'location': location,
                                'file_name': file_name,
                                'error': str(e)
                            })

                        # if file_Status=="Success":
                        #     break


    print(error_dag_file)
    if error_file:
        df_1 = pd.DataFrame(error_file)
        print(df_1)
        df_1.to_csv(os.path.join(directory, f"error_file_dag_{timestr}.csv"))

    if error_dag_file:
        df_2 = pd.DataFrame(error_file)
        print(df_2)
        df_2.to_csv(os.path.join(directory, f"error_dag_file_{timestr}.csv"))



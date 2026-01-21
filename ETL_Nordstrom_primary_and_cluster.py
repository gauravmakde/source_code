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
r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-07-01//Translated_sp2_table//**//*.sql',
    recursive=True)

def fetch_key(sql_file,file_name,location):

    with open(sql_file, "r") as f:
        py = f.read()
        print(py)
        pattern =re.compile("(PRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED)")
        search_primary_key=pattern.search(py)

        pattern_cluster = re.compile("(CLUSTER BY\s*([^)]+));")
        search_cluster_key = pattern_cluster.search(py)
        status_primary_key=""
        status_cluster_key=""
        primary_key_values=""
        cluster_by_remove=""
        cluster_key=""
        primary_sub=py

        if search_primary_key:
            primary_key_values = search_primary_key.group(2)
            primary_key=search_primary_key.group(1)
            status_primary_key="Primary Key is present"

            primary_by_remove = primary_key
            for primary_key_value in primary_key_values.split(","):
                pattern_cluster_check=re.compile("[^\w*]"+primary_key_value.strip()+"\s*STRING")
                primary_key_check = pattern_cluster_check.search(py)

                if primary_key_check:

                    primary_by_remove=primary_by_remove.replace(primary_key_value.strip()+",","").replace(primary_key_value.strip(),"")


            remove_space_bracket=re.sub("PRIMARY KEY \((\s*)\) NOT ENFORCED","PRIMARY KEY () NOT ENFORCED",primary_by_remove)

            if remove_space_bracket.strip() == "PRIMARY KEY () NOT ENFORCED":
                primary_sub = re.sub("(,\nPRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED)", "", py)
            else:
                primary_sub = re.sub("PRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED", remove_space_bracket.strip(), py)


        if search_cluster_key:
            cluster_keys = search_cluster_key.group(2) if search_cluster_key.group(2) else search_cluster_key.group(3)
            status_cluster_key = "Cluster Key is present"

            cluster_by_remove=search_cluster_key.group(1)
            for cluster_key in cluster_keys.split(","):
                pattern_cluster_check=re.compile("[^\w*]"+cluster_key.strip()+"\s*STRING")
                cluster_check = pattern_cluster_check.search(py)
                if cluster_check:
                    cluster_by_remove=cluster_by_remove.replace(cluster_key.strip()+",","").replace(cluster_key.strip(),"")


        if cluster_by_remove.strip()=="CLUSTER BY":

           sub=re.sub("(CLUSTER BY\s*([^)]+))(;)","\\3",primary_sub)
        else:
            sub=re.sub("(CLUSTER BY\s*([^)]+))(;)",cluster_by_remove.strip()+"\\3",primary_sub)
        data.append({
            'File_name':file_name,
            'location':location,
            'primary_key_status':status_primary_key,
            'primary_value':primary_key_values,
            'cluster_key_status': status_cluster_key,
            'cluster_value': cluster_key
        })

    return data,sub



if __name__ == "__main__":
    data = []
    for sql_file in ParamFolderLst:
        location = os.path.dirname(sql_file)
        file_name = os.path.basename(sql_file)
        print("\nLocation", location)
        print("File name\n", file_name)
        key_value,sub=fetch_key(sql_file,file_name,location)
        print("\n*****After Replacement*****\n")
        print(sub)
        df = pd.DataFrame(key_value)
        df.to_csv(directory + "\\Key_status_" + timestr + ".csv")
        output_file_path = os.path.join(output_directory, file_name)
        output_folder = os.path.dirname(output_file_path)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)  # Create the directory if it doesn't exist

        with open(output_file_path, 'w') as f_out:
            f_out.write(sub)
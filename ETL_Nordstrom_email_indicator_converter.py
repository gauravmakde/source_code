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
# print(len("reports 2024-07-03\\Output\\reports 2024-07-03//gaurav_makde\\JWN-Nordstrom\\ISF_DAGs\\17168\\config_json\\argument_curation_ongoing_17168_styling_styling_insights_kpi_read_kafka_to_teradata_curation_table_refresh.json"))
# exit()
if not os.path.exists(directory):
    os.mkdir(directory)

output_directory = os.path.join(directory, "Output")

if not os.path.exists(output_directory):
    os.mkdir(output_directory)

PyFolderLst = glob.glob(directory+'//gaurav_makde//**//*.*', recursive=True)



def email_indicator_convert(location,file_name,file):
    print(location)
    print(file)
    with open(file, "r") as f:
        py = f.read()

        existing_email_failure_indicator="'email_on_failure': True,"
        updated_email_failure_indicator="'email_on_failure': False,"
        email_on_failure = re.sub(existing_email_failure_indicator, updated_email_failure_indicator, py)
        print(email_on_failure)

        return email_on_failure

def file_fetch_wih_email(location,file_name,file):
    # print(location)
    # print(file)'
    data=[]
    with open(file, "r") as f:
        py = f.read()
        # print(py)
        existing_email_failure_indicator="'email_on_failure': True"
        # updated_email_failure_indicator="'email_on_failure': False,"
        email_on_failure = re.search( existing_email_failure_indicator,py)
        email_indicator="False"
        pattern_dag_id=re.compile("dag_id\s*=\s*('[^,\n]*)")

        search_dag_id=pattern_dag_id.search(py)
        dag_id=""
        if email_on_failure:
            print(file_name)
            print(location)
            email_indicator="True"
        if search_dag_id:
            print(search_dag_id.group(1))
            dag_id=search_dag_id.group(1)

        data.append({
            'file_name':file_name,
            'location':location,
            'dag_id':dag_id,
            'email_indicator':email_indicator
        })

    return  data  # return




if __name__ == "__main__":
    data = []
    for file in PyFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        print("Location", location)

        if '.py' in file_name:
            email_output_file = file_fetch_wih_email(location, file_name, file)
            # print(email_output_file)
            data.extend(email_output_file)
            output_file=email_indicator_convert(location,file_name,file)

            output_file_path = os.path.join(output_directory, location, file_name)
            output_folder = os.path.dirname(output_file_path)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)  # Create the directory if it doesn't exist
            with open(output_file_path, 'w') as f_out:
                f_out.write(output_file)

        else:
            print("Not python")
            output_file_path = os.path.join(output_directory, location, file_name)
            output_folder = os.path.dirname(output_file_path)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)  # Create the directory if it doesn't exist
            with open(output_file_path, 'w') as f_out:
                f_out.write(file)

    df = pd.DataFrame(data)
    print(df)
    df.to_csv(directory + '\\output\dag_With_email_indicator_true_' + timestr + '.csv', index=False)
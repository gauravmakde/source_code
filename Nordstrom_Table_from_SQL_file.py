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

# Get list of all .sql files in the specified directory and subdirectories
ParamFolderLst = glob.glob(r'C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-10-11//**//*.sql', recursive=True)

# ParamFolderLst = glob.glob('C://Users//gmakde//PycharmProjects//pythonProject//'+directory+'//DAG'+'/**/*.sql', recursive=True)

print(f"Number of SQL files found: {len(ParamFolderLst)}")

df_file = []

# Initialize counters for issues
file_not_found_count = 0
path_length_exceeds_count = 0

def remove_comments(sql):
    # Remove multiline comments: /* ... */
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Remove single-line comments: -- ...
    sql = re.sub(r'--.*', '', sql)
    
    return sql

# Iterate over each SQL file found
for sql_file in ParamFolderLst:
    location = os.path.dirname(sql_file)
    file_name = os.path.basename(sql_file)

    # Normalize the path to avoid issues
    sql_file = os.path.normpath(sql_file)

    print(f"Processing file: {sql_file}")

    # Check if path length exceeds 260 characters
    if len(sql_file) > 260:
        print(f"Path length exceeds 260 characters: {sql_file}")
        path_length_exceeds_count += 1
        df_file.append([location, file_name, 'Path length exceeds limit', 'N/A'])
        continue

    # Check if file exists
    if not os.path.exists(sql_file):
        print(f"File not found: {sql_file}")
        file_not_found_count += 1
        df_file.append([location, file_name, 'File not present', 'N/A'])
        continue

    try:
        with open(sql_file, "r") as file:
            second_list = []
            sql_content = file.read()

            # Remove comments from the SQL content
            cleaned_sql = remove_comments(sql_content)

            # Process each line in the cleaned SQL content
            for line in cleaned_sql.splitlines():
                tables = re.findall(
                    r'UPDATE[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|INTO[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|FROM[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|JOIN[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|TABLE[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|EXISTS[\n]*[\s]+[\w]*\.*[\w]+\.[\w]+|UPDATE[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|INTO[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|FROM[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|JOIN[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|TABLE[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|EXISTS[\n]*[\s]+\{[\w]+\}[\w]+\.[\w]+|UPDATE[\n]*[\s]+\{[\w]+\}\.[\w]+|INTO[\n]*[\s]+\{[\w]+\}\.[\w]+|FROM[\n]*[\s]+\{[\w]+\}\.[\w]+|JOIN[\n]*[\s]+\{[\w]+\}\.[\w]+|TABLE[\n]*[\s]+\{[\w]+\}\.[\w]+|EXISTS[\n]*[\s]+\{[\w]+\}\.[\w]+|UPDATE[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|INTO[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|FROM[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|JOIN[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|TABLE[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|EXISTS[\n]*[\s]+\{\{[\w]+\.[\w]+\}\}[\w]+\.[\w]+|UPDATE[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|INTO[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|FROM[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|JOIN[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|TABLE[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|EXISTS[\n]*[\s]+<[\w]+>[\w]+\.[\w]+|UPDATE[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>|INTO[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>|FROM[\n]*[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>|JOIN[\n]*[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>|TABLE[\n]*[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>|EXISTS[\n]*[\s]<[\w]+>[\w]+\.[\w]+<[\w]+>',
                    line.upper()
                )
                for table in tables:
                    if len(table) > 0:
                        first_list = [location, file_name, table.split()[0], table.split()[1]]
                        second_list.append(first_list)

            # Extend the list with data from the current file
            df_file.extend(second_list)

    except Exception as e:
        print(f"An error occurred while processing {sql_file}: {e}")
        df_file.append([location, file_name, 'Error', str(e)])

# Convert the list to a DataFrame and save as CSV
df = pd.DataFrame(df_file, columns=['Directory', 'File_name', 'DDL_statement', 'Table_name'])
df.to_csv(os.path.join(directory, f"Table_Fetch_From_SQL_File_{timestr}.csv"), index=False)

# Print summary
print(f"Processing completed. Results saved to {os.path.join(directory, f'Table_Fetch_From_SQL_File_{timestr}.csv')}")

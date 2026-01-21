import os
import re
import ast
import pandas as pd
import glob


def extract_task_details(file_path):
    """Extract task details from the given file."""
    task_pattern = re.compile(r'(\w+)\s*=\s*(\w+)\((.*?)\)\s*\n', re.DOTALL)
    tasks = []

    with open(file_path, 'r') as f:
        file_content = f.read()

    matches = re.findall(task_pattern, file_content)

    for match in matches:
        if match:
            task_name = match[0]
            operator_type = match[1]
            if task_name != "dag_sla":
                print(file_path.split('\\')[-1])
                container_command = None
                job_name = None
                Py_file_name = None
                if operator_type == "launch_k8s_api_job_operator":

                    pattern = r"container_command=\[(.*?)\]"
                    match1 = re.search(pattern, str(match), re.DOTALL)
                    if match1:
                        container_command = match1.group(0)
                        print("Extracted container_command:")
                        print(container_command)

                        try:
                            command_list_str = str(container_command).split('=')[1]

                            # Use `ast.literal_eval` to safely evaluate the string as a Python list
                            container_command_list = ast.literal_eval(command_list_str)

                            # The second element of the list is the path we're interested in
                            Py_file_name = container_command_list[1]

                            print(Py_file_name)
                        except:
                            print("Continue")
                    else:
                        print("No container_command found.")

                    pattern1 = r"container_image='([^']*)'"

                    match2 = re.search(pattern1, str(match), re.DOTALL)

                    if match2:
                        container_image = match2.group(1)
                        job_name = container_image.split('/')[-1].split(':')[0]
                        print("Container Image:", container_image, " Job Name :-", job_name)

                    else:
                        print("Container Image not found")

                if operator_type == "LivyOperator":
                    pattern = r"app_args=\[(.*?)\],"
                    matches = re.findall(pattern, str(match), re.DOTALL)
                    if matches:
                        container_command = matches[0].split(", ")
                        print(container_command)
                    else:
                        print("No match found")

                tasks.append({'file_path': file_path, 'file_name': file_path.split('\\')[-1], 'task_name': task_name,
                              'task_type': operator_type, 'Task': match, "Command": container_command,
                              "Py_File_Name": Py_file_name, "Container Job Name": job_name})

    return tasks


def find_files_with_dag_pattern(file_paths):
    """Given a list of file paths, find those that contain the DAG pattern."""
    pattern = r'with\s+DAG\((.*?)\)\s+as\s+dag:\n((?:\s+.*\n)*)'
    matching_files = []

    for file_path in file_paths:
        file_path = file_path.replace('\\', '\\')  # Normalize the file path
        print("File Name :- ", file_path)
        with open(file_path, 'r') as f:
            content = f.read()

        if re.findall(pattern, content, re.DOTALL):
            matching_files.append(file_path)

    return matching_files


def main():
    # Path to the root directory where Python files are located
    root_directory = r"D:\Input_Code"  # Replace with your root directory
    output_directory=r"D:\Output_Code"

    # Path to the CSV file containing the list of Python file names (without full paths)
    csv_file_path = 'Sprint_2_dag_name.csv'  # Replace with your actual CSV file path

    # Flag to enable/disable CSV processing (True = CSV mode, False = Normal mode)
    use_csv = True  # Set this flag to False if you want to run in normal mode without CSV

    if use_csv:
        # Read the CSV into a DataFrame
        df = pd.read_csv(os.path.join(root_directory,csv_file_path))
        print(df)

        # Ensure that the CSV has a column 'file_name' or adjust as needed
        if 'file_name' not in df.columns:
            print("Error: CSV file must contain a 'file_name' column.")
            return

        # Get all .py files in the directory (and subdirectories) using glob
        all_python_files = glob.glob(os.path.join(root_directory, '**', '*.py'), recursive=True)
        print(all_python_files)
        # Filter files by matching names from the CSV
        csv_file_names = df['file_name'].tolist()
        print(csv_file_names)
        # Normalize file names for comparison
        csv_file_names_normalized = [name.strip().lower() for name in csv_file_names]
        matching_files = [file for file in all_python_files if
                          os.path.basename(file).strip().lower() in csv_file_names_normalized]

        print(matching_files)
    else:
        # In normal mode, find all .py files in the root directory and subdirectories
        matching_files = glob.glob(os.path.join(root_directory, '**', '*.py'), recursive=True)

    # Process the matching files
    task_details = []
    for file_path in matching_files:
        file_tasks = extract_task_details(file_path)
        task_details.extend(file_tasks)

    if task_details:
        # Create DataFrame from task details
        result_df = pd.DataFrame(task_details)
        print(result_df)

        # Save DataFrame to CSV
        result_df.to_csv(os.path.join(output_directory,"57_ISF_analysis.csv"), index=False)
        print("Task details saved to 138_ISF_analysis.csv")
    else:
        print(f"No task details found with the given files.")


if __name__ == "__main__":
    main()

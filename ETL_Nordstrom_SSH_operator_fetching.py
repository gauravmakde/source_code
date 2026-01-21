import os
import csv
import re

directory_path = r'C:\Users\Saurabh.Honmane\Downloads\ISF-20240716T095859Z-001\ISF\Shared_by_Nordstrom_Airflow_DAGs_IsF_ISFExport_with_configs\ISFExport_with_configs'
csv_file = r'C:\Users\Saurabh.Honmane\Downloads\SSHOperator_count_test.csv'

ssh_operator_regex = re.compile(r'(\w+)\s*=\s*SSHOperator\((.*?)\)\n', re.DOTALL)
dag_pattern = re.compile(r"dag_id\s*=\s*'([^']+)'", re.DOTALL)
command_regex = re.compile(r'command\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"', re.DOTALL)
pattern1_regex = re.compile(r'-j\s+(\S+)')
#pattern2_regex = re.compile(r'-f\s+(\S+)')
pattern2_regex_f = re.compile(r'-f\s+(s3://\S+)')
pattern2_regex_t = re.compile(r'-t\s+(s3://\S+)')

with open(csv_file, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Root_Path', 'File_name', 'SSHOperator_Count', 'Dag_Id', 'Task_ID', 'SSHOperator_Details','Command','Pattern_1','Pattern_2'])

    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            if filename.endswith('.py'):
                file_path = os.path.join(root, filename)
                with open(file_path, 'r') as file:
                    file_contents = file.read()

                    ssh_operator_count = file_contents.count('SSHOperator')

                    ssh_operators = ssh_operator_regex.findall(file_contents)


                    if ssh_operator_count > 1:
                        file_name = os.path.basename(file_path)
                        dag_id_match = dag_pattern.search(file_contents)
                        dag_id = dag_id_match.group(1) if dag_id_match else 'N/A'

                        for var_name, ssh_operator_details in ssh_operators:
                            formatted_details = f"{var_name} = SSHOperator({ssh_operator_details.strip()})"

                            command_match = command_regex.search(ssh_operator_details)
                            command = command_match.group(0) if command_match else 'N/A'

                            Job_pattern_1 = pattern1_regex.search(command)
                            Pattern_Value1 = Job_pattern_1.group(1) if Job_pattern_1 else 'N/A'

                            #Job_pattern_2 = pattern2_regex.search(command)
                            #Pattern_Value2 = Job_pattern_2.group(1) if Job_pattern_2 else 'N/A'
                            job_pattern_2_f = pattern2_regex_f.search(command)
                            job_pattern_2_t = pattern2_regex_t.search(command)
                            if job_pattern_2_f:
                                Pattern_Value2 = job_pattern_2_f.group(1)
                            elif job_pattern_2_t:
                                Pattern_Value2 = job_pattern_2_t.group(1)
                            else:
                                Pattern_Value2 = 'N/A'

                            csvwriter.writerow([root, file_name, ssh_operator_count, dag_id, var_name, formatted_details, command, Pattern_Value1, Pattern_Value2])

print(f"SSHOperator details written in  {csv_file}")
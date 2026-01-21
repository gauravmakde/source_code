from croniter import croniter
from datetime import datetime, timezone
import glob
from datetime import date

dags = glob.glob('./pypeline_jobs/**.json', recursive=True)

for dag in dags:
    dag_lines = open(dag).readlines()
    for (num, line) in enumerate(dag_lines):
        print(num, line)
        if 'start_date' in line and 'pendulum' in line: # override existing start_date
            new_line = ''  # Remove any leading/trailing whitespace
            print(dag_lines[num])
            dag_lines[num] = new_line # Set the line to an empty string
            with open(dag, 'w') as file:
                file.writelines(dag_lines)
                break
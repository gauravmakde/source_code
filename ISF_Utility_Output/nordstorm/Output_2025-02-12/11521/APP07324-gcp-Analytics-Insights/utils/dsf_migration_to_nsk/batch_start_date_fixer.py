from croniter import croniter
from datetime import datetime, timezone
import glob
from datetime import date

########################################################################
# Sets all DAG cron times to their "previous" crontime from today      #
# This prevents DAG's from executing their "catchup" run automatically #
########################################################################

dags = glob.glob('./pypeline_jobs/*.json')

base = datetime.now(tz=timezone.utc)

for dag in dags:
    old_start_date = False
    dag_lines = open(dag).readlines()
    for (num, line) in enumerate(dag_lines):
        if 'start_date' in line and 'pendulum' in line: # override existing start_date
            old_start_date = num
        if 'dag_schedule' in line and 'None' not in line:
            target = num
            cron_time = dag_lines[target].split("""\"""")[3]
            print(cron_time)
            itr = croniter(cron_time, base)
            target_string= itr.get_prev(datetime).strftime('    "start_date": "pendulum.datetime(%Y, %-m, %-d, %-H, %-M)",')            
            if old_start_date:
                dag_lines[old_start_date] = target_string+"\n"
            else:
                dag_lines[target-1] = dag_lines[target-1]+target_string+"\n"
            print(dag_lines[target-1])
            with open(dag, 'w') as file:
                file.writelines(dag_lines)
            break
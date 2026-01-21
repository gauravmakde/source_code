####
### Removes AutoCommit and ET; from all SQL files and templates
####
import glob

all_sql = glob.glob('./**/*.sql', recursive=True) 

for file in all_sql:
    data = open(file).readlines()
    for (num, line) in enumerate(data):
         if "ET;" in line:
            data[num] = ''
         if "{autocommit_on};" in line:
            data[num] = ''
    with open(file, 'w') as file_new:
        file_new.writelines(data)
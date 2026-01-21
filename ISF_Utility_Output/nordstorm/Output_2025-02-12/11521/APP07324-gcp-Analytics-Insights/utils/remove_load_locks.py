import glob
import json
import os

all_sql = glob.glob('./**/*.sql', recursive=True) 

sql_files = set()

for file in all_sql:
    file_read = open(file).read()
    if 'CALL SYSDBA.RELEASE_LOAD_LOCK' not in file_read:
         continue
    data = open(file).readlines()
    for (num, line) in enumerate(data):
         if 'CALL SYSDBA.RELEASE_LOAD_LOCK'.lower() in line.lower():
              data[num] = ''
    with open(file, 'w') as file_new:
        file_new.writelines(data)
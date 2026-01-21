+# Reads in all JSON's, finds the spark_engine ask and adds `pause_idle_cluster_termination`
import glob
import sys
import os 

files = glob.glob('./pypeline_jobs/*.json')

# files = glob.glob('./templates/**/*.json', recursive=True)

for file in files:
    if '.json' in file:
        if 'pause_idle_cluster_termination' not in open(file).read() and 'spark_engine' in open(file).read():
            data = open(file).readlines()
            for (num, line) in enumerate(data):
                if 'spark_engine' in line:
                    data[num+1] = data[num+1]+'          "pause_idle_cluster_termination": "before_app",\n'
                    with open(file, 'w') as file_out:
                        file_out.writelines(data)

# Reads in all JSON's, finds the spark_engine ask and adds "retries": "0" for non-added files, regardless retries=0 setup in other engines
import glob

files = glob.glob('./pypeline_jobs/*.json')
#files = glob.glob('./templates/**/*.json', recursive=True)


for file in files:
    if '.json' in file:
        if 'spark_engine' in open(file).read():
            data = open(file).readlines()
            for (num, line) in enumerate(data):
                if 'spark_engine' in line:
                    if 'retries' in data[num+2]: 
                        continue
                    elif 'spark_engine' in line:
                        data[num+1] = data[num+1]+'          "retries": "0",\n'
                        with open(file, 'w') as file_out:
                           file_out.writelines(data)
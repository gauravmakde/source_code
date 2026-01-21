import os
import time
import pandas as pd


datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

f1=open('C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-06//first.txt', 'r')

final=[]
with open('C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-06//first.txt', 'r') as f:
    full_sql = f.read().split(';')
    final=[]
    for individual_sql in full_sql:
        # print("@@@@@@@@@@@@@@@")
        print(individual_sql)
        individual_bk=[]
        table=[]
        for file_line in individual_sql.split("\n"):

            if 'CREATE MULTISET TABLE' in file_line:
                table_name=file_line.split("TABLE")[1].split()[0].strip()
            if 'BK_' in file_line:
                word=file_line.strip().split()[0]
                if word.startswith('BK_'):
                    individual_bk.append(word)

        table.append(table_name)
        table.append(individual_bk)
        final.append(table)

final_dataframe = pd.DataFrame(final)
final_dataframe.columns=['Table_name','BK_columns']
final_dataframe.to_csv(directory + "\second_file_" + timestr + ".csv")

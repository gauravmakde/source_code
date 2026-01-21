import pandas as pd
import re
import os
import time
datestr=time.strftime("%Y-%m-%d")
timestr=time.strftime("%Y-%m-%d-%H%M")
directory='reports '+datestr

if not os.path.exists(directory):
    os.mkdir(directory)


df_1=pd.read_csv('C://Users//Gaurav.Makde//Downloads//Source_table.csv')
df_2=pd.read_csv('C://Users//Gaurav.Makde//Downloads//DDLs_Bigquery.csv')


final =[]
for j in df_1.iterrows():
    SourceName=j[1]['SourceName']
    SourceType=j[1]['SourceType']
    d=[]
    for k in df_2['ddl']:

        x=re.split('\`',k)
        name=x[1].split(".")
        table_name=name[2]
        if SourceName.lower()==table_name.lower():
            if SourceType=='view' and x[0].strip()=="CREATE VIEW":
                # print(TargetType)
                # print(x[0])
                print("Source name is ", SourceName)
                print("table_name is ", name)
                print("reference table is ",k.split("FROM")[1].strip())

                d.append(SourceType)
                d.append(SourceName)
                d.append(x[1])
                d.append(k.split("FROM")[1].strip())
                d.append(k)
                final.append(d)

dataframe=pd.DataFrame(final)
dataframe.columns=['SourceType','SourceName','Source_view_table','Target_Table','SQL']
dataframe.to_csv(directory+"\Final_table_response_"+timestr+".csv")
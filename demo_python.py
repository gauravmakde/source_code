import pandas as pd
import re
import os
import time
datestr=time.strftime("%Y-%m-%d")
timestr=time.strftime("%Y-%m-%d-%H%M")
directory='reports '+datestr

if not os.path.exists(directory):
    os.mkdir(directory)

df_final=pd.read_csv(directory+'\\View_BR_REFERENCEDB.csv')

e=[]
for dif in df_final.iterrows():
    StockPile_Dataset = dif[1]['StockPile_Dataset']
    Pre_Prod_Dataset = dif[1]['Pre-Prod_Dataset']
    Table_name = dif[1]['Table_name']


    d=[]
    project_id="gcp-edwprddata-prd-33200"


    sql=f'''
    create view `{project_id}.{Pre_Prod_Dataset}.{Table_name}`
    as (
    select * from `{project_id}.{StockPile_Dataset}.{Table_name}`
    );
    
    '''

    print(sql)
    d.append(StockPile_Dataset)
    d.append(Pre_Prod_Dataset)
    d.append(Table_name)
    d.append(sql)

    e.append(d)
    df=pd.DataFrame(e)
df.columns=['StockPile_Dataset','Pre-Prod_Dataset','Table_name','sql']

df.to_csv(directory+"\Final_sql_BRVW_REFERENCEDB_response_"+timestr+".csv")
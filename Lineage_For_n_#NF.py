import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

# tree = et.parse("C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-01//XML_read_input_file.xml")

df = pd.read_csv("reports 2024-02-27\\n_ar_trx_line_lineage.csv")
df_worflow=pd.read_csv("reports 2024-02-27\\wf_user_details.csv")
# print(df_worflow)
# print(df.columns)
distinct_level=sorted(df['level'].unique())

final_df=pd.DataFrame()
for df_level in distinct_level:
    df_level = [df_level]
    mask = df['level'].isin(df_level)
    active_customers = df[mask]
    # print(len(active_customers))
    imp_columns=active_customers[['TargetSchema','TargetName','SourceSchema','SourceName','wf_name','level']].sort_values(by=['TargetSchema','TargetName'])
    # newdf=imp_columns.merge(df_worflow,how='left',on='wf_name')
    # final_df = pd.concat([final_df, newdf], axis=1)
    # print(newdf)
    final_df = pd.concat([final_df, imp_columns], axis=1)
    print(final_df)


final_df.to_csv(directory + "\\3NF_Linage_n_ar_trx_line_lineage_lineage_" + timestr + ".csv")

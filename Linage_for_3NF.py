import os
import time
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

three_NF_table="n_direct_corp_adj_type_tv"
workflow='wf_N_DIRECT_CORP_ADJ_TYPE_TV'

if not os.path.exists(directory):
    os.mkdir(directory)

df=pd.read_csv(directory+'\\'+three_NF_table+'_lineage.csv')
# existing_3nf=pd.read_csv(directory+'\\3NF_table_lineage_completed.csv')  #coment when first time hitting the code

final_df=pd.DataFrame()

# def ittetation(df,final_df,target_table,count):
#
#
#     mask = df['TargetName'].isin(target_table)
#
#     active_customers = df[mask]
#     imp_columns = active_customers[
#         ['TargetSchema', 'TargetName', 'SourceSchema', 'SourceName', 'wf_name', 'level']].sort_values(
#         by=['TargetSchema', 'TargetName'])
#     imp_columns.columns = ['TargetSchema_'+ str(count), 'TargetName_'+ str(count), 'SourceSchema_'+ str(count), 'SourceName_' + str(count), 'wf_name_'+ str(count), 'level_'+ str(count)]
#     final_df = pd.concat([final_df, imp_columns], axis=1)
#     print(final_df)
#     return final_df


def ittetation(df,final_df,table_shcema,table_name,count):


    mask = df[['TargetSchema','TargetName']].eq([table_shcema,table_name]).all(axis=1)
    active_customers = df.loc[mask]

    imp_columns = active_customers[
        ['TargetSchema', 'TargetName', 'SourceSchema', 'SourceName', 'wf_name', 'level']].sort_values(
        by=['TargetSchema', 'TargetName'])
    imp_columns.columns = ['TargetSchema_'+ str(count), 'TargetName_'+ str(count), 'SourceSchema_'+ str(count), 'SourceName_' + str(count), 'wf_name_'+ str(count), 'level_'+ str(count)]
    print(imp_columns)
    final_df = pd.concat([final_df, imp_columns], axis=1)
    return final_df

count=1
table_shcema='finlglvwdb'
table_name = three_NF_table
three_nF_table=[table_name]


# existing_target=list(existing_3nf['table_schema_details']) #coment when first time hitting the code
existing_target=[] #uncoment when first time hitting the code

next_target=table_shcema+'.'+table_name


if next_target not in existing_target:
    existing_target.append(next_target)
    target_table = [table_name]
    print(next_target)


    final_df=ittetation(df,final_df,table_shcema,table_name,count)

    # print(final_df['SourceName_'+ str(count)])



print(existing_target)
print(len(final_df['SourceName_'+ str(count)]))

print("88888888888888888")
# if len (final_df)>0:
while (len(final_df['SourceName_' + str(count)] ) != 0
       and (len(final_df['SourceName_' + str(count)].unique())!=1  and not  pd.isna(final_df['SourceName_' + str(count)].unique()[0]))
):

       # and (len(final_df['SourceName_' + str(count)].unique())!=1  and not  pd.isna(final_df['SourceName_' + str(count)].unique()[0])))
# while len (final_df['SourceName_'+ str(count)])

    print("Andar hu bhai")
    if len (final_df['SourceName_'+ str(count)]):
        print("Inside the count :", count)
        for iter in final_df.iterrows():


            TargetSchema = iter[1]['TargetSchema_'+str(count)]
            TargetName = iter[1]['TargetName_'+str(count)]
            SourceSchema = iter[1]['SourceSchema_'+str(count)]
            SourceName = iter[1]['SourceName_'+str(count)]
            wf_name = iter[1]['wf_name_'+str(count)]
            level = iter[1]['level_'+str(count)]
            print(SourceName)

            if pd.isna(SourceName):
                continue
            else:
                next_target=SourceSchema+'.'+str(SourceName)
                print(next_target)
                target_table=[SourceName]
                if next_target not in existing_target :

                    existing_target.append(str(next_target))
                    print(existing_target)
                    print("Need to find the target for: ",next_target)
                    level=count+1
                    print(level)
                    final_df = ittetation(df,final_df,SourceSchema,SourceName,level)
                    # print(final_df)
                    print(final_df['SourceName_'+str(count)])
                else:
                    continue

        count += 1
# exit()
final_dataframe=pd.DataFrame(final_df)
final_dataframe.to_csv(directory + "\\3NF_Linage_" +three_NF_table +'_'+timestr + ".csv")
exit()
df_three_nF_table=pd.DataFrame(three_nF_table)
df_existing_target=pd.DataFrame(existing_target)
three_nF_table=pd.concat([df_three_nF_table,df_existing_target],axis=1)

final_dataframe.to_csv(directory + "\\3NF_Linage_" +three_NF_table +'_'+timestr + ".csv")
three_nF_table.columns=['3_nf_table','table_schema_details']
three_nF_table.to_csv(directory + "\\3NF_table_lineage_completed.csv")




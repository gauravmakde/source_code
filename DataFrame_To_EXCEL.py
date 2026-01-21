import pandas as pd
writer = pd.ExcelWriter("reports 2024-08-09\\Informatica_XML_Parsing_Report.xlsx", engine='xlsxwriter', mode='w')

df=pd.read_csv("C:\\Users\\Gaurav.Makde\\PycharmProjects\\pythonProject\\reports 2024-08-09\\dag_initial_details_2024-08-08-1557.csv")

# print(df)
x=[1,2]
for y in x:

    df.to_excel(writer, sheet_name='Metadata_'+str(y), startrow=1, header=True,index=False) # to add multiple sheet
    workbook = writer.book
    worksheet = writer.sheets['Metadata_'+str(y)]
    # Save and close the writer object to ensure data is written to the file
writer.close()

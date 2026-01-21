import os
import time
import re
import glob
import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

Enable_TZ_Column_check="Y"
Enable_TZ_view_column_check='Y'
InputFolderLst = glob.glob(directory + '//**//_tz_input.txt',recursive=True)
OutputFolderLSt=glob.glob("C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-08-28//Output//*.txt",recursive=True)

def fetch_td_timestamp_column(file):
    location = os.path.dirname(file)
    file_name = os.path.basename(file)
    with open(file,'r') as a:
        py=a.read()
        regex_for_table=r"CREATE\s*(\w*\s+)*TABLE\s+([\w.]+)"
        regex_for_timestamp=r"\s*(\w*)\s*TIMESTAMP\(\d{1}\)\s*WITH TIME ZONE"
        search_table_name=re.search(regex_for_table,py)
        table_name=search_table_name.group(2) if search_table_name else "None"

        all_Column_timestamp= re.findall(regex_for_timestamp, py)


        return (all_Column_timestamp,table_name)

def fetch_bq_table_timestamp_column(file,source_table_name,all_Column_timestamp,file_name,location):
    d=[]
    location = os.path.dirname(file)
    file_name = os.path.basename(file)
    with open(file,'r') as a:
        py=a.read()
        regex_for_table=r"CREATE\s*(\w*\s+)*TABLE\s+([\w.]+)"
        search_table_name=re.search(regex_for_table,py)
        output_table_name=search_table_name.group(2) if search_table_name else "None"
        if output_table_name.lower()==source_table_name.lower():

            Status='Y'
            for single_tmp in all_Column_timestamp:
                if single_tmp.lower() not in py.lower() and single_tmp.lower()+"_tz" not in py.lower():
                    Status='N'
            d.append({
                'location':location,
                'file_name':file_name,
                'table_name':output_table_name,
                'view_name':'None',
                'Final Delivery Status':Status

            })
            return d,output_table_name

def fetch_bq_view_timestamp_column(file,source_table_name,all_Column_timestamp,file_name,location):


    d=[]

    for next_file in file:
        with (open(next_file, 'r') as nf):
            location = os.path.dirname(next_file)
            file_name = os.path.basename(next_file)

            print("File name :",file_name)
            content = nf.read()

            print(source_table_name)

            regex_for_view=r"CREATE\s*(\w*\s+)*VIEW\s+([\w.]+)"
            regex_from_table=r"\s*FROM\s*([^;]*);"

            search_view_name=re.search(regex_for_view,content)
            output_view_name=search_view_name.group(2) if search_view_name else "None"
            #
            search_from_table_name = re.search(regex_from_table, content)

            output_from_table_name = search_from_table_name.group(1) if search_from_table_name else "None"
            if output_from_table_name.lower()==source_table_name.lower():

            #
                Status='Y'

                for single_tmp in all_Column_timestamp:
                    print(single_tmp)
                    if (single_tmp.lower() not in content.lower() and single_tmp.lower()+"_tz" not in content.lower() and single_tmp.lower()+"_utc" not in content.lower()) :
                        print("3 column not found")
                        if ('SELECT *' not in content):
                            Status='N'
                d.append({
                    'location':location,
                    'file_name':file_name,
                    'table_name':output_from_table_name,
                    'view_name': output_view_name,
                    'Final Delivery Status':Status

                })
                print(d)

                if output_from_table_name and output_from_table_name != output_view_name:

                    print("Searching for next file corresponding to:", output_view_name)

                    # Search for the next file in the location
                    next_file_pattern = os.path.join(location, '*.txt')
                    next_files = glob.glob(next_file_pattern, recursive=True)

                    d.extend(fetch_bq_view_timestamp_column(next_files, output_view_name, all_Column_timestamp,os.path.basename(next_file), os.path.dirname(next_file)))

    return d



if __name__ == "__main__":
    data = []
    error = []
    for file in InputFolderLst:
        location = os.path.dirname(file)
        file_name = os.path.basename(file)
        print("Location of file:", location)
        print("file name :", file_name)

        if Enable_TZ_Column_check == "Y":

            all_Column_timestamp,table_name=fetch_td_timestamp_column(file)


        if (len(all_Column_timestamp) > 0 and table_name):
            table_status,output_table_name = fetch_bq_table_timestamp_column(
                "C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-08-28//Output//_tz_output_2.txt",table_name, all_Column_timestamp, file_name, location)

        # print(table_status)
        data.extend(table_status)


        if Enable_TZ_Column_check == "Y" and Enable_TZ_view_column_check=='Y' and output_table_name:
            table_status = fetch_bq_view_timestamp_column(
                OutputFolderLSt,
                output_table_name, all_Column_timestamp, file_name, location)


        print(table_status)
        data.extend(table_status)
        print(data)




        df=pd.DataFrame(data)
        print(df)
        # df.to_csv(os.path.join(directory,'final_input.csv'))
        #
        # final_status = data['Final Delivery Status']
        # yes_count = final_status.count('Y')
        # no_count = final_status.count('N')
        # total_count = yes_count + no_count
        #
        # get_time = datetime.datetime.now()
        # get_time = get_time.strftime("%d-%m-%Y %H-%M-%S")
        #
        # excel_sheet_name = str(
        #     'Project_Name_For_Assurance_Report' + " - Code Conversion Assurance Report " + get_time + ".xlsx")
        # # excel_sheet_name=str(Project_Name_For_Assurance_Report+" - Code Conversion Assurance Report "+".xlsx")
        # writer = pd.ExcelWriter(excel_sheet_name, engine='xlsxwriter')
        # df.to_excel(writer, sheet_name='Assurance_Report', startrow=8, header=False, index=False)
        #
        # workbook = writer.book
        # worksheet = writer.sheets['Assurance_Report']
        #
        # # Add a header format.
        # header_format = workbook.add_format({
        #     'bold': True,
        #     'text_wrap': True,
        #     'valign': 'valign',
        #     'align': 'center',
        #     'fg_color': '#3c78d8',
        #     'color': 'white',
        #     'border': 1})
        # legend_format = workbook.add_format({
        #     'bold': True,
        #     'text_wrap': True,
        #     'valign': 'valign',
        #     'align': 'left',
        #     'color': 'black',
        #     'border': 1})
        # legend_format1 = workbook.add_format({
        #     'bold': True,
        #     'text_wrap': True,
        #     'valign': 'valign',
        #     'align': 'center',
        #     'color': 'black',
        #     'border': 1})
        #
        # format = workbook.add_format({'align': 'center', 'valign': 'valign', 'text_wrap': True})
        # format1 = workbook.add_format({'align': 'left', 'valign': 'valign', 'text_wrap': True})
        # border_format = workbook.add_format({
        #     'border': 1,
        #     'align': 'vcenter',
        #     'bold': True,
        #     'font_size': 15,
        # })
        # # Write the column headers with the defined format.
        # for col_num, value in enumerate(df.columns.values):
        #     val = 'A8:' + str(chr(65 + len(data) - 1)) + str(len(file_name) + 9) + ''
        #     worksheet.write(7, col_num, value, header_format)
        #     worksheet.set_column(col_num, col_num, 25, format)
        #     worksheet.hide_gridlines(2)
        #     worksheet.set_default_row(25)
        #     worksheet.conditional_format(val, {'type': 'no_blanks', 'format': border_format})
        #
        #     worksheet.write_column('A1:', ["NOTE:"], header_format)
        #     worksheet.write_column('A2:', ["   1. Y = Yes(Verified)", "   2. N = No", "   3. CM = Checked Manually",
        #                                    "   4. NA = Not Applicable"], legend_format)
        #
        #     # Total count
        #     worksheet.write_column('C2:', ["Final Delivery Status"], header_format)
        #     worksheet.write_column('D2:', ["Count"], header_format)
        #     worksheet.write_column('C3:', ["Y"], legend_format1)
        #     worksheet.write_column('C4:', ["N"], legend_format1)
        #     worksheet.write_column('C5:', ["Total Count"], header_format)
        #
        #     worksheet.write_column('D3:', [yes_count], legend_format1)
        #     worksheet.write_column('D4:', [no_count], legend_format1)
        #     worksheet.write_column('D5:', [total_count], header_format)
        #
        #     if col_num == 0:
        #         worksheet.set_column(col_num, col_num, 45, format1)
        # # Close the Pandas Excel writer and output the Excel file.
        # writer.close()
        # print("Excel Sheet Name : ", excel_sheet_name)
        # print("Report Generated")


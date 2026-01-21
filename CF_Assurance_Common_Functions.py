import pandas as pd
import fastavro
import glob
import sqlite3
import os, re,sys
from google.cloud import bigquery
from google.cloud import bigquery
import configparser
import datetime
import chardet

def create_database(Extracted_AVRO_File_Path,Enter_Project_Name):
    fileList = glob.glob(Extracted_AVRO_File_Path+"*/*", recursive=True)
    for fileName in fileList:
        try:
            # if os.path.exists(Enter_Project_Name+'_AVRO.db'):
            #     pass
            # else:
            sqliteConnection = sqlite3.connect(Enter_Project_Name+'_AVRO.db')
            cursor = sqliteConnection.cursor()
            with open(fileName, 'rb') as f:
                reader = fastavro.reader(f)
                # Load records in memory
                pd.set_option("display.max_columns", None)
                records = [r for r in reader]
                df = pd.DataFrame.from_records(records)
                column_list=[]
                if "constraint_e" in fileName:
                    data=df['columns']
                    for item in data:
                        column_list.append(str(item))
                    df.drop(['columns'],axis=1,inplace=True)
                    df['columns']=column_list

                pos=len(fileName.split('/'))
                fileName=fileName.split('/')[pos-2]
                df.to_sql(fileName,con=sqliteConnection, schema=None, if_exists='append')
                cursor.close()
        except:
            pass

def create_connection(Enter_Project_Name):
    conn = None
    try:
        conn = sqlite3.connect(Enter_Project_Name+'_AVRO.db')
    except Exception as e:
        #print(e)
        pass

    return conn


def DateTime():
    log_datetime = datetime.datetime.now()
    return log_datetime

#use for append feature name in log file
def Common(feature_name):
    log_datetime=DateTime()
    feature_name=str(log_datetime)+" "+"*"*20 +" "+ feature_name +" "+"*"*20+"\n"
    return feature_name

#use for if Result is no
def Result_No(input_file,output_file):
    log_datetime=DateTime()
    #Input File Result
    counter=1
    result_no=str(log_datetime)+" "+"Input File= \n"+"\t"*5
    for item in input_file:
        item=item.replace("\n"," ")
        result_no=result_no+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    #Output File Result
    counter=1
    result_no=result_no+"\n"+str(log_datetime)+" "+"Output File= \n"+"\t"*5
    for item in output_file:
        item=item.replace("\n"," ")
        result_no=result_no+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    result_no=result_no+"\n"+str(log_datetime)+" "+"Above condition is not satisfied\n"+str(log_datetime)+" "+"Final_Result=N\n\n"
    return result_no

#use for if Result is Yes
def Result_Yes(input_file,output_file):
    log_datetime=DateTime()
    #input File Result
    counter=1
    result_yes=str(log_datetime)+" "+"Input File= \n"+"\t"*5
    for item in input_file:
        item=item.replace("\n"," ")
        result_yes=result_yes+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    #Output File Result
    counter=1
    result_yes=result_yes+"\n"+str(log_datetime)+" "+"Output File= \n"+"\t"*5
    for item in output_file:
        item=item.replace("\n"," ")
        result_yes=result_yes+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    result_yes=result_yes+"\n"+str(log_datetime)+" "+"Above condition is satisfied\n"+str(log_datetime)+" "+"Final_Result=Y\n\n"
    return result_yes

def Output_Result_No(output_file):
    log_datetime=DateTime()
    #Output File Result
    counter=1
    result_no=str(log_datetime)+" "+"Output File= \n"+"\t"*5
    for item in output_file:
        item=item.replace("\n"," ")
        result_no=result_no+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    result_no=result_no+"\n"+str(log_datetime)+" "+"Above condition is not satisfied\n"+str(log_datetime)+" "+"Final_Result=N\n\n"
    return result_no

#use for if Result is Yes
def Output_Result_Yes(output_file):
    log_datetime=DateTime()
    #Output File Result
    counter=1
    result_yes=str(log_datetime)+" "+"Output File= \n"+"\t"*5
    for item in output_file:
        item=item.replace("\n"," ")
        result_yes=result_yes+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    result_yes=result_yes+"\n"+str(log_datetime)+" "+"Above condition is satisfied\n"+str(log_datetime)+" "+"Final_Result=Y\n\n"
    return result_yes


#use for if Result is NA
def Result_NA():
    log_datetime=DateTime()
    result_na="\n"+str(log_datetime)+" "+"Above funationality not present in input and output file.\n"+str(log_datetime)+" "+"Final_Result=NA\n\n"
    return result_na

def Result_CM(msg):
    log_datetime=DateTime()
    result_cm="\n"+str(log_datetime)+" "+msg+"\n"+str(log_datetime)+" "+"Final_Result=CM\n\n"
    return result_cm

#use for if Result is Encoding
def Result_Encoding():
    log_datetime=DateTime()
    result_encoding="\n"+str(log_datetime)+" "+"File has different encoding style\n"+str(log_datetime)+" "+"Final_Result=N\n\n"
    return result_encoding

def Result_Metadata(output_file):
    log_datetime=DateTime()
    #Output File Result
    counter=1
    result_yes=str(log_datetime)+" "+"Output File= \n"+"\t"*5
    for item in output_file:
        item=item.replace("\n"," ")
        result_yes=result_yes+str(counter)+")"+" "+item+"\n"+"\t"*5
        counter+=1

    result_yes=result_yes+"\n"+str(log_datetime)+" "+"Table/View is not present in Dictionary.\n"+str(log_datetime)+" "+"Final_Result=Meatadata\n\n"
    return result_yes


def Config_Setup():
    config = configparser.ConfigParser()
    execPath = os.getcwd()
    config.read(os.path.join(execPath, 'Config_Automation.ini'))
    inputFolderPath=config['DEFAULT']['InputFolderPath']
    OutputFolderPath=config['DEFAULT']['OutputFolderPath']
    cred_ValidateQueryOnBQ = (config['ValidateQueryOnBQ']['Json_path'])
    Append_project_id = (config['ValidateQueryOnBQ']['Project_ID_Name'])
    input_directory = os.listdir(inputFolderPath)
    output_directory= os.listdir(OutputFolderPath)
    inputFile=[]
    outputFile=[]
    for file1 in input_directory:
        inputFile.append(file1)

    for file2 in output_directory:
        try:
            outputFile.append(file2)
            fileName_O = file2
            if 'translated_' in file2.lower():
                file2 = re.sub("translated_", "",file2,flags=re.I)
                os.rename(OutputFolderPath+fileName_O,OutputFolderPath+file2)
                outputFile.append(file2)
        except:
            pass

    same_file=list(set(inputFile) & set(outputFile))
    same_file.sort()
    return same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ

#Check file is validate_on_BQ
# def validate_on_BQ(log_result,filename,BQ):
#     log_datetime=DateTime()
#     same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
#     log_result.append(Common("Validate on BQ"))
#     errorFileCounter = 0
#     clientBQ = bigquery.Client.from_service_account_json(json_credentials_path=cred_ValidateQueryOnBQ)
#     try:
#         fileReopen = open (OutputFolderPath+filename, "r")
#         completeFileContent = fileReopen.read()
#         job = clientBQ.query(completeFileContent)
#         if job.result():
#             BQ.append("Y")
#             log_result.append(str(log_datetime)+" "+"Final_Result=Y")
#     except:
#         errorFileCounter += 1
#         BQ.append("N")
#         log_result.append("Unexpected error: {},Description: {},Line No: {}\n\n\n".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
#         log_result.append(str(log_datetime)+" "+"Final_Result=N")

def validate_on_BQ(log_result,filename,BQ, clientBQ):
    log_datetime=DateTime()
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Validate on BQ"))
    errorFileCounter = 0
    # clientBQ = bigquery.Client.from_service_account_json(json_credentials_path=cred_ValidateQueryOnBQ)
    try:
        fileReopen = open (OutputFolderPath+filename, "r")
        completeFileContent = fileReopen.read()
        job = clientBQ.query(completeFileContent)
        if job.result():
            BQ.append("Y")
            log_result.append(str(log_datetime)+" "+"Final_Result=Y\n\n")
    except:
        errorFileCounter += 1
        BQ.append("N")
        log_result.append("Unexpected error: {},Description: {},Line No: {}\n\n\n".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
        log_result.append(str(log_datetime)+" "+"Final_Result=N\n\n")

def execute_on_BQ(log_result,filename,executeOnBQ, clientBQ):
    log_datetime=DateTime()
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Execute on BQ"))
    errorFileCounter = 0
    # clientBQ = bigquery.Client.from_service_account_json(json_credentials_path=cred_ValidateQueryOnBQ)
    try:
        fileReopen = open (OutputFolderPath+filename, "r")
        completeFileContent = fileReopen.read()
        object_Findings=re.findall(r'TABLE[\s]+(\`*[\w\-]+\`*\.*\`*[\w\-]+\`*\.\`*[\w\-]+\`*)|'
                                   r'EXISTS[\s]+(\`*[\w\-]+\`*\.*\`*[\w\-]+\`*\.\`*[\w\-]+\`*)|'
                                   r'VIEW[\s]+(\`*[\w\-]+\`*\.*\`*[\w\-]+\`*\.\`*[\w\-]+\`*)|'
                                   r'PROCEDURE[\s]+(\`*[\w\-]+\`*\.*\`*[\w\-]+\`*\.\`*[\w\-]+\`*)',completeFileContent,flags=re.I|re.DOTALL)
        if object_Findings:
            name=[i for i in object_Findings[0] if i!=''][0]
        schema=name.split('.')[1]
        schema=schema.strip('`')
        object_name=name.split('.')[2]
        object_name=object_name.strip('`')
        invokeStmt="SELECT * FROM foodmart.Get_Invoke_Statements where LOWER(datasetname)= LOWER('"+schema+"') and LOWER(objectname)= LOWER('"+object_name+"');"
        job = clientBQ.query(invokeStmt)
        for row in job:
            invokeResult=clientBQ.query(row[3])
            for stmt in invokeResult:
                if stmt[0]=='Success':
                    executeOnBQ.append("Y")
                    log_result.append(str(log_datetime)+" "+"Final_Result=Y\n\n")

                elif stmt[0]=='Failed':
                    executeOnBQ.append("N")
                    log_result.append(stmt[1]+"\n")
                    log_result.append(str(log_datetime)+" "+"Final_Result=N\n\n")
    except:
        errorFileCounter += 1
        executeOnBQ.append("N")
        log_result.append("Unexpected error: {},Description: {},Line No: {}\n\n\n".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
        log_result.append(str(log_datetime)+" "+"Final_Result=N\n\n")


def Log_Files_Folder():
    Logs_Files="Logs_Files/"
    try:
        os.mkdir(Logs_Files)
    except OSError:
        pass

    Success_Logs=Logs_Files+"Success_Logs/"
    try:
        os.mkdir(Success_Logs)
    except OSError:
        pass

    Falied_Logs=Logs_Files+"Falied_Logs/"
    try:
        os.mkdir(Falied_Logs)
    except OSError:
        pass
    return Success_Logs,Falied_Logs

def Log_Files(log_result,final_result,feature_dict):
    status=''
    Success_Logs,Falied_Logs=Log_Files_Folder()
    lst1=[]
    for key in feature_dict:
        lst=feature_dict[key]
        lst1.append(lst[-1])
    for val in lst1:
        if val=='N' or val=="Metadata" or 'N(' in val:
            status='N'
            break
        else:
            status='Y'
    fileName=lst1[0]
    try:
        fileName_O = fileName
        if '.txt' in fileName.lower():
            fileName = fileName.replace(".txt", ".log")
        if '.bteq' in fileName.lower():
            fileName = fileName.replace('.bteq', '.log')
        if '.ksh' in fileName.lower():
            fileName = fileName.replace('.ksh', '.log')
        if '.sql' in fileName.lower():
            fileName = fileName.replace('.sql', '.log')
        elif '.log' not in fileName.lower():
            fileName = fileName + (".log")
        os.rename(fileName_O,fileName)
    except:
        pass
    if status=='Y':
        final_result.append("Y")
        with open(Success_Logs+fileName,"w") as write_file:
            for i in log_result:
                write_file.write(str(i))
    else:
        final_result.append("N")
        with open(Falied_Logs+fileName,"w") as write_file:
            for i in log_result:
                write_file.write(str(i))

def Excel_Write(file_name,feature_dict,Project_Name_For_Assurance_Report):
    df=pd.DataFrame(feature_dict)
    final_status=feature_dict['Final Delivery Status']
    yes_count=final_status.count('Y')
    no_count=final_status.count('N')
    total_count=yes_count+no_count

    get_time=datetime.datetime.now()
    get_time=get_time.strftime("%d-%m-%Y %H-%M-%S")
    
    excel_sheet_name=str(Project_Name_For_Assurance_Report+" - Code Conversion Assurance Report "+get_time+".xlsx")
    # excel_sheet_name=str(Project_Name_For_Assurance_Report+" - Code Conversion Assurance Report "+".xlsx")
    writer = pd.ExcelWriter(excel_sheet_name, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Assurance_Report', startrow=8, header=False,index=False)

    workbook  = writer.book
    worksheet = writer.sheets['Assurance_Report']

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'valign',
        'align':'center',
        'fg_color': '#3c78d8',
        'color':'white',
        'border': 1})
    legend_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'valign',
        'align':'left',
        'color':'black',
        'border': 1})
    legend_format1 = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'valign',
        'align':'center',
        'color':'black',
        'border': 1})

    format=workbook.add_format({'align':'center','valign':'valign','text_wrap':True})
    format1=workbook.add_format({'align':'left','valign':'valign','text_wrap':True})
    border_format=workbook.add_format({
        'border':1,
        'align':'vcenter',
        'bold': True,
        'font_size':15,
    })
    # Write the column headers with the defined format.
    for col_num, value in enumerate(df.columns.values):
        val='A8:'+str(chr(65+len(feature_dict)-1))+str(len(file_name)+9)+''
        worksheet.write(7, col_num, value, header_format)
        worksheet.set_column(col_num,col_num,25,format)
        worksheet.hide_gridlines(2)
        worksheet.set_default_row(25)
        worksheet.conditional_format( val , { 'type' : 'no_blanks' , 'format' : border_format} )

        worksheet.write_column('A1:', ["NOTE:"],header_format)
        worksheet.write_column('A2:', ["   1. Y = Yes(Verified)","   2. N = No","   3. CM = Checked Manually","   4. NA = Not Applicable"],legend_format)

        # Total count
        worksheet.write_column('C2:', ["Final Delivery Status"],header_format)
        worksheet.write_column('D2:', ["Count"],header_format)
        worksheet.write_column('C3:', ["Y"],legend_format1)
        worksheet.write_column('C4:', ["N"],legend_format1)
        worksheet.write_column('C5:', ["Total Count"],header_format)

        worksheet.write_column('D3:', [yes_count],legend_format1)
        worksheet.write_column('D4:', [no_count],legend_format1)
        worksheet.write_column('D5:', [total_count],header_format)


        if col_num==0:
            worksheet.set_column(col_num,col_num,45,format1)
    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    print("Excel Sheet Name : ",excel_sheet_name)
    print("Report Generated")

flagI=re.I
flagD=re.DOTALL
flagID=re.I|re.DOTALL

class COMMON:
    

    def Read_file(self, file):
        with open(file, "rb") as f:
            result=chardet.detect(f.read())
            e=result['encoding']
        with open(file, "r", encoding=e) as f:
            file_obj=f.read()
        return file_obj

    def Readlines_file(self, file):
        with open(file, "rb") as f:
            result=chardet.detect(f.read())
            e=result['encoding']
        with open(file, "r", encoding=e) as f:
            file_obj=f.readlines()
        return file_obj

    def Remove_comments(self, file):
        fileLines = common_obj.Readlines_file(file)
        comment = 0
        counter = 0
        obj = ''
        final_obj = ''
        for fileLine in fileLines:
            if "/*" in fileLine:
                if '*/' in fileLine:
                    comment = 0
                    pass
                else:
                    comment = 1
                    pass
            elif comment == 1:
                if "*/" in fileLine:
                    comment = 0
                    pass
                else:
                    pass
            elif fileLine.startswith("--"):
                pass
            elif counter == 0:
                obj = ''
                obj = obj + fileLine
                final_obj = final_obj + obj
            # else:
            #     final_obj=final_obj+fileLine
        return final_obj

common_obj = COMMON()






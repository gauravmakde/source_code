from CF_Assurance_Common_Functions import *
import re, chardet
import sqlparse
#Netezza To BQ

def check_project_id_for_NZ(log_result,filename,directory,project_id_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Project ID"))
    try:
        input_log=[]
        file_obj=""
        if (filename,directory) in inputFile:
            comment=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                #file_obj = f.read()
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine
                project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*|FROM[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
                if project_id:
                    #print(project_id)
                    for ele in project_id:
                        if ele=='':
                            pass
                        else:
                            input_log.append(ele)
            # print(input_log)

            if (filename,directory) in outputFile:
                comment=0
                file_obj=''
                with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                    #file_obj = f.read()
                    fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine
                project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
                                      r'JOIN[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|FROM[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|PROCEDURE[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|EXISTS[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|INTO[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|UPDATE[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+|TABLE[\s]+\`'+Append_project_id+'\`\.[\w]+\.[\w]+',file_obj,flags=re.I)
                # print(filename)
                # print(project_id)
                if project_id:
                    project_id_result_for_NZ.append("N")
                    log_result.append(Output_Result_No(project_id))
                else:
                    project_id=re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
                    #print(project_id)
                    if project_id:
                        project_id_result_for_NZ.append("Y")
                        log_result.append(Output_Result_Yes(project_id))
                    else:
                        project_id_result_for_NZ.append("NA")
                        log_result.append(Result_NA())
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        project_id_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

    # try:
    #     input_log=[]
    #     if (filename,directory) in inputFile:
    #         with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
    #             file_obj = f.read()
    #             project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*\(|FROM[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*[\w]+\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
    #             for ele in project_id:
    #                 if ele=='':
    #                     pass
    #                 else:
    #                     input_log.append(ele)
    #
    #         if (filename,directory) in outputFile:
    #             with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
    #                 file_obj = f.read()
    #                 project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*\(|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
    #                 if project_id:
    #                     project_id_result_for_NZ.append("N")
    #                     log_result.append(Result_No(input_log,project_id))
    #                 else:
    #                     project_id=re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\(|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
    #                     if project_id:
    #                         project_id_result_for_NZ.append("Y")
    #                         log_result.append(Result_Yes(input_log,project_id))
    #                     else:
    #                         project_id_result_for_NZ.append("NA")
    #                         log_result.append(Result_NA())
    #
    # except:
    #     print("Unexpected error:", sys.exc_info()[0])
    #     project_id_result_for_NZ.append("Encoding")
    #     log_result.append(Result_Encoding())
    #     print("Error in processing: ", filename)

def check_datatype_for_NZ(log_result,filename,directory,Datatype_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Datatype Checking"))
    integer_column=[]
    input_all_datatype=[]
    output_all_datatype=[]
    BQ_input_cloumn=[]
    BQ_output_cloumn=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                table_obj=''
                table_counter=0
                dict_key_counter=0
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if table_counter==0:
                            table_obj=''
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                            if find_table:
                                table_obj=table_obj+fileLine
                                table_counter=1


                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                find_integer=re.findall(r"[\w]+[\s]+INTEGER[\s]+|[\w]+[\s]+INTEGER\,|[\w]+[\s]+BYTEINT[\s]+|[\w]+[\s]+BYTEINT\,|[\w]+[\s]+BIGINT[\s]+|[\w]+[\s]+BIGINT\,|[\w]+[\s]+SMALLINT[\s]+|[\w]+[\s]+SMALLINT\,|[\w]+[\s]+INT4[\s]+|[\w]+[\s]+INT4\,|[\w]+[\s]+INT8[\s]+|[\w]+[\s]+INT8\,",fileLine,flags=re.I)
                                for ele in find_integer:
                                    replce_integer=re.sub(r"[\s]+INT4[\s]*","INTEGER",ele,flags=re.I)
                                    replce_integer=re.sub(r"[\s]+INT8[\s]*","BIGINT",replce_integer,flags=re.I)
                                    replce_integer=re.sub(r"[\s]+|\,","",replce_integer,flags=re.I)
                                    if replce_integer.lower()=="andinteger" or replce_integer.lower()=="asinteger" or replce_integer=="":
                                        pass
                                    else:
                                        input_all_datatype.append(replce_integer.lower())
                                        BQ_input_cloumn.append(ele.lower())

                                find_numeric=re.findall(r"[\w]+[\s]+NUMERIC[\s]+|[\w]+[\s]+NUMERIC\,|[\w]+[\s]+DECIMAL[\s]+|[\w]+[\s]+DECIMAL\,",fileLine,flags=re.I)
                                for ele in find_numeric:
                                    ele=ele.replace(",","")
                                    replce_numeric=ele+"(18,0)"
                                    if ele.lower()=="andnumeric(18,0)" or ele.lower()=="asnumeric(18,0)" or ele=="":
                                        pass
                                    else:
                                        replce_numeric=re.sub(r"DECIMAL","NUMERIC",replce_numeric,flags=re.I)
                                        replce_numeric=re.sub(r"[\s]+","",replce_numeric,flags=re.I)
                                        input_all_datatype.append(replce_numeric.lower())
                                        BQ_input_cloumn.append(ele.lower())

                                find_numeric=re.findall(r"[\w]+[\s]+NUMERIC\([\d]+\)[\s]+|[\w]+[\s]+NUMERIC\([\d]+\)\,|[\w]+[\s]+NUMERIC\([\d]+\,[\d]+\)[\s]+|[\w]+[\s]+NUMERIC\([\d]+\,[\d]+\)\,",fileLine,flags=re.I)
                                for ele in find_numeric:
                                    x=re.findall(r'\([\d]+\)',ele)
                                    if x:
                                        ele=ele.replace(")",",0)")
                                    ele=ele.replace("),",")")
                                    if "andnumeric" in ele.lower() or "asnumeric" in ele.lower() or ele=="":
                                        pass
                                    else:
                                        find_numeric_digit=re.findall(r"[\d]+\,[\d]+",ele.upper())
                                        find_numeric_digit="".join(find_numeric_digit)
                                        find_numeric_digit=find_numeric_digit.split(",")
                                        if find_numeric_digit:
                                            if int(find_numeric_digit[0])>=38 or int(find_numeric_digit[1])>9:
                                                replce_numeric=ele.upper().replace("NUMERIC","BIGNUMERIC")
                                                replce_numeric=re.sub(r"[\s]+","",replce_numeric,flags=re.I)
                                                input_all_datatype.append(replce_numeric.lower())
                                                BQ_input_cloumn.append(ele.lower())
                                            else:
                                                replce_numeric=re.sub(r"[\s]+","",ele,flags=re.I)
                                                input_all_datatype.append(replce_numeric.lower())
                                                BQ_input_cloumn.append(ele.lower())


                                find_float=re.findall(r"[\w]+[\s]+FLOAT\([\d]+\)[\s]+|[\w]+[\s]+FLOAT\([\d]+\)\,",fileLine,flags=re.I)
                                for ele in find_float:

                                    x=re.findall(r'\(([\d]+)\)',ele)
                                    if x:
                                        if int(x[0])<=6 and int(x[0])>=0:
                                            replace_float=re.sub(r"\([\d]+\)","(6,0)",ele,flags=re.I)
                                            replace_float=re.sub(r"[\s]+","",replace_float,flags=re.I)
                                            input_all_datatype.append(replace_float.lower())
                                            BQ_input_cloumn.append(ele.lower())
                                        elif int(x[0])>=7 and int(x[0])<=15:
                                            replace_float=re.sub(r"\([\d]+\)","(15,0)",ele,flags=re.I)
                                            replace_float=re.sub(r"[\s]+","",replace_float,flags=re.I)
                                            input_all_datatype.append(replace_float.lower())
                                            BQ_input_cloumn.append(ele.lower())

                                find_string=re.findall(r"[\w]+[\s]+CHARACTER\([\d]+\)[\s]*|[\w]+[\s]+CHAR\([\d]+\)[\s]*|[\w]+[\s]+VARCHAR\([\d]+\)[\s]*|[\w]+[\s]+NCHAR\([\d]+\)[\s]*|[\w]+[\s]+NVARCHAR\([\d]+\)[\s]*|[\w]+[\s]+NATIONAL[\s]+CHARACTER[\s]+VARYING\([\d+]\)|[\w]+[\s]+CHARACTER[\s]+VARYING\([\d]+\)",fileLine,flags=re.I)
                                for ele in find_string:
                                    # print(ele)
                                    replce_string=re.sub(r" NATIONAL[\s]+CHARACTER[\s]+VARYING\(| CHARACTER[\s]+VARYING\(| CHARACTER\(| CHAR\(| VARCHAR\(| NCHAR\(| NVARCHAR\(","STRING(",ele,flags=re.I)
                                    replce_string=re.sub(r"[\s]+","",replce_string,flags=re.I)
                                    if "asstring(" in replce_string.lower():
                                        pass
                                    else:
                                        input_all_datatype.append(replce_string.lower())
                                        BQ_input_cloumn.append(ele.lower())

                                find_all=re.findall(r"[\w]+[\s]+REAL[\s]+|[\w]+[\s]+REAL\,|[\w]+[\s]+DOUBLE[\s]+PRECISION[\s]+|[\w]+[\s]+DOUBLE[\s]+PRECISION\,|[\w]+[\s]+BOOLEAN[\s]+|[\w]+[\s]+BOOLEAN\,|[\w]+[\s]+VARBINARY\([\d]+\)|[\w]+[\s]+ST_GEOMETRY\([\d]+\)|[\w]+[\s]+TEXT[\s]+|[\w]+[\s]+TEXT\,",fileLine,flags=re.I)
                                for ele in find_all:
                                    # ele=re.sub(r"\,","",ele,flags=re.I)
                                    replce_string=re.sub(r" REAL\,*"," NUMERIC(6,0)",ele,flags=re.I)
                                    replce_string=re.sub(r" DOUBLE[\s]+PRECISION\,*","FLOAT64",replce_string,flags=re.I)
                                    replce_string=re.sub(r" BOOLEAN\,*","BOOL",replce_string,flags=re.I)
                                    replce_string=re.sub(r" VARBINARY\([\d]+\)","BYTES",replce_string,flags=re.I)
                                    replce_string=re.sub(r" ST_GEOMETRY\([\d]+\)","GEOGRAPHY",replce_string,flags=re.I)
                                    replce_string=re.sub(r" TEXT\,|[\s]+TEXT[\s]+","STRING",replce_string,flags=re.I)
                                    replce_string=re.sub(r"[\s]+","",replce_string,flags=re.I)
                                    if replce_string.lower()=='asnumeric(6,0)' or replce_string.lower()=='asfloat64' or replce_string.lower()=='asbool' or replce_string.lower()=='asbytes' or replce_string.lower()=='asgeography' or replce_string.lower()=='asstring':
                                        pass
                                    else:
                                        input_all_datatype.append(replce_string.lower())
                                        BQ_input_cloumn.append(ele.lower())

                                find_date_time=re.findall(r"[\w]+[\s]+DATE[\s]+|[\w]+[\s]+DATE\,|[\w]+[\s]+TIME[\s]+|[\w]+[\s]+TIME\,|[\w]+[\s]+TIMESTAMP[\s]+|[\w]+[\s]+TIMESTAMP\,",fileLine,flags=re.I)
                                for ele in find_date_time:
                                    replce_string=re.sub(r"\,","",ele,flags=re.I)
                                    replce_string=re.sub(r" TIMESTAMP","DATETIME",replce_string,flags=re.I)
                                    replce_string=re.sub(r"[\s]+","",replce_string,flags=re.I)
                                    if replce_string=="asdate" or replce_string=="anddate" or replce_string=="todate" or replce_string=="elsedate":
                                        pass
                                    else:
                                        input_all_datatype.append(replce_string.lower())
                                        BQ_input_cloumn.append(ele.lower())

                            else:
                                table_obj=table_obj+fileLine

        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r") as f:
                fileLines = f.readlines()
                table_obj=''
                table_counter=0
                dict_key_counter=0
                key_name=''
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if table_counter==0:
                            all_input_cluster_by=[]
                            all_input_partition_by=[]
                            table_obj=''
                            key_name=''
                            all_input_cluster_by=[]
                            all_input_partition_by=[]
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                            if find_table:


                                table_obj=table_obj+fileLine
                                table_counter=1
                                name1=re.findall(r'[\w]+\.[\w]+\.([\w]+)',find_table[0])
                                name2=re.findall(r'TABLE[\s]+([\w]+)[\s]+',find_table[0])
                                if name1:
                                    key_name=name1[0].lower()
                                elif name2:
                                    key_name=name2[0].lower()


                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                find_integer=re.findall(r"[\w]+[\s]+INT64[\s]*|[\w]+[\s]+BYTEINT[\s]*|[\w]+[\s]+SMALLINT[\s]*|[\w]+[\s]+BIGINT[\s]*",fileLine,flags=re.I)
                                for item in find_integer:
                                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                                    if remove_space.lower()=='asint64' or remove_space.lower()=='asbyteint' or remove_space.lower()=='assmallint' or remove_space.lower()=='assbigint':
                                        pass
                                    else:
                                        BQ_output_cloumn.append(item)
                                        output_all_datatype.append(remove_space.lower())

                                find_numeric=re.findall(r"[\w]+[\s]+NUMERIC\([\d]+\,[\d]+\)|[\w]+[\s]+BIGNUMERIC\([\d]+\,[\d]+\)|[\w]+[\s]+FLOAT64|[\w]+[\s]+BOOL|[\w]+[\s]+BYTES|[\w]+[\s]+GEOGRAPHY",fileLine,flags=re.I)
                                for item in find_numeric:
                                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                                    if remove_space=='' or remove_space.lower()=='asfloat64' or remove_space.lower()=='asbool' or remove_space.lower()=='asbytes' or remove_space.lower()=='asgeography':
                                        pass
                                    else:
                                        BQ_output_cloumn.append(item)
                                        output_all_datatype.append(remove_space.lower())

                                find_string=re.findall(r"[\w]+[\s]+STRING\([\d]+\)|[\w]+[\s]+STRING[\s]+|[\w]+[\s]+STRING\,",fileLine,flags=re.I)
                                for item in find_string:
                                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                                    remove_space=re.sub(r"string\,","string",remove_space,flags=re.I)
                                    if remove_space=='' or remove_space.lower()=='asstring':
                                        pass
                                    else:
                                        BQ_output_cloumn.append(item)
                                        output_all_datatype.append(remove_space.lower())

                                find_date_time=re.findall(r"[\w]+[\s]+DATE[\s]+|[\w]+[\s]+DATE\,|[\w]+[\s]+TIME[\s]+|[\w]+[\s]+TIME\,|[\w]+[\s]+DATETIME[\s]+|[\w]+[\s]+DATETIME\,",fileLine,flags=re.I)
                                for item in find_date_time:
                                    remove_space=re.sub(r"[\s]+|,","",item,flags=re.I)
                                    if remove_space=='' or remove_space.lower()=='asdate' or remove_space.lower()=='astime' or remove_space.lower()=='asdatetime' :
                                        pass
                                    else:
                                        BQ_output_cloumn.append(item)
                                        output_all_datatype.append(remove_space.lower())

                            else:
                                table_obj=table_obj+fileLine

        # input_all_datatype.sort()
        # output_all_datatype.sort()
        # BQ_input_cloumn.sort()
        # BQ_output_cloumn.sort()
        if len(input_all_datatype)==0 and len(output_all_datatype) == 0:
            Datatype_result_for_NZ.append("NA")
            log_result.append(Result_NA())

        elif input_all_datatype==output_all_datatype:
            Datatype_result_for_NZ.append("Y")
            log_result.append(Result_Yes(BQ_input_cloumn,BQ_output_cloumn))
        else:
            Datatype_result_for_NZ.append("N")
            log_result.append(Result_No(BQ_input_cloumn,BQ_output_cloumn))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Datatype_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_cluster_and_partition_for_NZ(log_result,filename,directory,Cluster_and_partition_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Cluster By and Partition By Checking"))
    all_input_cluster_by=[]
    all_input_partition_by=[]
    all_output_cluster_by=[]
    all_output_partition_by=[]
    final_input=[]
    final_output=[]
    input_cluster_dict={}
    input_partition_dict={}
    output_cluster_dict={}
    output_partition_dict={}
    final_result='NA'
    final_input_cluster_by=[]
    final_input_partition_by=[]
    final_output_cluster_by=[]
    final_output_partition_by=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                table_obj=''
                table_counter=0
                dict_key_counter=0
                key_name=''
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if table_counter==0:
                            all_input_cluster_by=[]
                            all_input_partition_by=[]
                            table_obj=''
                            key_name=''
                            all_input_cluster_by=[]
                            all_input_partition_by=[]
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]+[\w]+\.[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                            if find_table:


                                table_obj=table_obj+fileLine
                                table_counter=1
                                name1=re.findall(r'[\w]+\.[\w]+\.([\w]+)',find_table[0])
                                name2=re.findall(r'TABLE[\s]+([\w]+)[\s]+',find_table[0])
                                if name1:
                                    key_name=name1[0].lower()
                                elif name2:
                                    key_name=name2[0].lower()


                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                dist1 = re.findall(r'DISTRIBUTE[\s]+ON[\s]+\(*(.*)\)*',table_obj,flags=re.I|re.DOTALL)
                                if dist1:
                                    dist1=re.sub(r"ORGANIZE[\s]+ON\(*(.*)\)*|;(.*)","",dist1[0],flags=re.I|re.DOTALL)
                                    # final_input.append("DISTRIBUTED ON "+dist1)
                                    dist1=re.split("[\s]+|\,|\)|\(|;",dist1,flags=re.I|re.DOTALL)
                                    while("" in dist1):
                                        dist1.remove("")

                                orga = re.findall(r'Organize[\s]+ON[\s]+\(*(.*)\)*',table_obj, flags=re.I)
                                if orga:
                                    orga=re.sub(r"DISTRIBUTE[\s]+ON\(*(.*)\)*|;(.*)","",orga[0],flags=re.I|re.DOTALL)
                                    # final_input.append("Organize ON "+dist1)
                                    orga=re.split("[\s]+|\,|\)|\(|;",orga,flags=re.I|re.DOTALL)
                                    while("" in orga):
                                        orga.remove("")
                                all_column=dist1+orga

                                for ele in all_column:
                                    ele=ele.lower()
                                    if ele == 'random':
                                        pk_random = re.findall(r'[\w]+[\s]+[\w]+[\S]*[\s]*[\S]*[\s]*[\S]*[\s]+primary[\s]+key',table_obj, flags=re.IGNORECASE)
                                        #print(pk_random)
                                        fk_random = re.findall(r'foreign[\s]+key[\s]+[\S]+',table_obj, flags=re.IGNORECASE)
                                        if pk_random:
                                            pk_random=re.split(r'[\s]+',str(pk_random[0]))
                                            all_input_cluster_by.append(pk_random[0])
                                        if fk_random:
                                            fk_random=re.split(r'foreign[\s]+key[\s]+\(|\)',str(fk_random[0]),flags=re.IGNORECASE)
                                            all_input_partition_by.append(fk_random[1])

                                    else:
                                        val=list(re.findall(r''+ele+'[\s]+[\w]+',table_obj, flags=re.IGNORECASE))
                                        for i in val:
                                            i.lower()
                                            a = re.split(r'[\s]+',i)
                                            if a[1] == 'character' or a[1] == 'integer' or a[1] == 'string' or a[1] == 'nvarchar' or a[1] == 'bigint' or a[1] == 'smallint':
                                                all_input_cluster_by.append(a[0])
                                            elif a[1] == 'date' or a[1] == 'datetime':
                                                all_input_partition_by.append(a[0])
                                            elif a[1] == 'timestamp':
                                                all_input_partition_by.append("date("+a[0]+")")

                                all_input_cluster_by.sort()
                                all_input_partition_by.sort()

                                input_cluster_dict[key_name]=all_input_cluster_by
                                input_partition_dict[key_name]=all_input_partition_by


                            else:
                                table_obj=table_obj+fileLine



        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                table_obj=''
                table_counter=0
                dict_key_counter=0
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if table_counter==0:
                            table_obj=''
                            key_name=''
                            all_output_cluster_by=[]
                            all_output_partition_by=[]
                            find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                            if find_table:
                                table_obj=table_obj+fileLine
                                table_counter=1
                                name1=re.findall(r'[\w]+\.([\w]+)',find_table[0])
                                name2=re.findall(r'EXISTS[\s]+([\w]+)[\s]+|TABLE[\s]+([\w]+)[\s]+',find_table[0])
                                if name1:
                                    key_name=name1[0].lower()
                                elif name2:
                                    for a in name2:
                                        for b in a:
                                            if b=='' or b=='IF' or b=='if':
                                                pass
                                            else:
                                                key_name=b.lower()
                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                dist1 = re.findall(r'PARTITION[\s]+BY[\s]+\(*(.*)\)*',table_obj,flags=re.I|re.DOTALL)
                                if dist1:
                                    dist1=re.sub(r"CLUSTER[\s]+BY\(*(.*)\)*|;(.*)","",dist1[0].lower(),flags=re.I|re.DOTALL)
                                    ts_col=re.findall("DATE\([\w]+\)",dist1,flags=re.I|re.DOTALL)
                                    # final_output.append("PARTITION BY "+dist1)
                                    all_output_partition_by=re.split("DATE\([\w]+\)|[\s]+|\,|\)|\(|;",dist1,flags=re.I|re.DOTALL)
                                    while("" in all_output_partition_by):
                                        all_output_partition_by.remove("")
                                    if ts_col:
                                        for ele in ts_col:
                                            all_output_partition_by.append(ele.lower())

                                orga = re.findall(r'CLUSTER[\s]+BY[\s]+\(*(.*)\)*',table_obj, flags=re.I)
                                if orga:
                                    orga=re.sub(r"PARTITION[\s]+BY\(*(.*)\)|;(.*)","",orga[0].lower(),flags=re.I|re.DOTALL)
                                    # final_output.append("CLUSTER BY "+orga)
                                    all_output_cluster_by=re.split("[\s]+|\,|\)|\(|;",orga,flags=re.I|re.DOTALL)
                                    while("" in all_output_cluster_by):
                                        all_output_cluster_by.remove("")

                                all_output_cluster_by.sort()
                                all_output_partition_by.sort()

                                output_cluster_dict[key_name]=all_output_cluster_by
                                output_partition_dict[key_name]=all_output_partition_by

                            else:
                                table_obj=table_obj+fileLine


        all_input_cluster_by.sort()
        all_input_partition_by.sort()
        all_output_cluster_by.sort()
        all_output_partition_by.sort()

        # print("Cluster Input",all_input_cluster_by)
        # print("Partition Input",all_input_partition_by)

        # print("Cluster Output",all_output_cluster_by)
        # print("Partition Output",all_output_partition_by)
        for key in input_cluster_dict:
            for ele in input_cluster_dict[key]:
                final_input_cluster_by.append(ele)
            if key in output_cluster_dict:
                for ele in output_cluster_dict[key]:
                    final_output_cluster_by.append(ele)

        for key in input_partition_dict:
            for ele in input_partition_dict[key]:
                final_input_cluster_by.append(ele)
            if key in output_cluster_dict:
                for ele in output_partition_dict[key]:
                    final_output_cluster_by.append(ele)


        for key in input_partition_dict:
            if key in output_cluster_dict:
                if len(output_partition_dict[key]) >4:
                    final_result='N'
                    break

                else:
                    if len(input_cluster_dict[key])==0 and len(input_partition_dict[key]) == 0 and len(output_cluster_dict[key])==0 and len(output_partition_dict[key]) == 0:
                        pass

                    elif len(input_cluster_dict[key]) > 4 and len(output_cluster_dict[key]) <= 4:
                        if any(element in output_cluster_dict[key] for element in input_cluster_dict[key]):
                            final_result='Y'
                        else:
                            final_result='N'
                            break

                    elif input_cluster_dict[key]==output_cluster_dict[key] and input_partition_dict[key]==output_partition_dict[key]:
                        final_result='Y'

                    else:
                        final_result='N'
                        break
            else:
                final_result='N'
                break


        if final_result=='N':
            Cluster_and_partition_result_for_NZ.append("N")
            log_result.append(Result_No(final_input_cluster_by,final_output_cluster_by))
        elif final_result=='NA':
            Cluster_and_partition_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif final_result=='Y':
            Cluster_and_partition_result_for_NZ.append("Y")
            log_result.append(Result_Yes(final_input_cluster_by,final_output_cluster_by))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Cluster_and_partition_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_comments_for_NZ(log_result,filename,directory,Check_comment_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Comments Checking"))
    input_log=[]
    input_comment_lst=[]
    output_log=[]
    comment=0
    output_comment_lst=[]
    try:
        if (filename,directory) in inputFile:
            SQL_query=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    if SQL_query==0:
                        cmt=re.findall(r'^[\s]*--|^[\s]*\#',fileLine)
                        if "/*" in fileLine:
                            input_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                input_comment_lst.append(fileLine.strip().upper())

                            if '*/' in fileLine:
                                comment=0
                                pass
                            else:
                                comment=1
                                pass
                        elif comment==1:
                            input_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                input_comment_lst.append(fileLine.strip().upper())

                            if "*/" in fileLine:
                                #print(fileLine)
                                comment=0
                                pass
                            else:
                                pass
                        elif cmt:
                            input_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                input_comment_lst.append(fileLine.strip().upper())
                        else:
                            input_sql_statements=re.findall(r"CALL[\s]+[\w]+\.[\w]+\.[\w]+|CREATE[\s]+TABLE|WITH[\s]+CTE[\s]+|CREATE[\s]+TEMP[\s]+TABLE|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+|CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|MERGE[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE|CREATE[\s]+PROCEDURE[\s]+|CREATE[\s]+FUNCTION[\s]+",fileLine,flags=re.I)
                            if input_sql_statements:
                                if ";" in fileLine:
                                    pass
                                else:
                                    SQL_query=1
                    if SQL_query==1:
                        if ";" in fileLine:
                            SQL_query=0




        if (filename,directory) in outputFile:
            SQL_query=0
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    if SQL_query==0:
                        cmt=re.findall(r'^[\s]*--|^[\s]*\#',fileLine)
                        if "/*" in fileLine:
                            output_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                fileLine=re.sub(r"dmtnethealthcare\.","",fileLine,flags=re.I)
                                output_comment_lst.append(fileLine.strip().upper())

                            if '*/' in fileLine:
                                comment=0
                                pass
                            else:
                                comment=1
                                pass
                        elif comment==1:
                            output_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                fileLine=re.sub(r"dmtnethealthcare\.","",fileLine,flags=re.I)
                                output_comment_lst.append(fileLine.strip().upper())

                            if "*/" in fileLine:
                                #print(fileLine)
                                comment=0
                                pass
                            else:
                                pass
                        elif cmt:
                            output_log.append(fileLine)
                            fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                            if fileLine=='':
                                pass
                            else:
                                fileLine=re.sub(r"dmtnethealthcare\.","",fileLine,flags=re.I)
                                output_comment_lst.append(fileLine.strip().upper())
                        else:
                            output_sql_statements=re.findall(r"CALL[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|TRUNCATE[\s]+TABLE[\s]|CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE",fileLine,flags=re.I)
                            if output_sql_statements:
                                if ";" in fileLine:
                                    pass
                                else:
                                    SQL_query=1
                    if SQL_query==1:
                        if ";" in fileLine:
                            SQL_query=0
        # print(input_comment_lst)
        # print(output_comment_lst)
        if len(input_comment_lst)==0 and len(output_comment_lst)==0:
            Check_comment_result_for_NZ.append("NA")
            log_result.append(Result_NA())

        elif input_comment_lst==output_comment_lst:
            Check_comment_result_for_NZ.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            Check_comment_result_for_NZ.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Check_comment_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def no_of_statements_for_NZ(log_result,filename,directory,no_of_statements_result_for_NZ,fun_project_id_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Number of Statements in Script"))
    with_Append_project_id_Final_input_sql_statements=[]
    without_Append_project_id_Final_input_sql_statements=[]
    with_Append_project_id_Final_output_sql_statements=[]
    without_Append_project_id_Final_output_sql_statements=[]
    all_input_file_statements=[]
    all_output_file_statements=[]
    flag=0
    try:
        # if 1==1:
        with open (os.path.join(inputFolderPath, filename), 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"[\s]*--",fileLine)
                if "/*" in fileLine:
                    if '*/' in fileLine:
                        comment=0
                        pass
                    else:
                        comment=1
                        pass
                elif comment==1:
                    if "*/" in fileLine:
                        comment=0
                        pass
                    else:
                        pass
                elif cmt :
                    pass
                elif "--" in fileLine:
                    fileLine=fileLine.split("--")
                    obj=obj+fileLine[0]
                else:
                    obj=obj+fileLine

            obj=re.sub(r'\.[\w]+\.','.',obj)
            file_obj1=obj.split(";")
            file_obj=[]
            for ele in file_obj1:
                find_word=re.findall(r'[\w]+',ele)
                if find_word:
                    file_obj.append(ele.strip())
            for element in file_obj:
                find_update=re.findall(r"UPDATE[\s]*[\w]+[\s]*FROM[\s]*[\w]+\.[\w]+",element,flags=re.I|re.DOTALL)
                if find_update:
                    for item in find_update:
                        find_table_and_schema=re.findall(r"[\w]+\.[\w]+",item,flags=re.I)
                        replace_alias=re.sub("UPDATE[\s]+[\w]+[\s]+","UPDATE "+find_table_and_schema[0]+"\n",item,flags=re.I)
                        element=element.replace(item+" ",replace_alias+" ")
                find_update=re.findall(r"UPDATE[\s]*[\w]+[\s]*FROM[\s]*[\w]+[\s]+",element,flags=re.I|re.DOTALL)
                if find_update:
                    for item in find_update:
                        find_table_and_schema=re.findall(r"FROM[\s]+[\w]+[\s]+",item,flags=re.I)
                        remove_space=re.sub(r"[\s]+"," ",find_table_and_schema[0])
                        find_table_and_schema=remove_space.split()
                        replace_alias=re.sub("UPDATE[\s]+[\w]+[\s]+","UPDATE "+find_table_and_schema[1]+"\n",item,flags=re.I)
                        element=element.replace(item,replace_alias+" ")
                find_update=re.findall(r"UPD[\s]*[\w]+[\s]*FROM[\s]*[\w]+\.[\w]+",element,flags=re.I|re.DOTALL)
                if find_update:
                    for item in find_update:
                        find_table_and_schema=re.findall(r"[\w]+\.[\w]+",item,flags=re.I)
                        replace_alias=re.sub("UPD[\s]+[\w]+[\s]+","UPD "+find_table_and_schema[0]+"\n",item,flags=re.I)
                        element=element.replace(item+" ",replace_alias+" ")
                find_update=re.findall(r"UPD[\s]*[\w]+[\s]*FROM[\s]*[\w]+[\s]+",element,flags=re.I|re.DOTALL)
                if find_update:
                    for item in find_update:
                        find_table_and_schema=re.findall(r"FROM[\s]+[\w]+[\s]+",item,flags=re.I)
                        remove_space=re.sub(r"[\s]+"," ",find_table_and_schema[0])
                        find_table_and_schema=remove_space.split()
                        replace_alias=re.sub("UPD[\s]+[\w]+[\s]+","UPD "+find_table_and_schema[1]+"\n",item,flags=re.I)
                        element=element.replace(item+" ",replace_alias+" ")


                input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*"
                                                r"|^WITH[\s]+[\w]+[\s]+|REPLACE[\s]+MACRO[\s]*[\w]+\.[\w]+|CREATE[\s]+VOLATILE[\s]+"
                                                r"TABLE[\s]*[\w]+[\s]*|REPLACE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|CREATE[\s]+VIEW[\s]*[\w]+\.[\w]+"
                                                r"|REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+[\s]+|^INSERT[\s]+INTO[\s]+[\w]+\(|^INS[\s]+[\w]+\.[\w]+|^INS[\s]+[\w]+[\s]+|^INS[\s]+[\w]+\(|"
                                                r"^UPDATE[\s]*[\w]+\.[\w]+|^UPDATE[\s]+[\w]+[\s]+|^UPD[\s]*[\w]+\.[\w]+|^UPD[\s]+[\w]+[\s]+"
                                                r"|DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+|"
                                                r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]*\;|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]*[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+[\s]*\;|"
                                                r"MERGE[\s]+[\w]+\.[\w]+|CREATE[\s]+SET[\s]+TABLE[\s]+[\w]+\.[\w]+|"
                                                r"CREATE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|"
                                                r"CREATE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|CALL[\s]+[\w]+\.[\w]+|^SELECT",element,flags=re.I)
                if fun_project_id_for_NZ == "Y":
                    # obj=re.sub("[\s]+SELECT","SELECT ",element,flags=re.I|re.DOTALL)
                    # if obj.upper().startswith("SELECT "):
                    #     find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                    #     if find_select:
                    #         find_select=re.sub(r"[\s]+","",find_select[0])
                    #         with_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                    #     all_input_file_statements.append(obj.upper())
                    for item in input_sql_statements:
                        remove_space=re.sub(r"[\s]+"," ",item)
                        if item.upper()=="SELECT":
                            all_input_file_statements.append(element)
                        else:
                            all_input_file_statements.append(remove_space.upper())
                        item=re.sub(r"\n","",item)
                        item=re.sub(r"CREATE[\s]+SET[\s]+TABLE[\s]+|CREATE[\s]+MULTISET[\s]+TABLE[\s]+|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"INSERT[\s]+INTO[\s]+|^INS[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"^SEL[\s]+","SELECT ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                        item=re.sub(r"REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+VIEW[\s]+","CREATE VIEW IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                        #item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                        if re.findall(r"drop[\s]+table[\s]*[\w]+\.[\w]+",item,flags=re.I):
                            item=re.sub(r"drop[\s]+table[\s]+","DROP TABLE IF EXISTS `"+Append_project_id+".",item,flags=re.I)
                        else:
                            item=re.sub(r"drop[\s]+table[\s]+","DROP TABLE IF EXISTS ",item,flags=re.I)
                        find_delete=re.findall(r"DELETE[\s]+FROM|DEL[\s]+FROM",item,flags=re.I)
                        if find_delete:
                            if "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                            if "WHERE " in obj.upper() and re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+[\s]*\;|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DEL FROM `"+Append_project_id+".",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+[\s]*\;|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)

                        if re.findall(r"INSERT[\s]+INTO[\s]*[\w]+\.[\w]+",item,flags=re.I):
                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        else:
                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                        if re.findall(r"UPDATE[\s]*[\w]+\.[\w]+|UPD[\s]*[\w]+\.[\w]+",item,flags=re.I):
                            item=re.sub(r"UPDATE|UPD ","UPDATE `"+Append_project_id+".",item,flags=re.I)
                        else:
                            item=re.sub(r"UPDATE|UPD ","UPDATE ",item,flags=re.I)
                        # item=re.sub(r"WITH[\s]+","WITH ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"[\s]+|\;|\(|\`","",item)
                        item=re.sub(r"\.[\s]+",".",item)
                        with_Append_project_id_Final_input_sql_statements.append(item.upper())
                else:
                    # obj=re.sub("[\s]+SELECT","SELECT ",element,flags=re.I)
                    # if obj.upper().startswith("SELECT "):
                    #     print("Yy")
                    #     find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I)
                    #     if find_select:
                    #         find_select=re.sub(r"[\s]+","",find_select[0])
                    #         without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                    #         all_input_file_statements.append(obj.upper())

                    for item in input_sql_statements:
                        remove_space=re.sub(r"[\s]+"," ",item)
                        if item.upper()=="SELECT":
                            all_input_file_statements.append(element)
                        else:
                            all_input_file_statements.append(remove_space.upper())
                        item=re.sub(r"\n","",item)
                        item=re.sub(r"CREATE[\s]+SET[\s]+TABLE[\s]+|CREATE[\s]+MULTISET[\s]+TABLE[\s]+|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"INSERT[\s]+INTO[\s]+|^INS[\s]+","INSERT INTO ",item,flags=re.I)
                        item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                        item=re.sub(r"^SEL[\s]+","SELECT ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                        item=re.sub(r"REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+VIEW[\s]+","CREATE VIEW IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS ",item,flags=re.I)
                        find_delete=re.findall(r"DELETE[\s]+FROM|DEL[\s]+FROM",item,flags=re.I)
                        if find_delete:
                            if "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                            if "WHERE " in obj.upper() and re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+[\s]*\;|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DEL FROM ",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+[\s]*\;|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                        item=re.sub(r"UPDATE|UPD ","UPDATE ",item,flags=re.I)
                        # item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                        item=re.sub(r"[\s]+|\;|\(|\`","",item)
                        item=re.sub(r"\.[\s]+",".",item)
                        without_Append_project_id_Final_input_sql_statements.append(item.upper())

        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"[\s]*--",fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt :
                        pass
                    elif "--" in fileLine:
                        fileLine=fileLine.split("--")
                        obj=obj+fileLine[0]
                    else:
                        obj=obj+fileLine

                file_obj1=obj.split(";")
                file_obj=[]
                for ele in file_obj1:
                    find_word=re.findall(r'[\w]+',ele)
                    if find_word:
                        file_obj.append(ele.strip())
                for element in file_obj:
                    if fun_project_id_for_NZ == "Y":
                        output_sql_statements=re.findall(r"^[\s]*WITH[\s]+[\w]+[\s]+|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+|^[\s]*SELECT[\s]+",element,flags=re.I)
                        for item in output_sql_statements:
                            remove_space=re.sub(r"[\s]+","",item)
                            # remove_space=re.sub(r"[\s]+"," ",item)
                            if remove_space.upper()=="SELECT":
                                all_output_file_statements.append(element)
                            else:
                                all_output_file_statements.append(item.upper())
                            item=re.sub(r"\`","",item)
                            item=re.sub(r"[\s]+","",item)
                            with_Append_project_id_Final_output_sql_statements.append(item.upper())
                    else:
                        output_sql_statements=re.findall(r"^[\s]*WITH[\s]+[\w]+[\s]+|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+|^[\s]*SELECT[\s]+",element,flags=re.I)
                        for item in output_sql_statements:
                            remove_space=re.sub(r"[\s]+","",item)
                            # remove_space=re.sub(r"[\s]+"," ",item)
                            if remove_space.upper()=="SELECT":
                                all_output_file_statements.append(element)
                            else:
                                all_output_file_statements.append(item.upper())

                            item=re.sub(r"[\s]+","",item)
                            without_Append_project_id_Final_output_sql_statements.append(item.upper())



        with_Append_project_id_Final_input_sql_statements.sort()
        with_Append_project_id_Final_output_sql_statements.sort()
        without_Append_project_id_Final_input_sql_statements.sort()
        without_Append_project_id_Final_output_sql_statements.sort()
        # print(without_Append_project_id_Final_input_sql_statements)
        # print(without_Append_project_id_Final_output_sql_statements)
        # print(with_Append_project_id_Final_input_sql_statements)
        # print(with_Append_project_id_Final_output_sql_statements)

        if fun_project_id_for_NZ=="Y":
            if len(with_Append_project_id_Final_input_sql_statements)==0 and len(with_Append_project_id_Final_output_sql_statements)==0 :
                no_of_statements_result_for_NZ.append("NA")
                log_result.append(Result_NA())
            elif len(with_Append_project_id_Final_input_sql_statements)>0:
                if with_Append_project_id_Final_input_sql_statements==with_Append_project_id_Final_output_sql_statements:
                    no_of_statements_result_for_NZ.append("Y")
                    log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
                else:
                    no_of_statements_result_for_NZ.append("N")
                    log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
            else:
                no_of_statements_result_for_NZ.append("N")
                log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
        else:
            if len(without_Append_project_id_Final_input_sql_statements) ==0 and len(without_Append_project_id_Final_output_sql_statements)==0:
                no_of_statements_result_for_NZ.append("NA")
                log_result.append(Result_NA())
            elif len(without_Append_project_id_Final_input_sql_statements)>0:
                if without_Append_project_id_Final_input_sql_statements==without_Append_project_id_Final_output_sql_statements:
                    no_of_statements_result_for_NZ.append("Y")
                    log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
                else:
                    no_of_statements_result_for_NZ.append("N")
                    log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
            else:
                no_of_statements_result_for_NZ.append("N")
                log_result.append(Result_No(all_input_file_statements,all_output_file_statements))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        no_of_statements_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_cross_join_for_NZ(log_result,filename,directory,cross_join_result_for_NZ,fun_project_id_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Verifying Join Conditions"))
    inputCrossJoin=[]
    outputCrossJoin=[]
    input_cross_join_list=[]
    output_cross_join_list=[]
    input_cross_join_log=[]
    output_cross_join_log=[]
    from_to_where=''
    final_result="NA"
    try:
        # if 1==1:
        if (filename,directory) in inputFile:
            file_obj=''
            comment=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine
                input_cross=re.findall(r'CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]*[\s]+WHERE|CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+\.[\w]+[\s]*|CROSS[\s]+JOIN[\s]+[\w]+[\s]*|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+\.[\w]+[\s]*|LEFT[\s]+JOIN[\s]+[\w]+[\s]*|LEFT[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+\.[\w]+[\s]*|RIGHT[\s]+JOIN[\s]+[\w]+[\s]*|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+\.[\w]+[\s]*|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]*|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+\.[\w]+[\s]*|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]*|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT',file_obj,flags=re.I|re.DOTALL)
                for ele in input_cross:
                    if ele.upper().endswith("WHERE"):
                        pass
                    else:
                        remove_space=re.sub(r"[\s]+","",ele)
                        inputCrossJoin.append(remove_space.lower())
                        input_cross_join_log.append(ele)

                input_from_to_where=re.findall(r'FROM(.*)[WHERE]*\;*',file_obj,flags=re.I|re.DOTALL)
                if input_from_to_where:
                    input_from_to_where[0]=re.sub(r'\.[\w]+\.','.',input_from_to_where[0])
                #print(input_from_to_where)
                for ele in input_from_to_where:
                    input_cross_join_log.append(ele)
                    from_to_where=ele
                if input_from_to_where:
                    remove_space=re.split("\,|\n|\t|[\s]+",input_from_to_where[0])
                    for item in remove_space:
                        if item=='':
                            pass
                        else:
                            input_cross_join_list.append(item.upper())

        if (filename,directory) in outputFile:
            file_obj=''
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                output_cross=re.findall(r'CROSS[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT',file_obj,flags=re.I|re.DOTALL)
                for ele in output_cross:
                    remove_space=re.sub(r"[\s]+","",ele)
                    if fun_project_id_for_NZ == 'Y':
                        remove_space=re.sub(Append_project_id+".|\`","",remove_space,flags=re.I)
                    outputCrossJoin.append(remove_space.lower())
                    output_cross_join_log.append(ele)
                output_comma_cross=re.findall(r"INNER[\s]+JOIN[\s]+\`*"+Append_project_id+"\.[\w]+\.[\w]+\`*[\s]+|INNER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|INNER[\s]+JOIN[\s]+[\w]+[\s]+|INNER[\s]+JOIN[\s]+\([\s]*SELECT[\s]+",file_obj,flags=re.I|re.DOTALL)
                str1=""
                if len(output_comma_cross)==0:
                    if from_to_where=='':
                        pass
                    else:
                        input_cross_join_log.remove(from_to_where)

                for ele_innerJoin in output_comma_cross:
                    str1=str1+" "+ele_innerJoin
                if str1=="":
                    pass
                else:
                    output_cross_join_log.append(str1)
                    output_comma_cross=' '.join(map(str, output_comma_cross)).upper().split('INNER JOIN')

                for ele_innerJoin in output_comma_cross:
                    remove_space=re.sub("[\s]+","",ele_innerJoin,flags=re.I)
                    if fun_project_id_for_NZ == 'Y':
                        remove_space=re.sub(Append_project_id+".","",remove_space,flags=re.I)

                    if remove_space=='':
                        pass
                    else:
                        output_cross_join_list.append(remove_space.upper())
        if len(output_cross_join_list)==0:
            final_result="NA"
        else:
            for item in output_cross_join_list:
                # print(item)
                item=re.sub("\(","",item)
                if item in input_cross_join_list:

                    final_result="true"
                else:
                    final_result="false"
                    break

        # print(inputCrossJoin)
        # print(outputCrossJoin)
        # print(len(inputCrossJoin))
        # print(len(outputCrossJoin))
        # print(input_cross_join_list)
        # print(output_cross_join_list)

        if len(inputCrossJoin)==0 and len(outputCrossJoin)==0 and final_result=="NA":
            cross_join_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif len(inputCrossJoin)>0 and len(outputCrossJoin)>0 or final_result!="false":
            if len(inputCrossJoin) == len(outputCrossJoin) and final_result!="false":
                cross_join_result_for_NZ.append("Y")
                log_result.append(Result_Yes(input_cross_join_log,output_cross_join_log))
            else:
                cross_join_result_for_NZ.append("N")
                log_result.append(Result_No(input_cross_join_log,output_cross_join_log))
        elif len(inputCrossJoin)==0 and len(outputCrossJoin)==0 or final_result=="true":
            cross_join_result_for_NZ.append("Y")
            log_result.append(Result_Yes(input_cross_join_log,output_cross_join_log))


        else:
            cross_join_result_for_NZ.append("N")
            log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        cross_join_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_not_like_for_NZ(log_result,filename,directory,not_like_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Not Like"))
    input_not_like=[]
    output_not_like=[]
    input_log=[]
    output_log=[]
    try:
        # if 1==1:
        if (filename,directory) in inputFile:
            file_obj=''
            comment=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine
                find_not_like=re.findall(r"\!\~\~LIKE\_ESCAPE\(\'\%*[\w]+\%*\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\,[\s]*\'\\\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\)|\!\~\~LIKE\_ESCAPE\(\'\%*\*+\%*\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\,[\s]*\'\\\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\)",file_obj,flags=re.I|re.DOTALL)
                for ele in find_not_like:
                    input_log.append(ele)
                    ele=re.findall(r"\!\~\~LIKE\_ESCAPE\((\'\%*[\w]+\%*\')[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\,[\s]*\'\\\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\)|\!\~\~LIKE\_ESCAPE\((\'\%*\*+\%*\')[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\,[\s]*\'\\\'[\s]*\:\:[\s]*\"VARCHAR\"[\s]*\)",ele,flags=re.I|re.DOTALL)[0]
                    for item in ele:
                        if item=="":
                            pass
                        else:
                            input_not_like.append("NOTLIKE"+item)

        if (filename,directory) in outputFile:
            file_obj=''
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                find_not_like=re.findall(r"NOT[\s]+LIKE[\s]+\'\%*[\w]+\%*\'|NOT[\s]+LIKE[\s]+\'\%*\*+\%*\'",file_obj,flags=re.I|re.DOTALL)
                for ele in find_not_like:
                    output_log.append(ele)
                    ele=re.findall(r"NOT[\s]+LIKE[\s]+(\'\%*[\w]+\%*\')|NOT[\s]+LIKE[\s]+(\'\%*\*+\%*\')",ele,flags=re.I|re.DOTALL)[0]
                    for item in ele:
                        if item=='':
                            pass
                        else:
                            output_not_like.append("NOTLIKE"+item)

        if len(input_not_like)==0 and len(output_not_like)==0 :
            not_like_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif input_not_like==output_not_like:
            not_like_result_for_NZ.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            not_like_result_for_NZ.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        not_like_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_now_for_NZ(log_result,filename,directory,now_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check now()"))
    input_now=[]
    output_now=[]
    input_log=[]
    output_log=[]
    try:
        # if 1==1:
        if (filename,directory) in inputFile:
            file_obj=''
            comment=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine
                find_input_now=re.findall(r"DATE[\s]*\([\s]*\'now\(0\)\'[\s]*\:\:[\s]*\"VARCHAR\"\)|DATE[\s]*\([\s]*\'now\'[\s]*\:\:[\s]*\"VARCHAR\"\)",file_obj,flags=re.I|re.DOTALL)

        if (filename,directory) in outputFile:
            file_obj=''
            comment=0
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                # find_output_now=re.findall(r"CAST[\s]*\([\s]*CURRENT_DATETIME[\s]+AS[\s]+DATE[\s]*\)",file_obj,flags=re.I|re.DOTALL)
                find_output_now=re.findall(r"CURRENT_DATETIME[\W]",file_obj,flags=re.I|re.DOTALL)
                # print(find_output_now)
        if len(find_input_now)==0 and len(find_output_now)==0 :
            now_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif len(find_input_now)==len(find_output_now):
            now_result_for_NZ.append("Y")
            log_result.append(Result_Yes(find_input_now,find_output_now))
        else:
            now_result_for_NZ.append("N")
            log_result.append(Result_No(find_input_now,find_output_now))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        now_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_default_schema_for_NZ(log_result,filename,directory,default_schema_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Default Schema(dm_schema)"))
    try:
        if (filename,directory) in outputFile:
            file_obj=''
            comment=0
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                find_default_schema=re.findall(r"dm_schema\.",file_obj,flags=re.I|re.DOTALL)
                # print(find_output_now)
        if len(find_default_schema)==0:
            default_schema_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif len(find_default_schema) >0:
            default_schema_result_for_NZ.append("N")
            log_result.append(Result_No([],find_default_schema))

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        default_schema_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_Case_when_for_NZ(log_result,filename,directory,Case_when_result_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Case When"))

    input_not_like=[]
    output_not_like=[]
    input_log=[]
    output_log=[]
    try:
        # if 1==1:
        if (filename,directory) in inputFile:

            file_obj=''
            comment=0
            insert_start=0
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()

                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                find_select_Case_when=re.findall(r"FROM[\S]*(.*)\;",file_obj,flags=re.I|re.DOTALL)
                for i in find_select_Case_when:
                    find_select_Case_whenin=re.findall(r"CASE[\s]+WHEN",i,flags=re.I|re.DOTALL)
                    for i in find_select_Case_whenin:
                        i=re.sub(r'[\s]+',' ',i,flags=re.DOTALL)
                        input_not_like.append(i)
                        input_log.append(i)


                find_select_Case_when=re.findall(r"SELECT[\S]*(.*)FROM",file_obj,flags=re.I|re.DOTALL)
                for i in find_select_Case_when:
                    find_select_Case_whenin=re.findall(r"THEN",i,flags=re.I|re.DOTALL)
                    for i in find_select_Case_whenin:
                        input_not_like.append(i)
                        input_log.append(i)


        if (filename,directory) in outputFile:
            file_obj=''
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                fileLines1 = f.read()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                find_select_Case_when=re.findall(r"FROM[\S]*(.*)\;",file_obj,flags=re.I|re.DOTALL)
                for i in find_select_Case_when:
                    find_select_Case_whenin=re.findall(r"CASE[\s]+WHEN",i,flags=re.I|re.DOTALL)
                    for i in find_select_Case_whenin:
                        i=re.sub(r'[\s]+',' ',i,flags=re.DOTALL)
                        output_not_like.append(i)
                        output_log.append(i)


                find_select_Case_when=re.findall(r"SELECT[\S]*(.*)FROM",file_obj,flags=re.I|re.DOTALL)
                for i in find_select_Case_when:
                    find_select_Case_whenop=re.findall(r"THEN",i,flags=re.I|re.DOTALL)
                    for i in find_select_Case_whenop:
                        output_not_like.append(i)
                        output_log.append(i)
        # print(input_not_like,output_not_like)
        if len(input_not_like)==0 and len(output_not_like)==0 :
            Case_when_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif input_not_like==output_not_like :
            Case_when_result_for_NZ.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            Case_when_result_for_NZ.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Case_when_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_lower_case_for_NZ(log_result,filename,directory,lower_case_result_for_NZ,fun_project_id_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Lower Case"))
    try:

        if (filename,directory) in outputFile:
            file_obj=''
            final_result_lst=[]
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                fileLines1 = f.read()
                final_result="Y"
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine

                if fun_project_id_for_NZ == "Y":
                    file_obj=file_obj.replace(Append_project_id+".","")

                find_lower_case=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
                # print(find_lower_case)
                for ele in find_lower_case:
                    split_name=re.split("[\s]+",ele)
                    split_name=split_name[1].split('.')
                    for i in split_name:
                        if i.islower():
                            pass
                        else:
                            final_result="N"
                            break
                if fun_project_id_for_NZ == "Y":
                    for ele in find_lower_case:
                        final_result_lst.append(re.sub(r'[\s]+\`'," `"+Append_project_id+".",ele))
                else:
                    final_result_lst=find_lower_case
        if len(find_lower_case)==0:
            lower_case_result_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif final_result=="N":
            lower_case_result_for_NZ.append("N")
            log_result.append(Output_Result_No(final_result_lst))
        elif final_result=="Y":
            lower_case_result_for_NZ.append("Y")
            log_result.append(Output_Result_Yes(final_result_lst))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        lower_case_result_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_alice_in_lower_case_for_NZ(log_result,filename,directory,alice_in_lower_case_for_NZ):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Alice In Lower Case"))
    try:

        if (filename,directory) in outputFile:
            file_obj=''
            final_result_lst=[]
            keyword_lst=["ALL","AND","ANY","ARRAY","AS","ASC","ASSERT_ROWS_MODIFIED","AT","BETWEEN","BY","CASE","CAST","COLLATE","CONTAINS","CREATE","CROSS","CUBE","CURRENT","DEFAULT","DEFINE","DESC","DISTINCT","ELSE","END","ENUM","ESCAPE","EXCEPT","EXCLUDE","EXISTS","EXTRACT","FALSE","FETCH","FOLLOWING","FOR","FROM","FULL","GROUP","GROUPING","GROUPS","HASH","HAVING","IF","IGNORE","IN","INNER","INTERSECT","INTERVAL","INTO","IS","JOIN","LATERAL","LEFT","LIKE","LIMIT","LOOKUP","MERGE","NATURAL","NEW","NO","NOT","NULL","NULLS","OF","ON","OR","ORDER","OUTER","OVER","PARTITION","PRECEDING","PROTO","QUALIFY","RANGE","RECURSIVE","RESPECT","RIGHT","ROLLUP","ROWS","SELECT","SET","SOME","STRUCT","TABLESAMPLE","THEN","TO","TREAT","TRUE","UNBOUNDED","UNION","UNNEST","USING","WHEN","WHERE","WINDOW","WITH","WITHIN"]
            with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.readlines()
                fileLines1 = f.read()
                final_result="Y"
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]
                    else:
                        file_obj=file_obj+fileLine


                find_alice=re.findall(r'AS[\s]+([\w]+)',file_obj,flags=re.I)
                # print(find_alice)
                for ele in find_alice:
                    if ele.islower() or ele in keyword_lst:
                        pass
                    else:
                        final_result="N"
                        break

        if len(find_alice)==0:
            alice_in_lower_case_for_NZ.append("NA")
            log_result.append(Result_NA())
        elif final_result=="N":
            alice_in_lower_case_for_NZ.append("N")
            log_result.append(Output_Result_No(find_alice))
        elif final_result=="Y":
            alice_in_lower_case_for_NZ.append("Y")
            log_result.append(Output_Result_Yes(find_alice))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        alice_in_lower_case_for_NZ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_BQ_Project_For_Virgin_Media(log_result,filename,directory,result_Check_BQ_ProjectID_For_Virgin_Media):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("BQ Project ID"))
    try:
        # if 1==1:
        Output_lst=[]
        project_id_not_present=[]
        with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
            fileobj=f.read()
            schema_lst=['am','gmip','mbw_bss','ciw','ice','mbw_bss_stage','ciw_stage','it_reporting','mbw','ref','cm','kpi','mbw_stage','summit','ddrs','marketing','mi_marts','usage_agg','ciw_commonviews']
            for schema in schema_lst:
                find_schema=re.findall(r'[\s]+'+schema+'\.[\w]+',fileobj,flags=re.I|re.DOTALL)
                if find_schema:
                    for ele in find_schema:
                        project_id_not_present.append(ele)

            if project_id_not_present:
                result_Check_BQ_ProjectID_For_Virgin_Media.append("N")
                print("\nProject ID not append in "+filename)
                for ele in project_id_not_present:
                    print(ele.strip())
                    Output_lst.append(ele)
                log_result.append(Output_Result_No(Output_lst))
            else:
                result_Check_BQ_ProjectID_For_Virgin_Media.append("Y")
                find_schema=re.findall(r'\$\{bq_project_id\}\.[\w]+\.[\w]+',fileobj,flags=re.I|re.DOTALL)
                if find_schema:
                    for ele in find_schema:
                        Output_lst.append(ele)
                    log_result.append(Output_Result_Yes(Output_lst))
                else:
                    log_result.append(Result_NA())
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_BQ_ProjectID_For_Virgin_Media.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION(log_result,filename,directory,result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("BEGIN_TRANSACTION_And_Commit_TRANSACTION"))
    Begin_list=[]
    commit_list=[]
    output_log=[]

    try:
    # if 1==1:
        with open (os.path.join(OutputFolderPath, filename), 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open (os.path.join(OutputFolderPath, filename), "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                if "/*" in fileLine:
                    if '*/' in fileLine:
                        comment=0
                        pass
                    else:
                        comment=1
                        pass
                elif comment==1:
                    if "*/" in fileLine:
                        comment=0
                        pass
                    else:
                        pass
                elif "--" in fileLine:
                    fileLine=fileLine.split("--")
                    obj=obj+fileLine[0]
                else:
                    obj=obj+fileLine
            find_Begin=re.findall(r"BEGIN[\s]*\;|BEGIN[\s]+TRANSACTION[\s]*\;", obj, flags=re.I)
            for item in find_Begin:
                Begin_list.append(item)
                output_log.append(item)

            find_commit=re.findall(r"COMMIT[\s]*\;|COMMIT[\s]+TRANSACTION[\s]*\;",obj,flags=re.I)
            for item in find_commit:
                commit_list.append(item)
                output_log.append(item)

        # print(Begin_list)
        # print(commit_list)

        if len(Begin_list)==0 and  len(commit_list)==0:
            log_result.append(Result_NA())
            result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION.append("NA")
        elif len(Begin_list) == len(commit_list):
            log_result.append(Output_Result_Yes(output_log))
            result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION.append("Y")
        else:
            log_result.append(Output_Result_No(output_log))
            result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)



#MemSQL
def Check_No_Of_Statements_For_MemSQL(log_result,filename,directory,result_check_no_of_statement_for_memsql):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Number Of Statement "))
    input_no_of_statement=[]
    output_no_of_statement=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                InputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        InputFileObj+=fileline
        InputFileObj+=';'
        sql_statements=sqlparse.split(InputFileObj)
        for sql in sql_statements:
            find_select=re.findall(r'^[\s]*\(*[\s]*SELECT',sql,flags=re.I|re.DOTALL)
            if find_select:
                input_no_of_statement.append('SELECT')
            else:
                find_sql_statement=re.findall(r'^WITH[\s]+\`*[\w]+\`*',sql,flags=re.I|re.DOTALL)
                if find_sql_statement:
                    ele=find_sql_statement[0].upper()
                    ele=ele.replace('`','')
                    ele=re.sub('[\s]+',' ',ele,flags=re.DOTALL)
                    input_no_of_statement.append(ele)


        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), "r") as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                OutputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        sql_statements=sqlparse.split(OutputFileObj)
        for sql in sql_statements:
            find_select=re.findall(r'^[\s]*\(*[\s]*SELECT',sql,flags=re.I|re.DOTALL)
            if find_select:
                output_no_of_statement.append('SELECT')
            else:
                find_sql_statement=re.findall(r'^WITH[\s]+\`*[\w]+\`*',sql,flags=re.I|re.DOTALL)
                if find_sql_statement:
                    ele=find_sql_statement[0].upper()
                    ele=ele.replace('`','')
                    ele=re.sub('[\s]+',' ',ele,flags=re.DOTALL)
                    output_no_of_statement.append(ele)
        # print(input_no_of_statement)
        # print(output_no_of_statement)
        #
        if len(input_no_of_statement)==0 and len(output_no_of_statement)==0:
            log_result.append(Result_NA())
            result_check_no_of_statement_for_memsql.append("NA")
        elif input_no_of_statement==output_no_of_statement:
            log_result.append(Result_Yes(input_no_of_statement,output_no_of_statement))
            result_check_no_of_statement_for_memsql.append("Y")
        else:
            log_result.append(Result_No(input_no_of_statement,output_no_of_statement))
            result_check_no_of_statement_for_memsql.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_check_no_of_statement_for_memsql.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Count_Of_Case_Statement(log_result,filename,directory,result_count_of_case_statement_for_memsql):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Count Of Case Statement "))
    input_case_statement=[]
    output_case_statement=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                InputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        InputFileObj+=fileline
        InputFileObj+=';'
        find_case=re.findall(r'[\W]CASE[\s]+',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_case:
            ele=re.sub(r'^[\W]','',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
            input_case_statement.append(ele)


        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), "r") as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                OutputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        find_case=re.findall(r'[\W]CASE[\s]+',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_case:
            ele=re.sub(r'^[\W]','',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
            output_case_statement.append(ele)
        # print(input_case_statement)
        # print(output_case_statement)

        if len(input_case_statement)==0 and len(output_case_statement)==0:
            log_result.append(Result_NA())
            result_count_of_case_statement_for_memsql.append("NA")
        elif len(input_case_statement)==len(output_case_statement):
            log_result.append(Result_Yes(input_case_statement,output_case_statement))
            result_count_of_case_statement_for_memsql.append("Y("+str(len(input_case_statement))+"/"+str(len(output_case_statement))+")")
        else:
            log_result.append(Result_No(input_case_statement,output_case_statement))
            result_count_of_case_statement_for_memsql.append("N("+str(len(input_case_statement))+"/"+str(len(output_case_statement))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_count_of_case_statement_for_memsql.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Count_Of_Doller_Parameters(log_result,filename,directory,result_count_of_doller_parameter_for_memsql):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Doller Parameters "))
    # input_case_statement=[]
    # output_case_statement=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                InputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        InputFileObj+=fileline
        InputFileObj+=';'
        input_doller_parameter=re.findall(r'\$\{[\w]+\}',InputFileObj,flags=re.I|re.DOTALL)
        input_doller_parameter.sort()
        # print(find_doller_parameter)
        # for ele in find_case:
        #     ele=re.sub(r'^[\W]','',ele,flags=re.I|re.DOTALL)
        #     ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
        #     input_case_statement.append(ele)


        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), "r") as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                OutputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        output_doller_parameter=re.findall(r'\$\{[\w]+\}',OutputFileObj,flags=re.I|re.DOTALL)
        output_doller_parameter.sort()

        # print(input_doller_parameter)
        # print(output_doller_parameter)

        if len(input_doller_parameter)==0 and len(output_doller_parameter)==0:
            log_result.append(Result_NA())
            result_count_of_doller_parameter_for_memsql.append("NA")
        elif input_doller_parameter==output_doller_parameter:
            log_result.append(Result_Yes(input_doller_parameter,output_doller_parameter))
            result_count_of_doller_parameter_for_memsql.append("Y("+str(len(input_doller_parameter))+"/"+str(len(output_doller_parameter))+")")
        else:
            log_result.append(Result_No(input_doller_parameter,output_doller_parameter))
            result_count_of_doller_parameter_for_memsql.append("N("+str(len(input_doller_parameter))+"/"+str(len(output_doller_parameter))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_count_of_doller_parameter_for_memsql.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Count_Of_Coalesce_Function(log_result,filename,directory,resut_count_of_coalesce_fun_for_memsql):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Coalesce Function Count "))
    # input_case_statement=[]
    # output_case_statement=[]
    try:
        if (filename,directory) in inputFile:
            with open (os.path.join(inputFolderPath, filename), 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open (os.path.join(inputFolderPath, filename), "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                InputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        InputFileObj+=fileline
        InputFileObj+=';'
        input_coalesce_fun=re.findall(r'COALESCE[\s]*\(',InputFileObj,flags=re.I|re.DOTALL)
        input_coalesce_fun.sort()
        # print(find_doller_parameter)
        # for ele in find_case:
        #     ele=re.sub(r'^[\W]','',ele,flags=re.I|re.DOTALL)
        #     ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
        #     input_case_statement.append(ele)


        if (filename,directory) in outputFile:
            with open (os.path.join(OutputFolderPath, filename), "r") as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                comment=0
                OutputFileObj=' '
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileline)
                    if '/*' in fileline:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if '*/' in fileline:
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        output_coalesce_fun=re.findall(r'COALESCE[\s]*\(',OutputFileObj,flags=re.I|re.DOTALL)
        output_coalesce_fun.sort()

        # print(input_coalesce_fun)
        # print(output_coalesce_fun)

        if len(input_coalesce_fun)==0 and len(output_coalesce_fun)==0:
            log_result.append(Result_NA())
            resut_count_of_coalesce_fun_for_memsql.append("NA")
        elif len(input_coalesce_fun)==len(output_coalesce_fun):
            log_result.append(Result_Yes(input_coalesce_fun,output_coalesce_fun))
            resut_count_of_coalesce_fun_for_memsql.append("Y("+str(len(input_coalesce_fun))+"/"+str(len(output_coalesce_fun))+")")
        else:
            log_result.append(Result_No(input_coalesce_fun,output_coalesce_fun))
            resut_count_of_coalesce_fun_for_memsql.append("N("+str(len(input_coalesce_fun))+"/"+str(len(output_coalesce_fun))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        resut_count_of_coalesce_fun_for_memsql.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)




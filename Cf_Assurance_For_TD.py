import re, chardet,json
import sqlparse
from mo_sql_parsing import parse
# import pprint
from mo_sql_parsing import format as mo_sql_format
import pandas as pd

from CF_Assurance_Common_Functions import *
feature_list=[]

def PROJECT_ID(log_result,project_ID,filename):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Project ID"))
    word_regex="[a-zA-Z0-9_\-\$\{\}]+"
    try:
        if filename in outputFile:
            comment=0
            file_obj=''
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine

            skip_from=re.findall(r"DAY[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"MONTH[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"YEAR[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"WEEK[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"DAYOFWEEK[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"DAYOFYEAR[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"OUARTER[\s]+FROM[\s]+[\w]+\.[\w]+|"
                                 r"ISOYEAR[\s]+FROM[\s]+[\w]+\.[\w]+",file_obj,flags=re.I|re.DOTALL)

            for item in skip_from:
                file_obj=re.sub(r""+item+"","",file_obj)

            generate_regex=''
            obj_lst=['FROM','JOIN','UPDATE','INTO','TABLE','TRUNCATE','EXISTS','CALL','VIEW','PROCEDURE','MERGE']
            for obj in obj_lst:
                generate_regex+=obj+'[\s]+\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                # generate_regex+=obj+'[\s]+\`*[\w]+\.[\w]+\`*[\s\;\,\)\(]|'
                # generate_regex+=obj+'[\s]+\`*\$*[\w]+\.\$*[\w]+\`*[\s\;\,\)\(]|'
                # generate_regex+=obj+'[\s]+\`*\$*\$*[\w]+\.\$*[\w]+\`*[\s\;\,\)\(]|'
                # generate_regex+=obj+'[\s]+\`*\$\{[\w]+\}\.\$\{[\w]+\}\`*[\s\;\,\)\(]|'
            generate_regex=re.sub(r'\|$','',generate_regex)
            project_id=re.findall(r''+generate_regex,file_obj,flags=re.I|re.DOTALL)
            # print(project_id)
            # project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|'
            #                       r'FROM[\s]+\`*[\w]+\.[\w]+\`*\;|'
            #                       r'PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*\(|'
            #                       r'FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s\;\,\)\(]|'
            #                       r'INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|'
            #                       r'TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]*\;',file_obj,flags=re.I)
            #FROM[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|JOIN[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|UPDATE[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|EXISTS[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|TABLE[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|INTO[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+|PROCEDURE[\s]+\{\{[\w]+\.[\w]+\}\}\.[\w]+[\W]+
            # print(filename)
            # print(project_id)
            #print(file_obj)
            if project_id:
                # project_ID.append("N")
                project_ID.append("N("+str(len(project_id))+")")
                log_result.append(Output_Result_No(project_id))
            else:
                Append_project_id=Append_project_id.replace("{","\{")
                Append_project_id=Append_project_id.replace("}","\}")
                Append_project_id=Append_project_id.replace("$","\$")
                # print(Append_project_id)
                generate_regex=''
                for obj in obj_lst:
                    generate_regex+=obj+'[\s]+\`*'+Append_project_id+'\`*\.\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    # generate_regex+=obj+'[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s\;\,\)\(]|'
                    # generate_regex+=obj+'[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*[\s\;\,\)\(]|'
                    # generate_regex+=obj+'[\s]+\`*'+Append_project_id+'\.\$*[\w]+\.\$*[\w]+\`*[\s\;\,\)\(]|'
                    # generate_regex+=obj+'[\s]+\`*'+Append_project_id+'\.\$*\$*[\w]+\.\$*[\w]+\`*[\s\;\,\)\(]|'
                generate_regex=re.sub(r'\|$','',generate_regex)
                project_id=re.findall(r''+generate_regex,file_obj,flags=re.I|re.DOTALL)
                # project_id=re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\(|FROM[\s]+\`*'+Append_project_id+'\`*\.[\w]+\.[\w]+\`*[\s]*|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|JOIN[\s]+\`*'+Append_project_id+'\`*\.[\w]+\.[\w]+\`*[\s]*|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*\;|'
                #                       r'JOIN[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|FROM[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|UPDATE[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|TABLE[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|EXISTS[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|INTO[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*|PROCEDURE[\s]+\`*'+Append_project_id+'\.\{\{[\w]+\.[\w]+\}\}\.[\w]+\`*',file_obj,flags=re.I)
                # print(Append_project_id)
                # print("YYY",project_id)
                if project_id:
                    project_ID.append("Y("+str(len(project_id))+")")
                    log_result.append(Output_Result_Yes(project_id))
                else:
                    project_ID.append("NA")
                    log_result.append(Result_NA())
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        project_ID.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Data_Type(log_result,filename,DataType_result,check_decimal_datatype_without_scale):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Datatype Checking"))
    BQ_input_cloumn=[]
    input_all_datatype=[]
    BQ_output_cloumn=[]
    output_all_datatype=[]
    try:
        if filename in inputFile:
            comment=0
            table_counter=0
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                final_table_obj=''
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
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+SET[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    find_char=re.findall(r"[\w]+[\s]+VARCHAR[\s]*\([\d]+\)|[\w]+[\s]+CHAR[\s]*\([\d]+\)|[\w]+[\s]+BYTE[\s]*\([\d]+\)|[\w]+[\s]+VARCHAR[\s]+|[\w]+[\s]+VARCHAR\)|[\w]+[\s]+VARCHAR\,|[\w]+[\s]+CHAR[\s]+|[\w]+[\s]+CHAR\,|[\w]+[\s]+CHAR\)|[\w]+[\s]+BLOB[\s]+|[\w]+[\s]+BLOB\,|[\w]+[\s]+BLOB\)|[\w]+[\s]+BLOB[\s]*\([\d]+\)|[\w]+[\s]+CLOB[\s]+|[\w]+[\s]+CLOB\,|[\w]+[\s]+CLOB\)|[\w]+[\s]+CLOB[\s]*\([\d]+\)",table_obj,flags=re.I)
                    for ele in find_char:
                        rplce_char_to_string=re.sub(r"[\s]+CHAR\)*|[\s]+VARCHAR\)*|[\s]+CLOB"," STRING",ele,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+BYTE|[\s]+BLOB","BYTES",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"string,","string",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"DECIMAL,|DECIMAL\)*","NUMERIC",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+","",rplce_char_to_string,flags=re.I)
                        if rplce_char_to_string.lower()=="asstring" or rplce_char_to_string.lower()=="asbytes" or rplce_char_to_string.lower()=="asnumeric" or rplce_char_to_string=='' or rplce_char_to_string.lower().startswith("asstring"):
                            pass
                        else:
                            BQ_input_cloumn.append(ele)
                            input_all_datatype.append(rplce_char_to_string.lower())

                    find_decimal=re.findall(r"[\w]+[\s]+NUMBER[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DEC[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]*\([\d]+\)|[\w]+[\s]+DEC[\s]*\([\d]+\)|[\w]+[\s]+NUMERIC[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]+|[\w]+[\s]+NUMBER[\s]+|[\w]+[\s]+DECIMAL\,|[\w]+[\s]+NUMBER\,|[\w]+[\s]+DECIMAL\)|[\w]+[\s]+NUMBER\)|[\w]+[\s]+NUMERIC[\s]+|[\w]+[\s]+NUMEIC\,|[\w]+[\s]+NUMEIC\)",table_obj,flags=re.I)
                    if(check_decimal_datatype_without_scale=='N' or check_decimal_datatype_without_scale==''):
                        for ele in find_decimal:
                            if "as decimal" in ele.lower() or "as dec" in ele.lower() or "as numeric" in ele.lower():
                                pass
                            else:
                                BQ_input_cloumn.append(ele.lower())
                                find_column_value=re.findall(r"[\d]+\,[\d]+",ele.upper())
                                if find_column_value:
                                    find_column_value="".join(find_column_value)
                                    find_column_value=find_column_value.split(",")
                                    if int(find_column_value[0])>=38 or int(find_column_value[1])>9:
                                        replce_decimal=re.sub("[\s]+DECIMAL|[\s]+DEC"," BIGNUMERIC",ele,flags=re.I)
                                        replce_decimal=re.sub("[\s]+NUMERIC"," BIGNUMERIC",replce_decimal,flags=re.I)
                                        replce_decimal=re.sub("[\s]+NUMBER"," BIGNUMERIC",replce_decimal,flags=re.I)
                                        replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                                        input_all_datatype.append(replce_decimal.lower())
                                    else:
                                        replce_decimal=re.sub("[\s]+DECIMAL|[\s]+DEC"," NUMERIC",ele,flags=re.I)
                                        replce_decimal=re.sub("[\s]+NUMBER"," NUMERIC",replce_decimal,flags=re.I)
                                        replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                                        input_all_datatype.append(replce_decimal.lower())

                                else:
                                    replce_decimal=re.sub("[\s]+DECIMAL|[\s]+DEC"," NUMERIC",ele,flags=re.I)
                                    replce_decimal=re.sub(r"DECIMAL,|DECIMAL\)*","NUMERIC",replce_decimal,flags=re.I)
                                    replce_decimal=re.sub("[\s]+NUMBER"," NUMERIC",replce_decimal,flags=re.I)
                                    replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                                    input_all_datatype.append(replce_decimal.lower())
                        # print(input_all_datatype)

                    else:
                        for ele in find_decimal:

                            if "as decimal" in ele.lower() or "as dec" in ele.lower() or "as numeric" in ele.lower():
                                pass
                            else:
                                replce_decimal=re.sub("[\s]+DECIMAL|[\s]+DEC"," BIGNUMERIC",ele,flags=re.I)
                                replce_decimal=re.sub(r"DECIMAL,|DECIMAL\)*","BIGNUMERIC",replce_decimal,flags=re.I)
                                replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                                input_all_datatype.append(replce_decimal.lower())


                    find_interval=re.findall(r"[\w]+[\s]+INTERVAL",table_obj,flags=re.I)
                    for ele in find_interval:
                        remove_space=re.sub(r"[\s]+","",ele,flags=re.I)
                        if remove_space.lower()=="asinterval" or remove_space=='' or remove_space.lower()=="eachinterval":
                            pass
                        else:
                            BQ_input_cloumn.append(ele.lower())
                            input_all_datatype.append(remove_space.lower())


                    find_integer=re.findall(r"[\w]+[\s]+INTEGER\)*[\s]+|[\w]+[\s]+INTEGER\,|[\w]+[\s]+INT\)*[\s]+|[\w]+[\s]+INT\,|[\w]+[\s]+BIGINT\)*[\s]+|[\w]+[\s]+BIGINT\,|[\w]+[\s]+SMALLINT\)*[\s]+|[\w]+[\s]+SMALLINT\,|[\w]+[\s]+TINYINT\)*[\s]+|[\w]+[\s]+TINYINT\,|[\w]+[\s]+BYTEINT\)*[\s]+|[\w]+[\s]+BYTEINT\,",table_obj,flags=re.I)
                    for ele in find_integer:
                        # print(ele)
                        # replce_integer=re.sub(r"[\s]+INTEGER\)*[\s]*|[\s]+INT\)*[\s]*|[\s]+BIGINT\)*[\s]*|[\s]+SMALLINT\)*[\s]*|[\s]+TINYINT\)*[\s]*|[\s]+BYTEINT\)*[\s]*","INT64",ele,flags=re.I)
                        replce_integer=re.sub(r"[\s]+INT,|[\s]+INT\)|[\s]+INT[\s]+","INTEGER",ele,flags=re.I)
                        replce_integer=re.sub(r"[\s]+|\,|\)","",replce_integer,flags=re.I)
                        # print(replce_integer)
                        if replce_integer=="" or replce_integer.lower()=="andint" or replce_integer.lower()=="asint" or replce_integer.lower()=="andint64" or replce_integer.lower()=="asint64" or replce_integer.lower()=="asbigint" or replce_integer.lower()=="andbigint" or replce_integer.lower()=="asinteger" or replce_integer.lower()=="assmallint" or replce_integer.lower()=="andinteger" or replce_integer.lower()=="andsmallint":
                            pass
                        else:
                            input_all_datatype.append(replce_integer.lower())
                            BQ_input_cloumn.append(ele.lower())

                    find_timestamp=re.findall(r"[\w]+[\s]+TIMESTAMP[\s]*|[\w]+[\s]+DATE[\s]+|[\w]+[\s]+DATE\,|[\w]+[\s]+DATE\)",table_obj,flags=re.I)
                    for ele in find_timestamp:
                        replce_timestamp=re.sub(r"[\s]+TIMESTAMP","DATETIME",ele,flags=re.I)
                        replce_timestamp=re.sub(r"[\s]+DATE\,|[\s]+DATE\)","DATE",replce_timestamp,flags=re.I)
                        replce_timestamp=re.sub(r"[\s]+","",replce_timestamp,flags=re.I)
                        if replce_timestamp.lower()=="betweendatetime" or replce_timestamp.lower()=="thendate" or replce_timestamp.lower()=="betweendate" or replce_timestamp.lower()=="todate" or replce_timestamp.lower()=="selectdate" or replce_timestamp.lower()=="elsedate" or replce_timestamp.lower()=="anddatetime" or replce_timestamp.lower()=="defaultdatetime" or replce_timestamp.lower()=="asdatetime" or "asdatetime" in replce_timestamp.lower() or replce_timestamp.lower()=="" or replce_timestamp.lower()=="asdate" or replce_timestamp.lower()=="anddate" or replce_timestamp.lower()=="defaultdate":
                            pass
                        else:
                            input_all_datatype.append(replce_timestamp.lower())
                            BQ_input_cloumn.append(ele.lower())

                    find_float=re.findall(r"[\w]+[\s]+FLOAT[\s]+|[\w]+[\s]+FLOAT\,|[\w]+[\s]+FLOAT\)|[\w]+[\s]+DOUBLE[\s]+|[\w]+[\s]+DOUBLE\)|[\w]+[\s]+DOUBLE\,",table_obj,flags=re.I)
                    for ele in find_float:
                        replce_float=re.sub(r"[\s]+FLOAT|[\s]+DOUBLE","FLOAT64",ele,flags=re.I)
                        replce_float=re.sub(r"[\s]+|\,|\)","",replce_float,flags=re.I)
                        if replce_float.lower()=="andfloat64" or replce_float.lower()=="asfloat64" or replce_float=="":
                            pass
                        else:
                            input_all_datatype.append(replce_float.lower())
                            BQ_input_cloumn.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                final_table_obj=''
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
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    find_integer=re.findall(r"[\w]+[\s]+INT64[\s]*|[\w]+[\s]+INTEGER[\s]*|[\w]+[\s]+SMALLINT[\s]*|[\w]+[\s]+BYTEINT[\s]*|[\w]+[\s]+TINYINT[\s]*|[\w]+[\s]+BIGINT[\s]*",table_obj,flags=re.I)
                    for item in find_integer:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        if remove_space.lower() == "asint64" or remove_space.lower()=="andint64" or remove_space.lower() == "asinteger" or remove_space.lower()=="andinteger" or remove_space.lower()=="asbigint" or remove_space.lower()=="andbigint" or remove_space.lower()=="assmallint" or remove_space.lower()=="andsmallint" or remove_space.lower()=="astinyint" or remove_space.lower()=="andtinyint" or remove_space.lower()=="asbyteint" or remove_space.lower()=="andbyteint":
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())
                    find_datetime=re.findall(r"[\w]+[\s]+DATETIME[\s]*|[\w]+[\s]+DATE[\s]+|[\w]+[\s]+DATE[\s]*\,|[\w]+[\s]+DATE\)",table_obj,flags=re.I)
                    for item in find_datetime:
                        remove_space=re.sub(r"[\s]+|\)|\,","",item,flags=re.I)
                        if remove_space.lower()=="betweendatetime" or remove_space.lower()=="thendate" or remove_space.lower()=="todate" or remove_space.lower()=="selectdate" or remove_space.lower()=="elsedate" or  remove_space.lower() == "asdatetime" or remove_space.lower()=="anddatetime" or remove_space.lower()=="asdate" or remove_space.lower()=="anddate" or remove_space.lower()=="defaultdate":
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())
                    find_float=re.findall(r"[\w]+[\s]+FLOAT64[\s]*",table_obj,flags=re.I)
                    for item in find_float:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        if remove_space.lower() == "asfloat64" or remove_space.lower() == "andfloat64" or "asfloat64" in remove_space.lower() :
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

                    find_char=re.findall(r"\`[\w]+\`[\s]+STRING\([\d]+\)|[\w]+[\s]+STRING\([\d]+\)|[\w]+[\s]+BYTES\([\d]+\)|[\w]+[\s]+STRING[\s]+|[\w]+[\s]+STRING\,",table_obj,flags=re.I)
                    for item in find_char:
                        remove_space=re.sub(r"[\s]+|\`","",item,flags=re.I)
                        remove_space=re.sub(r"string,","string",remove_space,flags=re.I)
                        if remove_space.lower() == "asstring" or remove_space.lower() == "andstring":
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

                    find_char=re.findall(r"[\w]+[\s]+DECIMAL\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+NUMERIC\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+NUMERIC\([\d]+\)|[\w]+[\s]+BIGDECIMAL\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+BIGNUMERIC\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+NUMERIC[\s]+|[\w]+[\s]+NUMERIC\,|[\w]+[\s]+NUMERIC\)",table_obj,flags=re.I)
                    for item in find_char:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        remove_space=re.sub(r"NUMERIC\)","NUMERIC",remove_space,flags=re.I)
                        if "asnumeric" in remove_space.lower() or remove_space.lower().startswith("asbignumeric"):
                            pass
                        else:

                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

                    find_interval=re.findall(r"[\w]+[\s]+INTERVAL",table_obj,flags=re.I)
                    for item in find_interval:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        if remove_space=="asinterval" or remove_space=='':
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

        input_all_datatype.sort()
        output_all_datatype.sort()
        BQ_input_cloumn.sort()
        BQ_output_cloumn.sort()

        # print(len(input_all_datatype))
        # print(len(output_all_datatype))
        # print("INPUT :",input_all_datatype)
        # print("OUTPUT:",output_all_datatype)
        # print(output_all_datatype-input_all_datatype)
        for ele in input_all_datatype:
            if ele in output_all_datatype:
                pass
            else:
                pass
                # print(ele)
        if len(input_all_datatype)==0 and len(output_all_datatype) == 0:
            DataType_result.append("NA")
            log_result.append(Result_NA())

        elif input_all_datatype==output_all_datatype:
            DataType_result.append("Y")
            log_result.append(Result_Yes(BQ_input_cloumn,BQ_output_cloumn))
        else:
            DataType_result.append("N")
            log_result.append(Result_No(BQ_input_cloumn,BQ_output_cloumn))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        DataType_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Verify_Index_And_Partition(log_result,filename,partition_result,index,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Index Checking"))
    input_cluster_dict={}
    input_partition_dict={}
    output_cluster_dict={}
    output_partition_dict={}
    index_input_final_lst=[]
    index_output_final_lst=[]
    partition_input_final_lst=[]
    partition_output_final_lst=[]
    index_final_result='NA'
    partition_final_result='NA'
    try:
    # if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                table_obj=''
                table_counter=0
                dict_key_counter=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:  #table_counter
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
                    else:
                        if table_counter==0:
                            table_obj=''
                            input_index_column=[]
                            input_partition_column=[]
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+\,|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+SET[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                            if find_table:
                                # print(find_table)
                                table_obj=table_obj+fileLine
                                table_counter=1

                                name1=re.findall(r'[\w]+\.([\w]+)',find_table[0],flags=re.I)
                                name2=re.findall(r'TABLE[\s]+([\w]+)',find_table[0],flags=re.I)
                                if name1:
                                    key_name=name1[0].lower()
                                elif name2:
                                    key_name=name2[0].lower()


                                # print("key",key_name)
                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                # print(table_obj)
                                find_index=re.findall(r"INDEX[\s]*[\w]*[\s]*\(.*\)",table_obj,flags=re.I|re.DOTALL)
                                for ele in find_index:
                                    find_index_len=len(re.findall(r"INDEX",ele,flags=re.I|re.DOTALL))
                                    # print(find_index_len)
                                    if find_index_len>1:
                                        pass
                                    else:
                                        ele=re.sub(r"\n","",ele)
                                        table_obj=re.sub(r"INDEX[\s]*\(.*\)[\s]*\;",ele,table_obj,flags=re.I|re.DOTALL)
                                    # print(ele)
                                    # find_index
                                # print(table_obj)
                                index_cloumn=re.findall(r"Unique[\s]+index[\s]*[\w]*[\s]*\((.*\))|NO[\s]+Primary[\s]+index[\s]*[\w]*[\s]*\((.*\))|UNIQUE[\s]+Primary[\s]+index[\s]*[\w]*[\s]*\((.*\))|Primary[\s]+index[\s]*[\w]*[\s]*\((.*\))|index[\s]*[\w]*[\s]*\((.*\))",table_obj,flags=re.I)
                                # print(index_cloumn)
                                for tuple_ele in index_cloumn:
                                    for ele in tuple_ele:
                                        if ele.strip()=='':
                                            pass
                                        else:
                                            input_index=re.split("\)",ele,flags=re.I|re.DOTALL)[0]
                                            ele=re.split(",",input_index)
                                            for i in ele:
                                                i=re.sub(r'[\s]+','',i,flags=re.I|re.DOTALL)
                                                if i.lower() not in input_index_column:
                                                    input_index_column.append(i.lower())
                                # print(input_index_column)
                                partition_cloumn_with_over=re.findall(r"OVER[\s]*\([\s]*PARTITION[\s]+BY[\s]*",table_obj,flags=re.I)
                                if partition_cloumn_with_over:
                                    pass
                                else:
                                    partition_cloumn=re.findall(r"PARTITION[\s]+BY([\s]*[\w]*[\s]*)*\((.*\))",table_obj,flags=re.I|re.DOTALL)
                                    # print(partition_cloumn)
                                    for tuple_ele in partition_cloumn:
                                        for ele in tuple_ele:
                                            if ele.strip()=='':
                                                pass
                                            else:
                                                input_partition=re.split("\)",ele)[0]
                                                if "BETWEEN " in input_partition.upper():
                                                    input_partition=re.sub(r"BETWEEN[\s]+(.*)","",input_partition,flags=re.I|re.DOTALL)
                                                ele=re.split(",",input_partition)
                                                for i in ele:
                                                    i = re.sub(r"RANGE\_N[\s]*\(", "", i, flags = re.I)
                                                    i=re.sub(r'[\s]+','',i)
                                                    input_partition_column.append(i.lower())
                                    
                                    for ele in input_index_column:
                                        find_datatype=re.findall(r''+ele+'[\s]+([\w]+)',table_obj.upper(),flags=re.I)
                                        for i in find_datatype:
                                            if "FLOAT" in i:
                                                input_index_column.remove(ele)
                                    for ele in input_partition_column:
                                        find_datatype=re.findall(r"[\s]+'+ele+'[\s]+([\w]+)",table_obj.upper(),flags=re.I)
                                        for i in find_datatype:
                                            if "CHAR" in i or "VARCHAR" in i or "STRING" in i:
                                                input_partition_column.remove(ele)
                                                input_index_column.append(ele.lower())
                                    # print(input_partition_column)
                                # print(key_name)
                                # print(input_index_column)
                                input_cluster_dict[key_name]=input_index_column
                                input_partition_dict[key_name]=input_partition_column
                            else:
                                table_obj=table_obj+fileLine
        # print(input_cluster_dict)
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                table_obj=''
                table_counter=0
                dict_key_counter=0
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
                    else:
                        if table_counter==0:
                            table_obj=''
                            key_name=''
                            output_cluster_column=[]
                            output_partition_column=[]
                            if fun_project_ID=="Y":
                                fileLine=fileLine.replace(Append_project_id+".","")
                            find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+[\W]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+[\W]+|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\W]+|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\W]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+\`*[\w]+\.[\w]+\`*[\W]+|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+|"
                                                  r"CREATE[\s]+TABLE[\s]+\`*[\w]+\.[\w]+\`*[\W]+",fileLine,flags=re.I)
                            if find_table:
                                table_obj=table_obj+fileLine
                                table_counter=1
                                name1=re.findall(r'[\w]+\.([\w]+)',find_table[0],flags=re.I)
                                name2=re.findall(r'EXISTS[\s]+([\w]+)|TABLE[\s]+([\w]+)',find_table[0],flags=re.I)
                                if name1:
                                    key_name=name1[0].lower()
                                elif name2:
                                    for a in name2:
                                        for b in a:
                                            if b.strip()=='' or b=='IF' or b=='if':
                                                pass
                                            else:
                                                key_name=b.lower()
                                if ";" in fileLine:
                                    dict_key_counter+=1
                                    table_obj=table_obj+fileLine
                                    table_counter=0
                                    # print("table_obj",table_obj)
                                    cluster_by_cloumn=re.findall(r"Cluster[\s]+BY[\s]*\(*(.*)\)*",table_obj,flags=re.I)
                                    # print("cluster_by_cloumn",cluster_by_cloumn)
                                    if cluster_by_cloumn:
                                        find_word=re.findall(r'[\w]+[\s]+[\w]+',cluster_by_cloumn[0])
                                        if find_word:
                                            ele=re.split("[\s]+",find_word[0])
                                            cluster_by_cloumn[0]=re.sub(r''+ele[0]+"(.*)",''+ele[0]+";",cluster_by_cloumn[0])
                                        cluster_column=re.split("[\s]+AS[\s]+|;|\(|\)|\"|'",cluster_by_cloumn[0].lower())
                                        for ele in cluster_column:
                                            ele=re.split(",",ele)
                                            for i in ele:
                                                if i.strip()=='':
                                                    pass
                                                else:
                                                    i=re.sub(r'[\s]+','',i)
                                                    output_cluster_column.append(i.lower())

                                        for ele in output_cluster_column:
                                            float_col=re.findall(r''+ele+'[\s]+([\w]+)',table_obj,flags=re.I|re.DOTALL)
                                            if float_col:
                                                if 'FLOAT' in float_col[0]:
                                                    index_final_result='N'

                                    partition_cloumn_with_over=re.findall(r"OVER[\s]*\([\s]*PARTITION[\s]+BY[\s]*",table_obj,flags=re.I)
                                    if partition_cloumn_with_over:
                                        pass
                                    else:

                                        partition_cloumn=re.findall(r"PARTITION[\s]+BY[\s]*[\w]*[\s]*\((.*\))|PARTITION[\s]+BY[\s]+([\w]+)",table_obj,flags=re.I)
                                        # print(partition_cloumn)
                                        if partition_cloumn:
                                            for i in partition_cloumn:
                                                for x in i:
                                                    if x.strip() == "":
                                                        pass
                                                    else:
                                                        x=re.sub('\,[\s]*GENERATE_ARRAY.*','',x,flags=re.I|re.DOTALL)
                                                        partition_column=re.split("[\s]+AS[\s]+|;|\(|\)",x.lower())
                                                        # print(partition_column)
                                                        for ele in partition_column:
                                                            # print(ele.strip())
                                                            # print("EEE",ele)
                                                            ele=re.split(",",ele)
                                                            for i in ele:
                                                                i=i.strip()
                                                                if i.strip()=='' or i.lower()=="month":
                                                                    pass
                                                                else:
                                                                    i=re.sub(r'[\s]+','',i)
                                                                    output_partition_column.append(i.lower())
                                    output_cluster_dict[key_name]=output_cluster_column
                                    output_partition_dict[key_name]=output_partition_column

                        elif table_counter==1:
                            if ";" in fileLine:
                                dict_key_counter+=1
                                table_obj=table_obj+fileLine
                                table_counter=0
                                # print(table_obj)
                                cluster_by_cloumn=re.findall(r"Cluster[\s]+BY[\s]*\(*(.*)\)*",table_obj,flags=re.I)
                                # print("cluster_by_cloumn",cluster_by_cloumn)
                                if cluster_by_cloumn:
                                    find_word=re.findall(r'[\w]+[\s]+[\w]+',cluster_by_cloumn[0])
                                    if find_word:
                                        ele=re.split("[\s]+",find_word[0])
                                        cluster_by_cloumn[0]=re.sub(r''+ele[0]+"(.*)",''+ele[0]+";",cluster_by_cloumn[0])
                                    cluster_column=re.split("[\s]+AS[\s]+|;|\(|\)|\"|'",cluster_by_cloumn[0].lower())
                                    for ele in cluster_column:
                                        ele=re.split(",",ele)
                                        for i in ele:
                                            if i.strip()=='':
                                                pass
                                            else:
                                                i=re.sub(r'[\s]+','',i)
                                                output_cluster_column.append(i.lower())

                                    for ele in output_cluster_column:
                                        float_col=re.findall(r''+ele+'[\s]+([\w]+)',table_obj,flags=re.I|re.DOTALL)
                                        if float_col:
                                            if 'FLOAT' in float_col[0]:
                                                index_final_result='N'

                                partition_cloumn_with_over=re.findall(r"OVER[\s]*\([\s]*PARTITION[\s]+BY[\s]*",table_obj,flags=re.I)
                                if partition_cloumn_with_over:
                                    pass
                                else:

                                    partition_cloumn=re.findall(r"PARTITION[\s]+BY[\s]*[\w]*[\s]*\((.*\))|PARTITION[\s]+BY[\s]+([\w]+)",table_obj,flags=re.I)
                                    # print(partition_cloumn)
                                    if partition_cloumn:
                                        for i in partition_cloumn:
                                            for x in i:
                                                if x.strip() == "":
                                                    pass
                                                else:
                                                    x=re.sub('\,[\s]*GENERATE_ARRAY.*','',x,flags=re.I|re.DOTALL)
                                                    partition_column=re.split("[\s]+AS[\s]+|;|\(|\)",x.lower())
                                                    # print(partition_column)
                                                    for ele in partition_column:
                                                        # print(ele.strip())
                                                        # print("EEE",ele)
                                                        ele=re.split(",",ele)
                                                        for i in ele:
                                                            i=i.strip()
                                                            if i.strip()=='' or i.lower()=="month":
                                                                pass
                                                            else:
                                                                i=re.sub(r'[\s]+','',i)
                                                                output_partition_column.append(i.lower())
                                output_cluster_dict[key_name]=output_cluster_column
                                output_partition_dict[key_name]=output_partition_column


                            else:
                                table_obj=table_obj+fileLine
        # print(input_cluster_dict)
        # print(output_cluster_dict)
        # print(input_partition_dict)
        # print(output_partition_dict)
        for key in input_cluster_dict:
            for ele in input_cluster_dict[key]:
                index_input_final_lst.append(ele)

            if key in output_cluster_dict:
                for ele in output_cluster_dict[key]:
                    index_output_final_lst.append(ele)
        # print("index_input_final_lst", index_input_final_lst)

        if index_final_result=='N':
            pass
        else:
            for key in input_cluster_dict:
                val_lst=input_cluster_dict[key]
                # print("VVV",input_cluster_dict[key])
                if key in output_cluster_dict:
                    final_column=[]
                    # print(key)
                    if len(output_cluster_dict[key]) > 4:
                        index_final_result='N'
                        break
                    elif len(input_cluster_dict[key]) !=0 and len(output_cluster_dict[key])==0 or len(input_cluster_dict[key])==0 and len(output_cluster_dict[key])!=0:
                        index_final_result='N'
                        break
                    elif len(input_cluster_dict[key])==0 and len(output_cluster_dict[key])==0:
                        # print("ff")
                        pass
                    else:
                        ctr=0
                        for input_ele in input_cluster_dict[key]:
                            for output_ele in output_cluster_dict[key]:
                                if input_ele.lower() in output_ele.lower():
                                    if ctr == 4:
                                        break
                                    if output_ele not in final_column:
                                        final_column.append(output_ele)
                                        ctr+=1

                        if len(final_column)==4:
                            if any(element in final_column for element in input_cluster_dict[key]):
                                index_final_result='Y'
                            else:
                                final_column.sort()
                                input_cluster_dict[key].sort()
                                if final_column == input_cluster_dict[key]:
                                    index_final_result='Y'
                        elif input_cluster_dict[key]==output_cluster_dict[key]:
                            index_final_result='Y'
                        else:
                            index_final_result='N'
                            break
                elif len(val_lst)==0:
                    pass
                else:
                    # print("KKK",key)
                    index_final_result='N'
                    break
        if index_final_result=='N':
            index.append("N")
            log_result.append(Result_No(index_input_final_lst,index_output_final_lst))
        elif index_final_result=='NA':
            index.append("NA")
            log_result.append(Result_NA())
        elif index_final_result=='Y':
            index.append("Y")
            log_result.append(Result_Yes(index_input_final_lst,index_output_final_lst))

        log_result.append(Common("Partition Checking"))
        for key in input_partition_dict:
            for ele in input_partition_dict[key]:
                partition_input_final_lst.append(ele)

            if key in output_partition_dict:
                for ele in output_partition_dict[key]:
                    partition_output_final_lst.append(ele)
        for key in input_partition_dict:
            val_lst=input_partition_dict[key]
            if key in output_partition_dict:
                if len(input_partition_dict[key])==0 and len(output_partition_dict[key]) == 0:
                    pass
                    # partition_final_result='NA'

                elif len(input_partition_dict[key]) > 0 and len(output_partition_dict[key]) > 0:
                    if input_partition_dict[key]== output_partition_dict[key]:
                        partition_final_result='Y'
                    else:
                        partition_final_result='N'
                        break

                else:
                    partition_final_result='N'
                    break
            elif len(val_lst)==0:
                pass
            else:
                partition_final_result='N'
                break



        if partition_final_result=='N' and len(partition_input_final_lst)==0 and len(partition_output_final_lst)==0:
            partition_result.append("NA")
            log_result.append(Result_NA())
        elif partition_final_result=='N':
            partition_result.append("N")
            log_result.append(Result_No(partition_input_final_lst,partition_output_final_lst))
        elif partition_final_result=='NA':
            partition_result.append("NA")
            log_result.append(Result_NA())
        elif partition_final_result=='Y':
            partition_result.append("Y")
            log_result.append(Result_Yes(partition_input_final_lst,partition_output_final_lst))


    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        index.append("Encoding")
        partition_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def No_Of_Statement_In_Script(log_result,filename,Result_No_Of_Statement,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Number of Statements in Script"))
    with_Append_project_id_Final_input_sql_statements=[]
    without_Append_project_id_Final_input_sql_statements=[]
    with_Append_project_id_Final_output_sql_statements=[]
    without_Append_project_id_Final_output_sql_statements=[]
    all_input_file_statements=[]
    all_output_file_statements=[]
    flag=0
    word_regex="[a-zA-Z0-9_\-\$\{\}]+"
    special_char=['$','{','}']
    Regex_Project_Id=Append_project_id
    for i in special_char:
        Regex_Project_Id=Regex_Project_Id.replace(i,'\\'+i)
    # print(filename)
    try:
    # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--",fileLine)
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

            find_activity_and_errorcode=re.findall(r"\.IF[\s]+ERRORCODE|\.IF[\s]+ACTIVITYCOUNT",obj,flags=re.I|re.DOTALL)
            if find_activity_and_errorcode:
                flag=1
            obj=obj.replace('#','_')
            obj=re.sub(r'REPLACE[\s]+MACRO[\s]+[\w]*\.*[\w]+[\s]*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)[\s]*(AS)*[\s]*\(','',obj,flags=re.I|re.DOTALL)

            file_obj1=obj.split(";")
            file_obj=[]
            for ele in file_obj1:
                file_obj.append(ele.strip())
            for element in file_obj:
                # print(element)
                element+=' '
                find_update_with_alice=re.findall(r"^[\s]*UPDATE[\s]+([\w]+)[\s]+FROM[\s]+",element,flags=re.I|re.DOTALL)
                # print(find_update_with_alice)
                if find_update_with_alice:
                    find_table=re.findall(r'[\,\s]('+word_regex+'\.'+word_regex+')[\s]+(AS)*[\s]*'+find_update_with_alice[0],element,flags=re.I|re.DOTALL)
                    if find_table:
                        element=re.sub(r'UPDATE[\s]+'+find_update_with_alice[0],'UPDATE '+find_table[0][0],element,flags=re.I|re.DOTALL)
                    else:
                        find_table=re.findall(r'[\,\s]('+word_regex+')[\s]+(AS)*[\s]*'+find_update_with_alice[0],element,flags=re.I|re.DOTALL)
                        if find_table:
                            element=re.sub(r'UPDATE[\s]+'+find_update_with_alice[0],'UPDATE '+find_table[0][0],element,flags=re.I|re.DOTALL)

                find_update_with_alice=re.findall(r"^[\s]*UPD[\s]+([\w]+)[\s]+FROM[\s]+",element,flags=re.I|re.DOTALL)
                # print(find_update_with_alice)
                if find_update_with_alice:
                    find_table=re.findall(r'[\,\s]('+word_regex+'\.'+word_regex+')[\s]+(AS)*[\s]*'+find_update_with_alice[0],element,flags=re.I|re.DOTALL)
                    if find_table:
                        element=re.sub(r'UPD[\s]+'+find_update_with_alice[0],'UPD '+find_table[0][0],element,flags=re.I|re.DOTALL)
                    else:
                        find_table=re.findall(r'[\,\s]('+word_regex+')[\s]+(AS)*[\s]*'+find_update_with_alice[0],element,flags=re.I|re.DOTALL)
                        if find_table:
                            element=re.sub(r'UPD[\s]+'+find_update_with_alice[0],'UPD '+find_table[0][0],element,flags=re.I|re.DOTALL)
                    # print(find_table)
                    # print(element)
                # find_update=re.findall(r"UPDATE[\s]*[\w]+[\s]*FROM[\s]*[\w]+\.[\w]+",element,flags=re.I|re.DOTALL)
                # if find_update:
                #     for item in find_update:
                #         find_table_and_schema=re.findall(r"[\w]+\.[\w]+",item,flags=re.I)
                #         replace_alias=re.sub("UPDATE[\s]+[\w]+[\s]+","UPDATE "+find_table_and_schema[0]+"\n",item,flags=re.I)
                #         element=element.replace(item+" ",replace_alias+" ")
                # find_update=re.findall(r"UPDATE[\s]*[\w]+[\s]*FROM[\s]*[\w]+[\s]+",element,flags=re.I|re.DOTALL)
                # if find_update:
                #     for item in find_update:
                #         find_table_and_schema=re.findall(r"FROM[\s]+[\w]+[\s]+",item,flags=re.I)
                #         if find_table_and_schema:
                #             remove_space=re.sub(r"[\s]+"," ",find_table_and_schema[0])
                #             find_table_and_schema=remove_space.split()
                #             replace_alias=re.sub("UPDATE[\s]+[\w]+[\s]+","UPDATE "+find_table_and_schema[1]+"\n",item,flags=re.I)
                #             element=element.replace(item,replace_alias+" ")
                # find_update=re.findall(r"UPD[\s]*[\w]+[\s]*FROM[\s]*[\w]+\.[\w]+",element,flags=re.I|re.DOTALL)
                # if find_update:
                #     for item in find_update:
                #         find_table_and_schema=re.findall(r"[\w]+\.[\w]+",item,flags=re.I)
                #         replace_alias=re.sub("UPD[\s]+[\w]+[\s]+","UPD "+find_table_and_schema[0]+"\n",item,flags=re.I)
                #         element=element.replace(item+" ",replace_alias+" ")
                # find_update=re.findall(r"UPD[\s]*[\w]+[\s]*FROM[\s]*[\w]+[\s]+",element,flags=re.I|re.DOTALL)
                # if find_update:
                #     for item in find_update:
                #         find_table_and_schema=re.findall(r"FROM[\s]+[\w]+[\s]+",item,flags=re.I)
                #         if find_table_and_schema:
                #             remove_space=re.sub(r"[\s]+"," ",find_table_and_schema[0])
                #             find_table_and_schema=remove_space.split()
                #             replace_alias=re.sub("UPD[\s]+[\w]+[\s]+","UPD "+find_table_and_schema[1]+"\n",item,flags=re.I)
                #             element=element.replace(item+" ",replace_alias+" ")

                # print(element)
                obj_to_find=["CREATE[\s]+TABLE", "CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE", "CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE", "CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE", "CREATE[\s]+VOLATILE[\s]+TABLE", "REPLACE[\s]+PROCEDURE", "CREATE[\s]+VIEW", "REPLACE[\s]+VIEW", "INSERT[\s]+INTO","INSERT", "INS", "UPDATE", "UPD", "DELETE[\s]+FROM", "DELETE", "DEL[\s]+FROM", "DROP[\s]+TABLE", "MERGE", "CREATE[\s]+SET[\s]+TABLE", "CREATE[\s]+MULTISET[\s]+TABLE", "CREATE[\s]+PROCEDURE", "CREATE[\s]+FUNCTION","ALTER[\s]+TABLE","EXEC"]
                # "REPLACE[\s]+MACRO",
                generate_regex=''
                for obj in obj_to_find:
                    generate_regex+="^[\s]*"+obj+'[\s]+\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    generate_regex+="^[\s]*"+obj+'[\s]+\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                generate_regex=re.sub(r'\|$','',generate_regex)
                input_sql_statements=re.findall(r""+generate_regex,element,flags=re.I)
                # input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*"
                #                                 r"|^WITH[\s]+CTE[\s]+[\w]+[\s]+|REPLACE[\s]+MACRO[\s]*[\w]+\.[\w]+|CREATE[\s]+VOLATILE[\s]+"
                #                                 r"TABLE[\s]*[\w]+[\s]*|REPLACE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|CREATE[\s]+VIEW[\s]*[\w]+\.[\w]+"
                #                                 r"|REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+[\s]+|^INSERT[\s]+INTO[\s]+[\w]+\(|^INS[\s]+[\w]+\.[\w]+|^INS[\s]+[\w]+[\s]+|^INS[\s]+[\w]+\(|"
                #                                 r"^UPDATE[\s]*[\w]+\.[\w]+|^UPDATE[\s]+[\w]+[\s]+|^UPD[\s]*[\w]+\.[\w]+|^UPD[\s]+[\w]+[\s]+"
                #                                 r"|DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+|"
                #                                 r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]*[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+[\s]*|"
                #                                 r"MERGE[\s]+[\w]+\.[\w]+|CREATE[\s]+SET[\s]+TABLE[\s]+[\w]+\.[\w]+|"
                #                                 r"CREATE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|"
                #                                 r"CREATE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|"
                #                                 r""+generate_regex,element,flags=re.I)   #|CALL[\s]+[\w]+\.[\w]+
                # print(input_sql_statements)
                if fun_project_ID == "Y":
                    obj=re.sub("[\s]+SELECT|SELECT[\s]+","SELECT ",element,flags=re.I|re.DOTALL)
                    if obj.upper().startswith("SELECT ") or obj.upper().startswith("SEL "):
                        find_select=re.findall(r"^SELECT[\s]+|^SEL[\s]+",obj,flags=re.I|re.DOTALL)
                        if find_select:
                            if "SEL " in find_select[0].upper():
                                find_select[0]=re.sub(r"SEL","SELECT",find_select[0])
                            find_select=re.sub(r"[\s]+","",find_select[0])
                            with_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                        all_input_file_statements.append(obj.upper())
                    for item in input_sql_statements:
                        # print(item)
                        remove_space=re.sub(r"[\s]+"," ",item)
                        all_input_file_statements.append(remove_space.upper())
                        item=re.sub(r"\n","",item)
                        item=re.sub(r"CREATE[\s]+SET[\s]+TABLE[\s]+|CREATE[\s]+MULTISET[\s]+TABLE[\s]+|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        # item=re.sub(r"INSERT[\s]+INTO[\s]+|^INS[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"^SEL[\s]+","SELECT ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                        item=re.sub(r"REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+VIEW[\s]+","CREATE VIEW IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                        #item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                        if re.findall(r"drop[\s]+table[\s]*"+word_regex+"\."+word_regex+"",item,flags=re.I):
                            item=re.sub(r"drop[\s]+table[\s]+","DROP TABLE IF EXISTS `"+Append_project_id+".",item,flags=re.I)
                        else:
                            item=re.sub(r"drop[\s]+table[\s]+","DROP TABLE IF EXISTS ",item,flags=re.I)
                        find_delete=re.findall(r"DELETE[\s]+FROM|DEL[\s]+FROM|DELETE[\s]+",item,flags=re.I)
                        if find_delete:
                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)

                        if re.findall(r"INSERT[\s]+INTO[\s]*"+word_regex+"\."+word_regex+"",item,flags=re.I):
                            item=re.sub(r"INSERT[\s]+INTO[\s]+|^[\s]*INSERT[\s]+|^INS[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                        else:
                            item=re.sub(r"INSERT[\s]+INTO[\s]+|^[\s]*INSERT[\s]+|^INS[\s]+","INSERT INTO ",item,flags=re.I)
                        if re.findall(r"UPDATE[\s]*[\w]+\.[\w]+|UPD[\s]*[\w]+\.[\w]+",item,flags=re.I):
                            # print(item)
                            item=re.sub(r"UPDATE|^UPD ","UPDATE `"+Append_project_id+".",item,flags=re.I)
                        else:
                            # print(item)
                            item=re.sub(r"UPDATE|^UPD ","UPDATE ",item,flags=re.I)
                        item=re.sub(r"^[\s]*EXEC[\s]+","CALL ",item,flags=re.I)
                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                        item=re.sub(r"[\s]+|\;|\(|\`|,","",item)
                        item=re.sub(r"\.[\s]+",".",item)
                        item=re.sub(r""+Regex_Project_Id+'.','',item)
                        with_Append_project_id_Final_input_sql_statements.append(item.upper())
                else:
                    # print("ddd",element)
                    obj=re.sub("[\s]+SELECT|SELECT[\s]+","SELECT ",element,flags=re.I)
                    if obj.upper().startswith("SELECT ") or obj.upper().startswith("SEL "):
                        find_select=re.findall(r"^SELECT[\s]+|^SEL[\s]+",obj,flags=re.I)
                        if find_select:
                            # print(find_select[0])
                            if "SEL " in find_select[0].upper():
                                find_select[0]=re.sub(r"SEL","SELECT",find_select[0])
                            find_select=re.sub(r"[\s]+","",find_select[0])

                            without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                        all_input_file_statements.append(obj.upper())
                    for item in input_sql_statements:
                        remove_space=re.sub(r"[\s]+"," ",item)
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
                        find_delete=re.findall(r"DELETE[\s]+FROM|DEL[\s]+FROM|DELETE[\s]+",item,flags=re.I)
                        if find_delete:
                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                        item=re.sub(r"UPDATE|^UPD ","UPDATE ",item,flags=re.I)
                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                        item=re.sub(r"[\s]+|\;|\(|\`","",item)
                        item=re.sub(r"\.[\s]+",".",item)
                        without_Append_project_id_Final_input_sql_statements.append(item.upper())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*",fileLine)
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

                # obj=re.sub(r'\.[\w]+\.','.',obj)
                # file_obj1=re.split(r"\;|^BEGIN[\s]+|^END[\s]+",obj)#obj.split(";")
                find_proc=re.findall(r'CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+[a-zA-Z0-9_\-\$\{\}]*\.*[a-zA-Z0-9_\-\$\{\}]+\.[a-zA-Z0-9_\-\$\{\}]+[\s]*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)',obj,flags=re.I|re.DOTALL)
                for proc in find_proc:
                    obj=obj.replace(proc,'')
                obj=re.sub(r'^[\s]*BEGIN','',obj,flags=re.I|re.DOTALL)
                find_temp_table=re.findall(r'CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+|CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+',obj,flags=re.I|re.DOTALL)
                for ele in find_temp_table:
                    find_table=re.findall(r'[\w]+$',ele,flags=re.I|re.DOTALL)[0]
                    if find_table.upper() != 'IF':
                        obj=obj.replace(ele,'CREATE TEMPORARY TABLE IF NOT EXISTS '+find_table)
                # print(find_temp_table)
                # obj=re.sub(r'CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+[a-zA-Z0-9_\-\$\{\}]*\.*[a-zA-Z0-9_\-\$\{\}]+\.[a-zA-Z0-9_\-\$\{\}]+[\s]*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)[\s]*BEGIN[\s]*','',obj,flags=re.I|re.DOTALL)
                file_obj1=obj.split(";")
                file_obj=[]
                for ele in file_obj1:
                    # print(ele)
                    # if "BEGIN" in ele.upper():
                    #     print(ele)
                    find_word=re.findall(r'[\w]+',ele)
                    if find_word:
                        file_obj.append(ele.strip())
                for element in file_obj:
                    element+=' '
                    # print(element)
                    if fun_project_ID == "Y":
                        # print(element)
                        obj_to_find=["TRUNCATE[\s]+TABLE","CALL","CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION","CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS","CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW","CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS","CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS","INSERT[\s]+INTO","UPDATE","DELETE[\s]+FROM","DROP[\s]+TABLE[\s]+IF[\s]+EXISTS","DROP[\s]+TABLE","ALTER[\s]+TABLE"]
                        generate_regex=''
                        for obj in obj_to_find:
                            generate_regex+="^[\s]*"+obj+'[\s]+\`*'+Regex_Project_Id+'\`*\.\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\$\s\;\,\)\(]|'
                            generate_regex+="^[\s]*"+obj+'[\s]+\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                        generate_regex=re.sub(r'\|$','',generate_regex)
                        generate_regex+="|^[\s]*SELECT[\s]+"
                        # print(generate_regex)
                        output_sql_statements=re.findall(r''+generate_regex,element,flags=re.I)
                        # output_sql_statements=re.findall(r""+generate_regex+"|"
                        #                                  r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|"    #CALL[\s]+[\w]+\.[\w]+|
                        #                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*\`[\w]+\.[\w]\`+"
                        #                                  r"|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\`|"
                        #                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+\`|INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]+INTO[\s]+\`[\w]+\`[\s]+|^UPDATE[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|^UPDATE[\s]+[\w]+|^UPDATE[\s]+\`[\w]+\`|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+FROM[\s]+\`[\w]+\`|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`[\w]+\`|TRUNCATE[\s]*TABLE[\s]*\`"+Append_project_id+"\`*\.[\w]+\.[\w]+|"
                        #                                  r"^[\s]*SELECT[\s]+",element,flags=re.I)
                        # print("OOOOOOO",output_sql_statements)
                        # if not output_sql_statements:
                        #     print("NNNN",element)
                        for item in output_sql_statements:
                            # print(item)
                            remove_space=re.sub(r"[\s]+","",item)
                            # remove_space=re.sub(r"[\s]+"," ",item)
                            if remove_space.upper()=="SELECT":
                                all_output_file_statements.append(element)
                            else:
                                all_output_file_statements.append(item.upper())
                            item=re.sub(r"\`","",item)
                            item=re.sub(r"[\s]+","",item)
                            item=re.sub(r''+Regex_Project_Id+'.','',item)
                            with_Append_project_id_Final_output_sql_statements.append(item.upper())
                    else:
                        output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|"
                                                         #r"CALL[\s]+[\w]+\.[\w]+|CALL[\s]+\`[\w]+\.[\w]+\`|"
                                                         r"CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*\`[\w]+\.[\w]+\`|"
                                                         r"^WITH[\s]+[\w]+|"
                                                         r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\`|"
                                                         r"CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`[\w]+\.[\w]+\`|"
                                                         r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\.[\w]+\`|CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\.[\w]+\`|"
                                                         r"INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`[\s]+|"
                                                         r"INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]+INTO[\s]+\`[\w]+\`[\s]+|"
                                                         r"UPDATE[\s]*[\w]+\.[\w]+|^UPDATE[\s]*\`[\w]+\.[\w]+\`|"
                                                         r"^UPDATE[\s]+[\w]+|^UPDATE[\s]+\`[\w]+\`|"
                                                         r"DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]*FROM[\s]*\`[\w]+\.[\w]+\`"
                                                         r"|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+FROM[\s]+\`[\w]+\`|"
                                                         r"DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`[\w]+\.[\w]+\`|"
                                                         r"DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`[\w]+\`|^[\s]*SELECT[\s]+",element,flags=re.I)
                        for item in output_sql_statements:
                            # print(item)
                            remove_space=re.sub(r"[\s]+","",item)
                            # remove_space=re.sub(r"[\s]+"," ",item)
                            if remove_space.upper()=="SELECT":
                                all_output_file_statements.append(element)
                            else:
                                all_output_file_statements.append(item.upper())
                            item=re.sub(r"\`","",item)
                            item=re.sub(r"[\s]+","",item)
                            without_Append_project_id_Final_output_sql_statements.append(item.upper())

        # with_Append_project_id_Final_input_sql_statements.sort()
        # with_Append_project_id_Final_output_sql_statements.sort()
        # without_Append_project_id_Final_input_sql_statements.sort()
        # without_Append_project_id_Final_output_sql_statements.sort()
        # print(without_Append_project_id_Final_input_sql_statements)
        # print(without_Append_project_id_Final_output_sql_statements)
        # print(len(without_Append_project_id_Final_input_sql_statements))
        # print(len(without_Append_project_id_Final_output_sql_statements))
        # print(filename)
        # print(with_Append_project_id_Final_input_sql_statements)
        # print(with_Append_project_id_Final_output_sql_statements)
        # print(len(with_Append_project_id_Final_input_sql_statements))
        # print(len(with_Append_project_id_Final_output_sql_statements))
        if flag==1:
            Result_No_Of_Statement.append("CM")
            log_result.append(Result_CM(".IF ACTIVITYCOUNT or .IF ERRORCODE is present in input file."))


        else:
            if fun_project_ID=="Y":
                if len(with_Append_project_id_Final_input_sql_statements)==0 and len(with_Append_project_id_Final_output_sql_statements)==0 :
                    Result_No_Of_Statement.append("NA")
                    log_result.append(Result_NA())
                elif len(with_Append_project_id_Final_input_sql_statements)>0:
                    if with_Append_project_id_Final_input_sql_statements==with_Append_project_id_Final_output_sql_statements:
                        Result_No_Of_Statement.append("Y")
                        log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
                    else:
                        Result_No_Of_Statement.append("N")
                        log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
                else:
                    Result_No_Of_Statement.append("N")
                    log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
            else:
                if len(without_Append_project_id_Final_input_sql_statements) ==0 and len(without_Append_project_id_Final_output_sql_statements)==0:
                    Result_No_Of_Statement.append("NA")
                    log_result.append(Result_NA())
                elif len(without_Append_project_id_Final_input_sql_statements)>0:
                    if without_Append_project_id_Final_input_sql_statements==without_Append_project_id_Final_output_sql_statements:
                        Result_No_Of_Statement.append("Y")
                        log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
                    else:
                        Result_No_Of_Statement.append("N")
                        log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
                else:
                    Result_No_Of_Statement.append("N")
                    log_result.append(Result_No(all_input_file_statements,all_output_file_statements))

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        Result_No_Of_Statement.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Pivot(log_result,filename,Number_Pivot_in_Script):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Number of TD_PIVOT in Script"))
    pivot_in_input_Script=[]
    Pivot_in_output_Script=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
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
                        if "--" in fileLine:

                            fileLine=fileLine.split("--")
                            fileLine=fileLine[0]
                        total_pivot=re.findall(r"TD\_PIVOT|TD\_UNPIVOT|UNPIVOT[\s]+\(*",fileLine,flags=re.I)
                        for item in total_pivot:
                            pivot_in_input_Script.append(item)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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

                    else:
                        if "--" in fileLine:

                            fileLine=fileLine.split("--")
                            fileLine=fileLine[0]
                        count_dml=re.findall(r"UNPIVOT[\s]*\(*|PIVOT[\s]+",fileLine,flags=re.I)
                        for item in count_dml:
                            Pivot_in_output_Script.append(item)

        if len(pivot_in_input_Script)==0 and len(Pivot_in_output_Script) == 0:
            Number_Pivot_in_Script.append("NA")
            log_result.append(Result_NA())
        elif len(pivot_in_input_Script)>0 and len(Pivot_in_output_Script)>0:
            if len(pivot_in_input_Script)==len(Pivot_in_output_Script):
                Number_Pivot_in_Script.append("Y")
                log_result.append(Result_Yes(pivot_in_input_Script,Pivot_in_output_Script))
            else:
                Number_Pivot_in_Script.append("N")
                log_result.append(Result_No(pivot_in_input_Script,Pivot_in_output_Script))
        else:
            Number_Pivot_in_Script.append("N")
            log_result.append(Result_No(pivot_in_input_Script,Pivot_in_output_Script))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Number_Pivot_in_Script.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Strtok_Function(log_result,filename,strtok_final_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check STRTOK Function"))
    input_strtok_all_column=[]
    final_strtok_column=[]
    output_strtok_all_column=[]
    result_for_strtok=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.read()
                strtok_lst=re.findall(r'(STRTOK(\(COALESCE)*\(.*?\)(.*?)\))',fileLine,flags=re.I|re.DOTALL)
                if strtok_lst:
                    #print(strtok_lst)
                    for i in strtok_lst:
                        input_strtok_all_column.append(i[0])
                        lst=re.sub(r'STRTOK|\(|COALESCE|SUBSTR','',i[0],flags=re.I)
                        split_col=lst.split(",")
                        #print(split_col[0])
                        find_table_name=re.findall(r"[\w]+(\.[\w]+)",split_col[0],flags=re.I|re.DOTALL)
                        find_table_name1=re.findall(r"[\w]+",split_col[0],flags=re.I|re.DOTALL)
                        if find_table_name:
                            final_strtok_column.append(find_table_name[0])
                        elif find_table_name1:
                            final_strtok_column.append(find_table_name1[0])

                final_strtok_column.sort()
        #print(final_strtok_column)
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.read()
                strtok_lst=re.findall(r'(SPLIT\((.*?)OFFSET(.*?)\])',fileLine,flags=re.I|re.DOTALL)
                strtok_lst.sort()
                for i in strtok_lst:
                    # print(i)
                    output_strtok_all_column.append(i[0])
                    for x in final_strtok_column:
                        # print(x)
                        if x.upper() in i[0].upper() and '[SAFE_OFFSET' in i[0].upper():
                            result_for_strtok.append("Y")
                        elif x.upper() in i[0].upper() and '[OFFSET' in i[0].upper():
                            result_for_strtok.append("N")
                if len(result_for_strtok) == 0:
                    strtok_final_result.append("NA")
                    log_result.append(Result_NA())
                elif 'N' in result_for_strtok:
                    strtok_final_result.append("N")
                    log_result.append(Result_No(input_strtok_all_column,output_strtok_all_column))
                else:
                    strtok_final_result.append("Y")
                    log_result.append(Result_Yes(input_strtok_all_column,output_strtok_all_column))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        strtok_final_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Hashrow_equivalent(log_result,filename,Hashrow_in_Script):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Hashrow equivalent function"))
    Hashrow_in_input_Script=[]
    Hashrow_in_output_Script=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    if fileLine.startswith("--"):
                        pass
                    else:
                        Total_Hashrow=re.findall(r"HASHROW",fileLine,flags=re.I)
                        for item in Total_Hashrow:
                            Hashrow_in_input_Script.append(item)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    if fileLine.startswith("--"):
                        pass
                    else:
                        count_sha1=re.findall(r"SHA1",fileLine,flags=re.I)
                        for item in count_sha1:
                            Hashrow_in_output_Script.append(item)

        if len(Hashrow_in_input_Script)==0 and len(Hashrow_in_output_Script) == 0:
            Hashrow_in_Script.append("NA")
            log_result.append(Result_NA())
        elif len(Hashrow_in_input_Script)>0 and len(Hashrow_in_output_Script)>0:
            if len(Hashrow_in_input_Script)==len(Hashrow_in_output_Script):
                Hashrow_in_Script.append("Y")
                log_result.append(Result_Yes(Hashrow_in_input_Script,Hashrow_in_output_Script))
            else:
                Hashrow_in_Script.append("N")
                log_result.append(Result_No(Hashrow_in_input_Script,Hashrow_in_output_Script))
        else:
            Hashrow_in_Script.append("N")
            log_result.append(Result_No(Hashrow_in_input_Script,Hashrow_in_output_Script))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Hashrow_in_Script.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Checking_BT_And_ET(log_result,filename,BT_ET_Result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking BT And ET"))
    input_BT_ET=[]
    final_BT_ET=[]
    output_BT_ET=[]
    final_output_BT_ET=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.read()
                find_BT_ET=re.findall(r'--% BT\;|--% ET\;',fileLine,flags=re.I|re.DOTALL)
                if find_BT_ET:
                    for i in find_BT_ET:
                        input_BT_ET.append(i)
                        replace_BT=re.sub(r"--% BT\;","BEGIN TRANSACTION;",i,flags=re.I)
                        replace_ET=re.sub(r"--% ET\;","COMMIT TRANSACTION;EXCEPTION WHEN ERROR THENROLLBACK TRANSACTION;",replace_BT,flags=re.I|re.DOTALL)
                        remove_space=re.sub(r"[\s]+","",replace_ET,flags=re.I)
                        final_BT_ET.append(remove_space)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.read()
                find_partten=re.findall(r'BEGIN[\s]+TRANSACTION\;|COMMIT[\s]*TRANSACTION\;[\s]*EXCEPTION[\s]*WHEN[\s]*ERROR[\s]*THEN[\s]*ROLLBACK[\s]*TRANSACTION\;',fileLine,flags=re.I|re.DOTALL)
                for i in find_partten:
                    remove_newline=i.replace("\n"," ")
                    output_BT_ET.append(remove_newline)
                    remove_space=re.sub(r"[\s]+","",remove_newline,flags=re.I)
                    final_output_BT_ET.append(remove_space)

        if len(final_BT_ET)==0:
            BT_ET_Result.append("NA")
            log_result.append(Result_NA())
        elif final_BT_ET == final_output_BT_ET:
            BT_ET_Result.append("Y")
            log_result.append(Result_Yes(input_BT_ET,output_BT_ET))

        else:
            BT_ET_Result.append("N")
            log_result.append(Result_No(input_BT_ET,output_BT_ET))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        BT_ET_Result.append("Encoding")
        log_result.append(Result_Encoding())

        print("Error in processing: ", filename)

def Checking_sys_calender(log_result,filename,sys_calender_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking SYS_CALENDAR"))
    input_sys_calender_log=[]
    output_sys_calender_log=[]
    final_input_sys_calender=[]
    final_output_sys_calender=[]
    try:
    # if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                find_SYS_CALENDAR=re.findall(r"SYS\_CALENDAR[\s]*\.[\s]*CALENDAR",fileLines,flags=re.I|re.DOTALL)
                for item in find_SYS_CALENDAR:
                    input_sys_calender_log.append(item.upper())
                    replace_sys_calender=re.sub(r"SYS\_CALENDAR\.","db_aedwd1.",item,flags=re.I|re.DOTALL)
                    remove_space=re.sub(r"[\s]+","",replace_sys_calender)
                    final_input_sys_calender.append(remove_space.upper())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                final_obj=""
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
                        if "--" in fileLine:

                            fileLine=fileLine.split("--")
                            fileLine=fileLine[0]
                            final_obj=final_obj+fileLine
                        else:
                            final_obj=final_obj+fileLine
                        # print(final_obj)

                find_db_aedwp1=re.findall(r"DB\_AEDWD1[\s]*\.[\s]*CALENDAR",final_obj,flags=re.I|re.DOTALL)
                for item in find_db_aedwp1:
                    output_sys_calender_log.append(item.upper())
                    remove_space=re.sub(r"[\s]+","",item)
                    final_output_sys_calender.append(remove_space.upper())

        if len(final_input_sys_calender)==0 and len(final_output_sys_calender)==0:
            sys_calender_result.append("NA")
            log_result.append(Result_NA())
        elif len(final_input_sys_calender)>0 and len(final_output_sys_calender)>0:
            if final_input_sys_calender==final_output_sys_calender:
                sys_calender_result.append("Y")
                log_result.append(Result_Yes(input_sys_calender_log,output_sys_calender_log))
            else:
                sys_calender_result.append("N")
                log_result.append(Result_No(input_sys_calender_log,output_sys_calender_log))
        else:
            sys_calender_result.append("N")
            log_result.append(Result_No(input_sys_calender_log,output_sys_calender_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        sys_calender_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_cross_join(log_result,filename,JoinResult,fun_project_ID):
    # print("yyy")
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Verifying Join Conditions"))
    inputCrossJoin=[]
    outputCrossJoin=[]
    input_cross_join_list=[]
    output_cross_join_list=[]
    input_cross_join_log=[]
    output_cross_join_log=[]
    from_to_where=''
    final_result="NA"
    file_obj=""
    word_regex="[a-zA-Z0-9_\-\$\{\}]+"
    join_type_lst=['CROSS[\s]+JOIN','LEFT[\s]+[\w]*[\s]*JOIN','RIGHT[\s]+[\w]*[\s]*JOIN','FULL[\s]+[\w]*[\s]*JOIN']
    Regex_Project_Id=Append_project_id
    special_char=['$','{','}']
    for i in special_char:
        Regex_Project_Id=Regex_Project_Id.replace(i,'\\'+i)
    # print(filename)
    try:
        if filename in inputFile:
            comment=0
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                #fileLines = f.read()
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                # fileLines = f.readlines()
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)

                    if "/*" in fileLine:
                        fileLine1=fileLine.split("/*")
                        file_obj=file_obj+fileLine1[0]
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
                        #print(fileLine)
                        pass
                    elif "--" in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]+'\n'
                    else:

                        file_obj=file_obj+fileLine
                # print(file_obj)
                input_join_regex=''
                input_join_regex='CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]*[\s]+WHERE|'
                for join in join_type_lst:
                    input_join_regex+=join+'[\s]+\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    input_join_regex+=join+'[\s]+\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    input_join_regex+=join+'[\s]*\([\s]*SELECT|'
                    input_join_regex+=join+'[\s]*\([\s]*SEL|'
                input_join_regex=re.sub(r'\|$','',input_join_regex)
                # input_cross=re.findall(r'CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]*[\s]+WHERE|CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|CROSS[\s]+JOIN[\s]+\([\s]*SEL|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]*\([\s]*SELECT|LEFT[\s]+JOIN[\s]*\([\s]*SEL|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+\([\s]*SEL|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SEL|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SEL',file_obj,flags=re.I|re.DOTALL)
                input_cross=re.findall(r''+input_join_regex,file_obj,flags=re.I|re.DOTALL)
                # print(input_cross)
                for ele in input_cross:
                    if ele.upper().endswith("WHERE"):
                        pass
                    else:
                        remove_space=re.sub(r'[\s]+OUTER[\s]+','',ele,flags=re.I|re.DOTALL)
                        remove_space=re.sub(r"[\s]+","",remove_space)
                        inputCrossJoin.append(remove_space.upper())
                        input_cross_join_log.append(ele)

                input_from_to_where=re.findall(r'FROM(.*)[WHERE]*\;*',file_obj,flags=re.I|re.DOTALL)
                if input_from_to_where:
                    for ele in input_from_to_where:
                        input_cross_join_log.append(ele.upper())
                        from_to_where=ele
                    remove_space_from_select=re.sub(r"\(SEL[\s]+","(SELECT ",input_from_to_where[0],flags=re.I)
                    remove_space_from_select=re.sub(r"\([\s]+SELECT","(SELECT",remove_space_from_select,flags=re.I)
                    #remove_space_from_select=re.sub(r"\([\s]+SELECT","(SELECT",input_from_to_where[0],flags=re.I)
                    remove_space=re.split("\,|\n|\t|[\s]+",remove_space_from_select)
                    for item in remove_space:
                        if item=='':
                            pass
                        else:
                            input_cross_join_list.append(item.upper())

        # print(input_cross_join_log)
        # print(input_cross_join_list)
        # print(filename)
        if filename in outputFile:
            comment=0
            file_obj=""
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                #fileLines = f.read()
                # fileLines = f.readlines()
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
                        #print(fileLine)
                        pass
                    elif "--" in fileLine and "' -- '" not  in fileLine:

                        fileLine=fileLine.split("--")
                        file_obj=file_obj+fileLine[0]+'\n'
                    else:

                        file_obj=file_obj+fileLine
                # print(file_obj)
                file_obj=re.sub(r"db_aedwd1.","SYS_CALENDAR.",file_obj,flags=re.I|re.DOTALL)
                output_join_regex=''

                for join in join_type_lst:
                    output_join_regex+=join+'[\s]+\`*'+Regex_Project_Id+'\`*\.\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    output_join_regex+=join+'[\s]+\`*'+word_regex+'\`*\.\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    output_join_regex+=join+'[\s]+\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    output_join_regex+=join+'[\s]*\([\s]*SELECT|'
                output_join_regex=re.sub(r'\|$','',output_join_regex)
                # output_cross=re.findall(r'CROSS[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]*\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT',file_obj,flags=re.I|re.DOTALL)
                output_cross=re.findall(r''+output_join_regex,file_obj,flags=re.I|re.DOTALL)
                # print(output_cross)
                for ele in output_cross:
                    remove_space=re.sub(r'[\s]+OUTER[\s]+','',ele,flags=re.I|re.DOTALL)
                    remove_space=re.sub(r"[\s]+","",remove_space)
                    if fun_project_ID=='Y':
                        remove_space=re.sub(Regex_Project_Id+".|\`","",remove_space,flags=re.I)
                        #print(remove_space)
                    outputCrossJoin.append(remove_space.upper())
                    output_cross_join_log.append(ele)

                inner_join_regex=''
                for join in ['INNER[\s]+JOIN']:
                    inner_join_regex+=join+'[\s]+\`*'+Regex_Project_Id+'\`*\.\`*'+word_regex+'\`*\.\`*'+word_regex+'[\s\;\,\)\(]|'
                    inner_join_regex+=join+'[\s]+\`*'+word_regex+'\`*\.\`*'+word_regex+'[\s\;\,\)\(]|'
                    inner_join_regex+=join+'[\s]+\`*'+word_regex+'\`*[\s\;\,\)\(]|'
                    inner_join_regex+=join+'[\s]*\([\s]*SELECT|'
                inner_join_regex=re.sub(r'\|$','',inner_join_regex)
                # output_comma_cross=re.findall(r"INNER[\s]+JOIN[\s]+\`*"+Append_project_id+"\.[\w]+\.[\w]+\`*[\s]+|INNER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|INNER[\s]+JOIN[\s]+[\w]+[\s]+|INNER[\s]+JOIN[\s]+\([\s]*SELECT[\s]+",file_obj,flags=re.I|re.DOTALL)
                output_comma_cross=re.findall(r""+inner_join_regex,file_obj,flags=re.I|re.DOTALL)
                # print(output_comma_cross)
                str1=""
                if len(output_comma_cross)==0:
                    if from_to_where=='':
                        pass
                    else:
                        input_cross_join_log.remove(from_to_where.upper())

                for ele_innerJoin in output_comma_cross:
                    str1=str1+" "+ele_innerJoin
                if str1=="":
                    pass
                else:
                    output_cross_join_log.append(str1)
                    output_comma_cross=' '.join(map(str, output_comma_cross)).upper().split('INNER JOIN')

                for ele_innerJoin in output_comma_cross:
                    remove_space=re.sub("[\s]+","",ele_innerJoin,flags=re.I)
                    if fun_project_ID == 'Y':
                        remove_space=re.sub(Regex_Project_Id+".|\`","",remove_space,flags=re.I)
                        #print(remove_space)

                    if remove_space=='':
                        pass
                    else:
                        output_cross_join_list.append(remove_space.upper())
        # print("II",input_cross_join_list)
        # print("OO",output_cross_join_list)
        if len(output_cross_join_list)==0:
            final_result="NA"
        else:
            # print(input_cross_join_list)
            for item in output_cross_join_list:
                if item in input_cross_join_list:
                    # print(item)
                    final_result="true"
                else:
                    # print(item)
                    final_result="false"
                    break
        inputCrossJoin.sort()
        outputCrossJoin.sort()
        # print(inputCrossJoin)
        # print(outputCrossJoin)
        # print(len(inputCrossJoin))
        # print(len(outputCrossJoin))
        # print(final_result)

        if len(inputCrossJoin)==0 and len(outputCrossJoin)==0 and final_result=="NA":
            JoinResult.append("NA")
            log_result.append(Result_NA())
        elif len(inputCrossJoin)>0 and len(outputCrossJoin)>0 or final_result!="false":
            # print(final_result)
            if len(inputCrossJoin) == len(outputCrossJoin) and final_result!="false":
                JoinResult.append("Y")
                log_result.append(Result_Yes(input_cross_join_log,output_cross_join_log))
            else:
                # print("NN")
                JoinResult.append("N")
                log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

        else:
            JoinResult.append("N")
            # print("MMM")
            log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        JoinResult.append("Encoding")
        log_result.append(Result_Encoding())
        print(e)
        print("Error in processing: ", filename)

def check_comment_for_TD(log_result,filename,comment_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Comments Checking"))
    input_log=[]
    input_comment_lst=[]
    output_log=[]
    output_comment_lst=[]
    comment=0
    try:
        if filename in inputFile:
            SQL_query=0
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            if fileLine.startswith(r" --% BT") or fileLine.startswith(r"--% BT") or fileLine.startswith(r"--% ET"):
                                pass
                            else:
                                input_log.append(fileLine)
                                fileLine=re.sub(r'[\s]+|\-|\%','',fileLine)
                                if fileLine=='':
                                    pass
                                else:
                                    input_comment_lst.append(fileLine.strip().upper())
                        else:
                            input_sql_statements=re.findall(r"CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|WITH[\s]+CTE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+|REPLACE[\s]+PROCEDURE[\s]+|CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|MERGE[\s]+|CREATE[\s]+SET[\s]+TABLE[\s]+|CREATE[\s]+MULTISET[\s]+TABLE[\s]+|CREATE[\s]+PROCEDURE[\s]+|CREATE[\s]+FUNCTION[\s]+|SEL[\s]+|DEL[\s]+|UPD[\s]+",fileLine,flags=re.I)
                            if input_sql_statements:
                                if ";" in fileLine:
                                    pass
                                else:
                                    SQL_query=1
                    if SQL_query==1:
                        if ";" in fileLine:
                            SQL_query=0

        if filename in outputFile:
            SQL_query=0
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                                output_comment_lst.append(fileLine.strip().upper())
                        else:
                            output_sql_statements=re.findall(r"CALL[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]+|^WITH[\s]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]+|CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|TRUNCATE[\s]+TABLE[\s]|CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE",fileLine,flags=re.I)
                            if output_sql_statements:
                                if ";" in fileLine:
                                    pass
                                else:
                                    SQL_query=1
                    if SQL_query==1:
                        if ";" in fileLine:
                            SQL_query=0


        if len(input_comment_lst)==0 and len(output_comment_lst)==0:
            comment_result.append("NA")
            log_result.append(Result_NA())

        elif input_comment_lst==output_comment_lst:
            comment_result.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            comment_result.append("N")
            log_result.append(Result_No(input_log,output_log))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        comment_result.append("Encoding")
        log_result.append(Result_Encoding())
        #print(e)
        print("Error in processing: ", filename)

def Check_View_Column_List(log_result,filename,View_Cloumn_List_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking View Cloumn List"))
    input_view_Dict={}
    output_view_Dict={}
    view_input_log=[]
    view_output_log=[]
    view_final_result='NA'
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            input_final_list=[]
                            find_view_in_line=re.findall(r"REPLACE[\s]+VIEW|CREATE[\s]+VIEW",fileLine,flags=re.I)

                            if find_view_in_line:

                                if find_view_in_line[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    view_input_log.append(obj)
                                    find_view_name=re.findall(r"VIEW[\s]+[\w]+\.([\w]+)",obj,flags=re.I|re.DOTALL)
                                    find_Alias=re.findall(r"\((.*)\)",obj,flags=re.DOTALL)
                                    if find_Alias:
                                        for item in find_Alias:
                                            remove_space=re.sub(r"[\s]+","",item)
                                            Alias_column=remove_space.split(",")
                                    find_Cloumns=re.findall(r"SELECT(.*)FROM",obj,flags=re.DOTALL|re.I)
                                    if find_Cloumns:
                                        for item in find_Cloumns:
                                            remove_space=re.sub(r"[\s]+","",item)
                                            view_column=remove_space.split(",")

                                    for i in range(0,len(Alias_column)):
                                        if view_column[i] != Alias_column[i]:
                                            input_column_list=view_column[i]+" AS "+Alias_column[i]
                                        else:
                                            input_column_list=view_column[i]

                                        remove_space=re.sub(r"[\s]+","",input_column_list)
                                        input_final_list.append(remove_space.upper())
                                        input_final_list.sort()

                                    input_view_Dict[find_view_name[0].upper()]=input_final_list
                                else:
                                    obj=obj+fileLine

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            find_view_in_line=re.findall(r"CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW|REPLACE[\s]+VIEW|CREATE[\s]+VIEW",fileLine,flags=re.I)
                            if find_view_in_line:
                                if find_view_in_line[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():

                                    counter=0
                                    obj=obj+fileLine
                                    view_output_log.append(obj)
                                    find_view_name=re.findall(r"VIEW[\s]+[\w]+\.([\w]+)",obj,flags=re.I|re.DOTALL)
                                    find_Cloumns=re.findall(r"SELECT(.*)FROM",obj,flags=re.DOTALL|re.I)
                                    if find_Cloumns:
                                        for item in find_Cloumns:
                                            remove_space=re.sub(r"[\s]+","",item.upper())
                                            view_column=remove_space.split(",")
                                            view_column.sort()
                                    output_view_Dict[find_view_name[0].upper()]=view_column

                                else:
                                    obj=obj+fileLine

        for key in input_view_Dict:
            if key in output_view_Dict:
                if len(input_view_Dict[key])==0 and len(output_view_Dict[key]) == 0:
                    pass

                elif len(input_view_Dict[key]) > 0 and len(output_view_Dict[key]) > 0:
                    if input_view_Dict[key]== output_view_Dict[key]:
                        view_final_result='Y'
                    else:
                        view_final_result='N'
                        break
                else:
                    view_final_result='N'
                    break
            else:
                view_final_result='N'
                break

        if view_final_result=='N':
            View_Cloumn_List_result.append("N")
            log_result.append(Result_No(view_input_log,view_output_log))
        elif view_final_result=='NA':
            View_Cloumn_List_result.append("NA")
            log_result.append(Result_NA())
        elif view_final_result=='Y':
            View_Cloumn_List_result.append("Y")
            log_result.append(Result_Yes(view_input_log,view_output_log))
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        View_Cloumn_List_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Hard_Coded_Value(log_result,filename,Hard_Coded_Value_Result):
    #print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Verifying Hard Coded Value"))
    input_hard_coded_log=[]
    input_hard_coded_list=[]
    output_hard_coded_log=[]
    output_hard_coded_list=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                counter=0
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
                    else:
                        find_hard_coded_value=re.findall(r"\'[\w]+\'|\'\~\'|\'\$\{[\w]+\}\'|"
                                                         r"\'\^\^[\d]+\'|\'[\w]+\%\'|\'\%[\w]+\%\'|"
                                                         r"\'\%[\w]+\'|\'\^\^\{[\w]+\}\'|"
                                                         r"\'[\w]+\$\{[\w]+\}\.[\w]+\'|\'\-\([\d]+\)\'|\'\-[\d]+\'|"
                                                         r"\'[\d]+\'|\'\([\d]+\)\'|\'[\w.+-]+@[\w-]+\.[\w.-]+\'|"
                                                         r"\'[\w]+\.[\w]+\'|\'[\w]*[\s]*\$*\{*[\w]*\}*\@*[\w]*\@*[\w]*\@*[\w]*\@*[\w]*\$*\{*[\w]*\}*\.*[\w]*\'|"
                                                         r"\'[\s]*[\w]+[\s]*\'",fileLine,flags=re.I)
                        if find_hard_coded_value:
                            #print(find_hard_coded_value)
                            for item in find_hard_coded_value:
                                input_hard_coded_log.append(item)
                                item=re.sub(r"[\s]+","",item,flags=re.I)
                                input_hard_coded_list.append(item)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
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
                    else:
                        find_hard_coded_value=re.findall(r"\'[\w]+\'|\'\~\'|\'\$\{[\w]+\}\'|"
                                                         r"\'\^\^[\d]+\'|\'[\w]+\%\'|\'\%[\w]+\%\'|"
                                                         r"\'\%[\w]+\'|\'\^\^\{[\w]+\}\'|"
                                                         r"\'[\w]+\$\{[\w]+\}\.[\w]+\'|\'\-\([\d]+\)\'|\'\-[\d]+\'|"
                                                         r"\'[\d]+\'|\'\([\d]+\)\'|\'[\w.+-]+@[\w-]+\.[\w.-]+\'|"
                                                         r"\'[\w]+\.[\w]+\'|\'[\w]*[\s]*\$*\{*[\w]*\}*\@*[\w]*\@*[\w]*\@*[\w]*\@*[\w]*\$*\{*[\w]*\}*\.*[\w]*\'|"
                                                         r"\'[\s]*[\w]+[\s]*\'",fileLine,flags=re.I)
                        if find_hard_coded_value:
                            for item in find_hard_coded_value:
                                output_hard_coded_log.append(item)
                                item=re.sub(r"[\s]+","",item,flags=re.I)
                                output_hard_coded_list.append(item)
        input_hard_coded_list.sort()
        output_hard_coded_list.sort()

        # print(input_hard_coded_list)
        # print(output_hard_coded_list)

        if input_hard_coded_list==0 and output_hard_coded_list==0:
            log_result.append(Result_NA())
            Hard_Coded_Value_Result.append("NA")
        elif input_hard_coded_list==output_hard_coded_list:
            log_result.append(Result_Yes(input_hard_coded_log,output_hard_coded_log))
            Hard_Coded_Value_Result.append("Y")
        else:
            log_result.append(Result_No(input_hard_coded_log,output_hard_coded_log))
            Hard_Coded_Value_Result.append("N")




    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Hard_Coded_Value_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def not_between(log_result,filename,not_between_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking NOT BETWEEN"))
    input_log=[]
    output_log=[]
    input_final_lst=[]
    output_final_lst=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                find_not_between=re.findall(r'[\s]+NOT[\s]+BETWEEN[\s]+[\d]+[\s]+AND[\s]+[\d]+|'
                                            r'[\s]+NOT[\s]+BETWEEN[\s]+[\w]+[\s]+AND[\s]+[\w]+|'
                                            r'[\s]+NOT[\s]+BETWEEN[\s]+',fileLines,flags=re.I|re.DOTALL)
                # print(find_not_between)
                for ele in find_not_between:
                    if ele=='':
                        pass
                    else:
                        input_log.append(ele)
                        ele=re.sub(r'[\s]+','',ele)
                        input_final_lst.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                find_not_between=re.findall(r'[\s]+NOT[\s]+BETWEEN[\s]+[\d]+[\s]+AND[\s]+[\d]+|'
                                            r'[\s]+NOT[\s]+BETWEEN[\s]+[\w]+[\s]+AND[\s]+[\w]+|'
                                            r'[\s]+NOT[\s]+BETWEEN[\s]+',fileLines,flags=re.I|re.DOTALL)
                # print(find_not_between)
                for ele in find_not_between:
                    if ele=='':
                        pass
                    else:
                        output_log.append(ele)
                        ele=re.sub(r'[\s]+','',ele)
                        output_final_lst.append(ele.lower())


        if len(input_final_lst)==0 and len(output_final_lst)==0:
            not_between_result.append("NA")
            log_result.append(Result_NA())
        elif input_final_lst==output_final_lst:
            not_between_result.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            not_between_result.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        log_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Distinct(log_result,filename,Check_Distinct_Result,DB_Conection,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Distinct"))
    final_result='NA'
    output_log=[]
    output_metadata_log=[]
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            find_insert_and_update=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|UPDATE[\s]+[\w]+\.[\w]+|"
                                                              r"INSERT[\s]+INTO[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+",fileLine,flags=re.I)
                            if find_insert_and_update:
                                if find_insert_and_update[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    #print(obj)
                                    if fun_project_ID !="Y":
                                        find_schema_and_table_name=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|UPDATE[\s]+[\w]+\.[\w]+",obj,flags=re.I)
                                        remove_insert_and_update=re.sub(r"Insert[\s]+into[\s]+|update[\s]+","",find_schema_and_table_name[0],flags=re.I)
                                        find_From=re.findall(r"SELECT(.*)EXCEPT[\s]+DISTINCT",obj,flags=re.I|re.DOTALL)
                                        cursor = DB_Conection.cursor()
                                        cursor.execute("select schema||'.'||name as table_name from table_e WHERE lower(table_type)=lower('SET') and lower(table_name)=lower(?)",(remove_insert_and_update,))
                                        rows = cursor.fetchall()
                                        if len(rows)!=0:
                                            if final_result!='N':
                                                for row in rows:
                                                    output_log.append(obj)
                                                    if remove_insert_and_update==row[0]:
                                                        if find_From:
                                                            if 'FROM ' in find_From[0].upper():
                                                                if 'SELECT DISTINCT' in obj.upper() and "EXCEPT DISTINCT" in obj.upper():
                                                                    final_result='Y'
                                                                else:
                                                                    final_result='N'
                                                                    break
                                                            else:
                                                                if "EXCEPT DISTINCT" in obj.upper():
                                                                    final_result='Y'
                                                                else:
                                                                    final_result='N'
                                                                    break
                                                        else:
                                                            final_result='N'
                                                            break
                                        else:
                                            output_metadata_log.append(remove_insert_and_update)
                                            final_result='Metadata'
                                            break
                                    else:
                                        find_schema_and_table_name=re.findall(r"INSERT[\s]+INTO[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+ ",obj,flags=re.I)
                                        remove_insert_and_update=re.sub(r"Insert[\s]+into[\s]+\`"+Append_project_id+"\.""|update[\s]+\`"+Append_project_id+"\.""","",find_schema_and_table_name[0],flags=re.I)
                                        find_From=re.findall(r"SELECT(.*)EXCEPT[\s]+DISTINCT",obj,flags=re.I|re.DOTALL)
                                        cursor = DB_Conection.cursor()
                                        cursor.execute("select schema||'.'||name as table_name from table_e WHERE lower(table_type)=lower('SET') and lower(table_name)=lower(?)",(remove_insert_and_update,))
                                        rows = cursor.fetchall()
                                        if len(rows)!=0:
                                            if final_result!='N':
                                                for row in rows:
                                                    output_log.append(obj)
                                                    if remove_insert_and_update==row[0]:
                                                        if find_From:
                                                            if 'FROM ' in find_From[0].upper():
                                                                if 'SELECT DISTINCT' in obj.upper() and "EXCEPT DISTINCT" in obj.upper():
                                                                    final_result='Y'
                                                                else:
                                                                    final_result='N'
                                                                    break
                                                            else:
                                                                if "EXCEPT DISTINCT" in obj.upper():
                                                                    final_result='Y'
                                                                else:
                                                                    final_result='N'
                                                                    break
                                                        else:
                                                            final_result='N'
                                                            break
                                        else:
                                            output_metadata_log.append(remove_insert_and_update)
                                            final_result='Metadata'
                                            break

                                else:
                                    obj=obj+fileLine
                if final_result=="N":
                    Check_Distinct_Result.append("N")
                    log_result.append(Output_Result_No(output_log))
                elif final_result=="NA":
                    Check_Distinct_Result.append("NA")
                    log_result.append(Result_NA())
                elif final_result=="Y":
                    Check_Distinct_Result.append("Y")
                    log_result.append(Output_Result_Yes(output_log))
                else:
                    Check_Distinct_Result.append("Metadata")
                    log_result.append(Result_Metadata(output_metadata_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Check_Distinct_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Default_Coloumn_Values(log_result,filename,Default_Coloumn_Values_Result,DB_Conection,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Default_Coloumn_Values"))
    final_result='NA'
    input_log=[]
    output_log=[]
    output_metadata_log=[]
    key_values={}
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            find_insert_and_update=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|"
                                                              r"INSERT[\s]+INTO[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+",fileLine,flags=re.I)
                            if find_insert_and_update:
                                if find_insert_and_update[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        input_log.append(obj)
                                        find_schema_and_table_name=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+",obj,flags=re.I)
                                        remove_insert=re.sub(r"Insert[\s]+into[\s]+","",find_schema_and_table_name[0],flags=re.I)
                                        cursor = DB_Conection.cursor()
                                        cursor.execute("SELECT schema||'.'||object_name as table_name,name,default_value FROM 'column_e' where table_name=lower(?) AND default_value is not NULL",(remove_insert,))
                                        rows = cursor.fetchall()
                                        if len(rows)!=0:
                                            if final_result!='N':
                                                for row in rows:
                                                    find_columns=re.findall(r""+find_schema_and_table_name[0]+"(.*)VALUES",obj,flags=re.I|re.DOTALL)
                                                    if row[1] in find_columns[0]:
                                                        pass
                                                    else:
                                                        key_values[remove_insert]=[row[1],row[2]]
                                        else:
                                            output_metadata_log.append(remove_insert)
                                            final_result='Metadata'
                                            break

                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    input_log.append(obj)
                                    find_schema_and_table_name=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+",obj,flags=re.I)
                                    remove_insert=re.sub(r"Insert[\s]+into[\s]+","",find_schema_and_table_name[0],flags=re.I)
                                    cursor = DB_Conection.cursor()
                                    cursor.execute("SELECT schema||'.'||object_name as table_name,name,default_value FROM 'column_e' where table_name=lower(?) AND default_value is not NULL",(remove_insert,))
                                    rows = cursor.fetchall()
                                    if len(rows)!=0:
                                        if final_result!='N':
                                            for row in rows:
                                                find_columns=re.findall(r""+find_schema_and_table_name[0]+"(.*)VALUES",obj,flags=re.I|re.DOTALL)
                                                if row[1] in find_columns[0]:
                                                    pass
                                                else:
                                                    key_values[remove_insert]=[row[1],row[2]]

                                    else:
                                        output_metadata_log.append(remove_insert)
                                        final_result='Metadata'
                                        break


                                else:
                                    obj=obj+fileLine

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            find_insert_and_update=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|"
                                                              r"INSERT[\s]+INTO[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+",fileLine,flags=re.I)
                            if find_insert_and_update:
                                if find_insert_and_update[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        output_log.append(obj)
                                        for key,value in key_values.items():
                                            if fun_project_ID !="Y":
                                                insert_statement="INSERT INTO "+key.upper()+" "
                                            else:
                                                insert_statement="INSERT INTO `"+Append_project_id.upper()+"."+key.upper()+"` "
                                            if insert_statement in obj.upper():
                                                find_columns=re.findall(r""+insert_statement+"(.*)VALUES|"+""+insert_statement+"(.*)\)[\s]*SELECT",obj,flags=re.I|re.DOTALL)
                                                for item in find_columns:
                                                    if item=="":
                                                        pass
                                                    else:
                                                        for ele in item:
                                                            if ele=="":
                                                                pass
                                                            else:
                                                                column_list=re.split("[\s]+|\,|\(|\)",ele.upper())
                                                                if value[0].upper() in column_list:
                                                                    find_values=re.findall(r"VALUES(.*)\;|\)[\s]*SELECT(.*)\;",obj,flags=re.I|re.DOTALL)
                                                                    for item in find_values:
                                                                        if item=="":
                                                                            pass
                                                                        else:
                                                                            for ele in item:
                                                                                if ele=="":
                                                                                    pass
                                                                                else:
                                                                                    default_value_list=re.split("[\s]+|\,|\(|\)|\'",ele.upper())
                                                                                    if value[1].upper() in default_value_list:
                                                                                        final_result="Y"
                                                                                    else:
                                                                                        final_result='N'
                                                                else:
                                                                    final_result="N"
                                                                    break

                                        counter=0

                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    output_log.append(obj)
                                    for key,value in key_values.items():
                                        if fun_project_ID !="Y":
                                            insert_statement="INSERT INTO "+key.upper()+" "
                                        else:
                                            insert_statement="INSERT INTO `"+Append_project_id.upper()+"."+key.upper()+"` "
                                        if insert_statement in obj.upper():
                                            find_columns=re.findall(r""+insert_statement+"(.*)VALUES|"+""+insert_statement+"(.*)\)[\s]*SELECT",obj,flags=re.I|re.DOTALL)
                                            for item in find_columns:
                                                if item=="":
                                                    pass
                                                else:
                                                    for ele in item:
                                                        if ele=="":
                                                            pass
                                                        else:
                                                            column_list=re.split("[\s]+|\,|\(|\)",ele.upper())
                                                            if value[0].upper() in column_list:
                                                                find_values=re.findall(r"VALUES(.*)\;|\)[\s]*SELECT(.*)\;",obj,flags=re.I|re.DOTALL)
                                                                for item in find_values:
                                                                    if item=="":
                                                                        pass
                                                                    else:
                                                                        for ele in item:
                                                                            if ele=="":
                                                                                pass
                                                                            else:
                                                                                default_value_list=re.split("[\s]+|\,|\(|\)|\'",ele.upper())
                                                                                if value[1].upper() in default_value_list:
                                                                                    final_result="Y"
                                                                                else:
                                                                                    final_result='N'
                                                            else:
                                                                final_result="N"
                                                                break

                                else:
                                    obj=obj+fileLine
                if final_result=="N":
                    Default_Coloumn_Values_Result.append("N")
                    log_result.append(Result_No(input_log,output_log))
                elif final_result=="NA":
                    Default_Coloumn_Values_Result.append("NA")
                    log_result.append(Result_NA())
                elif final_result=="Y":
                    Default_Coloumn_Values_Result.append("Y")
                    log_result.append(Result_Yes(input_log,output_log))
                else:
                    Default_Coloumn_Values_Result.append("Metadata")
                    log_result.append(Result_Metadata(output_metadata_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Default_Coloumn_Values_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Unique_Primary_Index(log_result,filename,Unique_Primary_Index_Result,fun_Unique_Primary_Index_Cloumn_FileList_Path):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Unique_Primary_Index"))
    final_result="NA"
    output_log=[]
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
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
                    elif fileLine.startswith("--"):
                        pass
                    else:
                        if counter==0:
                            obj=''
                            find_insert_and_update=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|"
                                                              r"INSERT[\s]+INTO[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+",fileLine,flags=re.I)
                            if find_insert_and_update:
                                if find_insert_and_update[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    #print(obj)
                                    output_log.append(obj)
                                    fileList_cloumn_list=[]
                                    final_output_list=[]
                                    with open(fun_Unique_Primary_Index_Cloumn_FileList_Path, "r",encoding=e) as f:
                                        fileList = f.readlines()
                                        for table_name_list in fileList:
                                            find_table_name_from_file=re.findall(r"INTO[\s]+([\w]+\.[\w]+)",obj,flags=re.I)
                                            find_table_name_from_filelist=re.findall(r"[\w]+\.[\w]+",table_name_list,flags=re.I)
                                            for item in find_table_name_from_file:
                                                for element in find_table_name_from_filelist:
                                                    if item.upper() == element.upper():
                                                        # print(item)
                                                        find_column_form_list=re.findall(r"\[(.*)\]",table_name_list,flags=re.I)
                                                        if find_column_form_list:
                                                            column_list=re.split(r"\,|\'",find_column_form_list[0])
                                                            for item in column_list:
                                                                if " " in item or "''" in item or item.strip()=='':
                                                                    pass
                                                                else:
                                                                    fileList_cloumn_list.append(item.upper())
                                        # print(len(fileList_cloumn_list))
                                        if len(fileList_cloumn_list)>0:
                                            fileList_cloumn_list.append("RN=1")
                                    find_cloumn_in_file=re.findall(r"ROW\_NUMBER\(\)[\s]+OVER\(PARTITION[\s]+BY[\s]*(.*)\)[\s]*RN[\s]*FROM[\s]*\(",obj,flags=re.I|re.DOTALL)
                                    if find_cloumn_in_file:
                                        outputFile_cloumn=re.split(r"\,|[\s]+",find_cloumn_in_file[0])
                                        for item in outputFile_cloumn:
                                            if " " in item or "''" in item or item.strip()=='':
                                                pass
                                            else:
                                                final_output_list.append(item.upper())

                                    find_rn=re.findall(r"where[\s]+(RN[\s]*\=[\s]*1)",obj,flags=re.I)
                                    if find_rn:
                                        remove_space=re.sub(r"[\s]+","",find_rn[0])
                                        final_output_list.append(remove_space.upper())
                                    # print(filename)
                                    #
                                    # print(fileList_cloumn_list)
                                    # print(final_output_list)
                                    if len(fileList_cloumn_list)>0 or len(final_output_list)>0:
                                        if fileList_cloumn_list == final_output_list:
                                            final_result="Y"
                                        else:
                                            final_result="N"
                                            break
                                else:
                                    obj=obj+fileLine
                if final_result=="N":
                    Unique_Primary_Index_Result.append("N")
                    log_result.append(Output_Result_No(output_log))
                elif final_result=="NA":
                    Unique_Primary_Index_Result.append("NA")
                    log_result.append(Result_NA())
                else:
                    Unique_Primary_Index_Result.append("Y")
                    log_result.append(Output_Result_Yes(output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Unique_Primary_Index_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def COALESCE(log_result,filename,Coalesce_Result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking COALESCE"))
    COALESCE_in_input_Script=[]
    COALESCE_in_output_Script=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
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
                        if "--" in fileLine:

                            fileLine=fileLine.split("--")
                            fileLine=fileLine[0]
                        total_pivot=re.findall(r"COALESCE",fileLine,flags=re.I)
                        for item in total_pivot:
                            COALESCE_in_input_Script.append(item.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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

                    else:
                        if "--" in fileLine:

                            fileLine=fileLine.split("--")
                            fileLine=fileLine[0]
                        count_dml=re.findall(r"COALESCE",fileLine,flags=re.I)
                        for item in count_dml:
                            COALESCE_in_output_Script.append(item.lower())
        # print(COALESCE_in_input_Script)
        # print(COALESCE_in_output_Script)
        # print(len(COALESCE_in_input_Script))
        # print(len(COALESCE_in_output_Script))
        if len(COALESCE_in_input_Script)==0 and len(COALESCE_in_output_Script) == 0:
            Coalesce_Result.append("NA")
            log_result.append(Result_NA())
        elif len(COALESCE_in_input_Script)>0 and len(COALESCE_in_output_Script)>0:
            if COALESCE_in_input_Script==COALESCE_in_output_Script:
                Coalesce_Result.append("Y")
                log_result.append(Result_Yes(COALESCE_in_input_Script,COALESCE_in_output_Script))
            else:
                Coalesce_Result.append("N")
                log_result.append(Result_No(COALESCE_in_input_Script,COALESCE_in_output_Script))
        else:
            Coalesce_Result.append("N")
            log_result.append(Result_No(COALESCE_in_input_Script,COALESCE_in_output_Script))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Coalesce_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Exceptions(log_result,filename,check_Exception_Result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Checking Exception"))
    Exception=[]
    final_output_Exception=[]
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.read()
                find_partten=re.findall(r'BEGIN[\s]+TRANSACTION\;|COMMIT[\s]*TRANSACTION\;[\s]*EXCEPTION[\s]*WHEN[\s]*ERROR[\s]*THEN[\s]*ROLLBACK[\s]*TRANSACTION\;',fileLine,flags=re.I|re.DOTALL)
                for i in find_partten:
                    remove_newline=i.replace("\n"," ")
                    Exception.append(remove_newline)
                    remove_space=re.sub(r"[\s]+","",remove_newline,flags=re.I)
                    final_output_Exception.append(remove_space)
        # print(Exception)
        # print(final_output_Exception)
        if len(final_output_Exception)>0:
            check_Exception_Result.append("CM")
            log_result.append(Result_CM("BEGIN TRANSACTION and COMMIT TRANSACTION is present in input file."))
        else:
            check_Exception_Result.append("NA")
            log_result.append(Result_NA())





    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        check_Exception_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def System_Tables(log_result,filename,check_System_Table_Result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("System Table"))
    sys_table_in_input_Script=[]
    final_input_list=[]
    sys_table_in_output_Script=[]
    final_output_list=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--%[\s]+\#',fileLine)
                    if cmt:
                        pass
                    else:
                        sys_table=re.findall(r"dbc.tables[\s]+|dbc.tablesv[\s]+|dbc.columns[\s]+|dbc.columnsv[\s]+",fileLine,flags=re.I)
                        for item in sys_table:
                            sys_table_in_input_Script.append(item)
                            # if "DBC.TABLES " in item.upper():
                            item=re.sub(r"dbc.tables[\s]+|dbc.tablesv[\s]+",".INFORMATION_SCHEMA.TABLES",item,flags=re.I)
                            item=re.sub(r"dbc.columns[\s]+|dbc.columnsv[\s]+",".INFORMATION_SCHEMA.COLUMNS",item,flags=re.I)
                            final_input_list.append(item)
        # print(sys_table_in_input_Script)
        # print(final_input_list)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--%[\s]+\#',fileLine)
                    if cmt:
                        pass
                    else:

                        if counter==0:
                            obj=''
                            findselect=re.findall(r"^SELECT ",fileLine,flags=re.I)
                            if findselect:
                                if findselect[0].upper() in fileLine.upper():
                                    counter=1
                                    obj=obj+fileLine
                                    if ";" in fileLine.upper():
                                        counter=0
                        else:
                            if counter==1 :
                                if ";" in fileLine.upper():
                                    counter=0
                                    obj=obj+fileLine
                                    # print(obj)
                                    find_sys_table=re.findall(r"\.\`*INFORMATION\_SCHEMA\.TABLES|\.\`*INFORMATION\_SCHEMA\.COLUMNS",obj,flags=re.I)
                                    for item in find_sys_table:
                                        sys_table_in_output_Script.append(item)
                                        remove_tick=re.sub(r"[\s]+|\`","",item)
                                        final_output_list.append(remove_tick)

                                else:
                                    obj=obj+fileLine


        # print(sys_table_in_output_Script)
        # print(final_output_list)
        if len(final_input_list)==0:
            check_System_Table_Result.append("NA")
            log_result.append(Result_NA())
        elif len(final_input_list)>0 or len(final_output_list)>0:
            if final_input_list==final_output_list:
                check_System_Table_Result.append("Y")
                log_result.append(Result_Yes(sys_table_in_input_Script,sys_table_in_output_Script))
            else:
                check_System_Table_Result.append("N")
                log_result.append(Result_No(sys_table_in_input_Script,sys_table_in_output_Script))
        else:
            check_System_Table_Result.append("N")
            log_result.append(Result_No(sys_table_in_input_Script,sys_table_in_output_Script))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        check_System_Table_Result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Table_Column_Case_Check(log_result,filename,result_CS_Col_Name_Case):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check CS Table Column Names Case"))
    col_in_input_Script=[]
    final_input_list=[]
    col_in_output_Script=[]
    final_output_list=[]
    try:
        if filename in inputFile:
            comment=0
            table_counter=0
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                final_table_obj=''
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
                            # print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    elif "--" in fileLine:
                        fileLine=fileLine.split("--")
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+SET[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    table_obj=re.sub('[0-9]+\.[0-9]+','',table_obj,flags=re.I)
                    table_obj=re.sub(r'COMPRESS[\s]*\(.*?\)','',table_obj,flags=re.I|re.DOTALL)
                    find_char=re.findall(r"[\w]+[\s]+STRING[\s]*\,*|[\w]+[\s]+BIGINT[\s]*\,*|[\w]+[\s]+SMALLINT[\s]*\,*|[\w]+[\s]+BYTEINT[\s]*\,*|[\w]+[\s]+NUMBER[\s]*\([\d]+\)|[\w]+[\s]+NUMBER\(\d+\,\d+\)|[\w]+[\s]+TIMESTAMP[\s]*\([\d]+\)|[\w]+[\s]+TIMESTAMP|[\w]+[\s]+VARCHAR[\s]*\([\d]+\)|[\w]+[\s]+CHAR[\s]*\([\d]+\)|[\w]+[\s]+BYTES[\s]*\([\d]+\)|[\w]+[\s]+BYTES[\s]*\,*|[\w]+[\s]+BYTE[\s]*\([\d]+\)|[\w]+[\s]+BYTE[\s]*\,*|[\w]+[\s]+BLOB[\s]*\([\d]+\)|[\w]+[\s]+BLOB[\s]*\,*|[\w]+[\s]+VARCHAR[\s]+|[\w]+[\s]+VARCHAR\)|[\w]+[\s]+VARCHAR\,|[\w]+[\s]+CHAR[\s]+|[\w]+[\s]+CHAR\,|[\w]+[\s]+INTEGER\,*|[\w]+[\s]+CHAR\)|[\w]+[\s]+DECIMAL\(\d+\,\d+\)|[\w]+[\s]+DECIMAL\(\d+\)|[\w]+[\s]+DECIMAL|[\w]+[\s]+FLOAT[\s]*\([\d]+\)|[\w]+[\s]+FLOAT|[\w]+[\s]+DATETIME|[\w]+[\s]+DATE|[\w]+[\s]+DATE[\s]*FORMAT|\,[\s]*[\w]+\.[\w]+[\s]*\,|\,[\s]+[\w]+[\s]+[\w]+\,|[\s]+AS[\s]+[\w]+[\s]*\,|\([\w]+\.[\w]+\)|\([\w]+\.[\w]+\[\s]+",table_obj,flags=re.I)
                    #AS[\s]+[\w]+[\s]*\)*[\s]*AS[\s]*[\w]+|AS[\s]+[\w]+[\s]*\)[\s]*[\w]+|
                    # print("find_char", len(find_char),find_char)
                    for ele in find_char:
                        ele=re.sub(r"\([\d]+\)|\([\d]+\,[\d]+\)","",ele,flags=re.I)
                        if "and " in ele.lower() or "between " in ele.lower() or "no fallback" in ele.lower() or "no case" in ele.lower() or "by date" in ele.lower() or "between date" in ele.lower() or " by string" in ele.lower() or " by varchar" in ele.lower() or " as varchar" in ele.lower() or ele.lower()=="by date" or ele.lower()=="default date" or ele.lower()=="default timestamp" or ele.lower()=="by string"  or ele.lower()=="by integer" or  ele.lower()=="on date"or ele.lower()=="on string" or ele.lower()=="on integer"  or ele.lower()=="as date"  or ele.lower()=="as string"  or ele.lower()=="as integer"  or ele.lower()=="and date"  or ele.lower()=="and string"  or ele.lower()=="and integer":
                            pass
                        else:
                            # print(ele)   #AS[\s]+[\w]+[\s]*\)*[\s]*AS[\s]*[\w]+|AS[\s]+[\w]+[\s]*\)[\s]*[\w]+|
                            ele=re.sub(r"[\s]+STRING[\s]*\,*|[\s]+BIGINT[\s]*\,*|[\s]+SMALLINT[\s]*\,*|[\s]+BYTEINT[\s]*\,*|[\s]+NUMBER[\s]+|[\s]+NUMBER[\s]*\,|[\s]+TIMESTAMP[\s]*\([\d]+\)|[\s]+TIMESTAMP|[\s]+VARCHAR[\s]*\([\d]+\)|[\s]+CHAR[\s]*\([\d]+\)|[\s]+CHAR[\s]*|[\s]+BYTES[\s]*\([\d]+\)|[\s]+BYTES[\s]*\,*|[\s]+BYTE[\s]*\([\d]+\)|[\s]+BYTE[\s]*\,*|[\s]+BLOB[\s]*\([\d]+\)|[\s]+BLOB[\s]*\,*|[\s]+VARCHAR[\s]*|[\s]+VARCHAR\)|[\s]+VARCHAR\,*|[\s]+CHAR[\s]+|[\s]+CHAR\,|[\s]+INTEGER\,*|[\s]+CHAR\)|[\s]+DECIMAL\(\d+\,\d+\)|[\s]+DECIMAL\([\d]+\)|[\s]+DECIMAL|[\s]+FLOAT[\s]*\([\d]+\)|[\s]+FLOAT|[\s]+DATETIME|[\s]+DATE|[\s]+DATE[\s]*FORMAT","",ele,flags=re.I)
                            # print("ll",ele)
                            ele=re.sub(r"AS[\s]*\)[\s]*AS|AS\)","",ele,flags=re.I)
                            ele=re.sub(r"\,|[\s]+","",ele)
                            # print("ll",ele)
                            final_input_list.append(ele)
                            col_in_input_Script.append(ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                final_table_obj=''
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
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    find_integer=re.findall(r"[\w]+[\s]+DATETIME[\s]*\([\d]+\)|[\w]+[\s]+DATETIME|[\w]+[\s]+NUMERIC[\s]*\([\d]+\)|[\w]+[\s]+NUMERIC[\s]*\([\d]+\,[\d]+\)|[\w]+[\s]+NUMERIC|[\w]+[\s]+INT64[\s]*\([\d]+\)|[\w]+[\s]+INT64|[\w]+[\s]+STRING[\s]*\([\d]+\)|[\w]+[\s]+STRING|[\w]+[\s]+FLOAT64[\s]*\([\d]+\)|[\w]+[\s]+FLOAT64|[\w]+[\s]+BYTES[\s]*\([\d]+\)|[\w]+[\s]+BYTES|[\w]+[\s]+DATE|[\w]+\.[\w]+\,|\,[\s]+[\w]+[\s]+[\w]+\,|[\s]+AS[\s]+[\w]+\,|\([\w]+\.[\w]+\)|\([\w]+\.[\w]+\[\s]+",table_obj,flags=re.I)
                    #AS[\s]+[\w]+[\s]*\)*[\s]*AS[\s]*[\w]+|AS[\s]+[\w]+[\s]*\)[\s]*[\w]+|
                    # print("find_integer",find_integer)
                    for ele in find_integer:
                        # print("ele",ele)
                        if ele.lower()=="by date" or ele.lower()=="by string"  or ele.lower()=="by integer" or  ele.lower()=="on date"or ele.lower()=="on string" or ele.lower()=="on integer"  or ele.lower()=="as date"  or ele.lower()=="as string"  or ele.lower()=="as integer"  or ele.lower()=="and date"  or ele.lower()=="and string"  or ele.lower()=="and integer":
                            pass
                        else:
                            ele=re.sub(r"[\s]+STRING[\s]*|[\s]+FLOAT64[\s]*|[\s]+BYTES[\s]*|[\s]+INT64[\s]*|[\s]+INTEGER[\s]*|[\s]+STRING[\s]*|[\s]+NUMERIC[\s]*|[\s]+DATETIME[\s]*|[\s]+DATE[\s]*","",ele,flags=re.I)
                            # print(ele)
                            ele=re.sub(r"AS[\s]*\)[\s]*AS|AS\)","",ele,flags=re.I)
                            ele=re.sub(r"\,|[\s]+","",ele)
                            # print("ele",ele)
                            final_output_list.append(ele)
                            col_in_output_Script.append(ele)

                # print(filename)
                # print("input_all_datatype =",final_input_list)
                # print("output_all_datatype=",final_output_list)
                lstProcess = [item for item in final_input_list if item not in final_output_list]
                #with open(OutputFolderPath+filename, "w",encoding=e) as f
                # print(lstProcess)
                # for fileLine in fileLines:
                #     for item in lstProcess:
                #         fileLine=re.sub(r""+item+"",item,fileLine,flags=re.I)
                #         print(fileLine)

                if len(final_output_list)==0:
                    log_result.append(Result_NA())
                    result_CS_Col_Name_Case.append("NA")
                elif final_input_list==final_output_list:
                    # print("col_case has been done successfully")
                    log_result.append(Result_Yes(col_in_input_Script,col_in_output_Script))
                    result_CS_Col_Name_Case.append("Y")
                else:
                    log_result.append(Result_No(col_in_input_Script,lstProcess))
                    result_CS_Col_Name_Case.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_CS_Col_Name_Case.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def find_view_column_name_case(sql_obj):
    column_lst=[]
    parsed = sqlparse.parse(sql_obj)
    identifier_obj=''
    # print(parsed)
    for ele in parsed:
        for item in ele.tokens:
            # print(item)
            if str(item).upper() == 'FROM':
                break
            else:
                identifier_obj+=str(item)
    # identifier_obj=re.sub(r'[\s]+AS','AS',identifier_obj,flags=re.I|re.DOTALL)
    # print(identifier_obj)
    raw_column_lst=[]
    column_str=''
    bracket_cnt=0
    for i in identifier_obj:
        if i == '(':
            bracket_cnt+=1
        elif i == ')':
            bracket_cnt-=1
        if i==',' and bracket_cnt == 0:
            bracket_cnt=0
            raw_column_lst.append(column_str.strip())
            column_str=''
        else:
            column_str+=i
    raw_column_lst.append(column_str.strip())
    for item in raw_column_lst:
        try:
            col_name=''
            find_name=re.findall(r'[\w]+$',item)
            if find_name:
                col_name=find_name[0]
            else:
                find_name=re.findall(r'^[\w]+',item)
                if find_name:
                    col_name=find_name[0]
            if col_name != '':
                if re.findall(r'^[\s]*PERIOD[\s]*\(',item,flags=re.I|re.DOTALL):
                    column_lst.append(col_name+'_eff_dt')
                    column_lst.append(col_name+'_expr_dt')
                else:
                    column_lst.append(col_name)
        except Exception as e:
            pass


    return column_lst

def CS_View_Column_Case_Check(log_result,filename,result_CS_View_Case_Check):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("CS VIEW Column Name Case"))
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'\\\'|\\\"','.',InputFileObj,flags=re.I|re.DOTALL)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;|REPLACE[\s]+.*?VIEW.*?\;',InputFileObj,flags=re.I|re.DOTALL)
        # print(find_view)
        input_column_lst=[]
        for view in find_view:
            view=re.sub('\$','_DOLLAR',view,flags=re.I|re.DOTALL)
            view=re.sub('\#','_NUMBER',view,flags=re.I|re.DOTALL)
            select_col_name=re.findall(r'VIEW[\s]+[\w]+\.[\w]+[\s]*\((.*?)\)',view,flags=re.I|re.DOTALL)
            if select_col_name:
                select_col_name=select_col_name[0]
                find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_col_name,flags=re.I|re.DOTALL)
                # print(find_alice_in_quotes)
                for ele in find_alice_in_quotes:
                    new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                    # print(ele)
                    # print(new_ele)
                    select_col_name=select_col_name.replace(ele,new_ele)
                select_col_name=re.sub(r'[\s]+','',select_col_name,flags=re.I|re.DOTALL).split(',')
                # print(select_col_name)
            if select_col_name:
                input_column_lst=select_col_name
            else:
                # view=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',view,flags=re.I|re.DOTALL)
                # view=re.sub(r'substring[\s]*\(.*?[\W]FROM[\s]+','(',view,flags=re.I|re.DOTALL)
                find_sel=re.findall(r'[\W]SEL[\W]',view,flags=re.I|re.DOTALL)
                for ele in find_sel:
                    new_ele=re.sub(r'SEL','SELECT',ele,flags=re.I|re.DOTALL)
                    view=view.replace(ele,new_ele)
                find_select_to_from=re.findall(r'[\W]SEL[\W].*|[\W]SELECT[\W].*',view,flags=re.I|re.DOTALL)
                # for select_from in find_select_to_from:
                if find_select_to_from:
                    select_from=find_select_to_from[0].strip()
                    select_from=re.sub(r'^[\W]SELECT[\W]|[\W]SEL[\W]|FROM$','',select_from,flags=re.I|re.DOTALL)
                    select_from=re.sub(r'[\s]+',' ',select_from,flags=re.I|re.DOTALL)
                    find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_from,flags=re.I|re.DOTALL)
                    for ele in find_alice_in_quotes:
                        new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                        new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                        new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                        # print(ele)
                        # print(new_ele)
                        select_from=select_from.replace(ele,new_ele)
                    select_from=re.sub('[\s]+\(','(',select_from,flags=re.I|re.DOTALL)
                    input_column_lst=find_view_column_name_case(select_from)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'\\\'|\\\"','.',OutputFileObj,flags=re.I|re.DOTALL)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;',OutputFileObj,flags=re.I|re.DOTALL)
        output_column_lst=[]
        for view in find_view:
            view=re.sub('[\s]+\(','(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'substring[\s]*\(.*?[\W]FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            find_select_to_from=re.findall(r'[\W]SEL[\W].*|[\W]SELECT[\W].*',view,flags=re.I|re.DOTALL)
            # for select_from in find_select_to_from:
            if find_select_to_from:
                select_from=find_select_to_from[0].strip()
                select_from=re.sub(r'^[\W]SELECT[\W]|[\W]SEL[\W]|FROM$','',select_from,flags=re.I|re.DOTALL)
                select_from=re.sub(r'[\s]+',' ',select_from,flags=re.I|re.DOTALL)
                find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_from,flags=re.I|re.DOTALL)
                for ele in find_alice_in_quotes:
                    new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                    select_from=select_from.replace(ele,new_ele)
                output_column_lst=find_view_column_name_case(select_from)
        # log_result.append(Common("Check View Columns Column Case"))

        input_column_lst.sort()
        output_column_lst.sort()

        input_column_for_log_order=[]
        output_column_for_log_order=[]
        if input_column_lst != output_column_lst:
            for ele in input_column_lst:
                if ele not in output_column_lst:
                    input_column_for_log_order.append(ele+" : Column Not Found In Output File")
                else:
                    input_column_for_log_order.append(ele)

            for ele in output_column_lst:
                if ele not in input_column_lst:
                    output_column_for_log_order.append(ele+" : Column Not Found In Input File")
                else:
                    output_column_for_log_order.append(ele)
        if len(input_column_lst)==0 and len(output_column_lst)==0:
            log_result.append(Result_NA())
            result_CS_View_Case_Check.append("NA")
        elif input_column_lst==output_column_lst:
            log_result.append(Result_Yes(input_column_lst,output_column_lst))
            result_CS_View_Case_Check.append("Y")
        else:
            log_result.append(Result_No(input_column_for_log_order,output_column_for_log_order))
            result_CS_View_Case_Check.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_CS_View_Case_Check.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Table_DataType_Check(log_result,filename,result_Cs_DataType_Check):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("CS Datatype Checking"))
    BQ_input_cloumn=[]
    input_all_datatype=[]
    BQ_output_cloumn=[]
    output_all_datatype=[]
    try:
        if filename in inputFile:
            comment=0
            table_counter=0
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                final_table_obj=''
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
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+SET[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+|"
                                                  r"CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE|"
                                                  r"REPLACE[\s]+VIEW",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    find_char=re.findall(r"[\(\,][\s]*[\w]+[\s]+TIMESTAMP[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+TIMESTAMP|"
                                         r"[\(\,][\s]*[\w]+[\s]+DECIMAL[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+DECIMAL[\s]*\([\d]+\,[\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+DECIMAL|"
                                         r"[\(\,][\s]*[\w]+[\s]+NUMBER[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+NUMBER[\s]*\([\d]+\,[\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+NUMBER[\s]+\,|"
                                         r"[\(\,][\s]*[\w]+[\s]+BYTEINT[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+BYTEINT|"
                                         r"[\(\,][\s]*[\w]+[\s]+SMALLINT[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+SMALLINT|"
                                         r"[\(\,][\s]*[\w]+[\s]+INTEGER[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+INTEGER|"
                                         r"[\(\,][\s]*[\w]+[\s]+BIGINT[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+BIGINT|"
                                         r"[\(\,][\s]*[\w]+[\s]+CHAR[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+CHAR|"
                                         r"[\(\,][\s]*[\w]+[\s]+VARCHAR[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+VARCHAR|"
                                         r"[\(\,][\s]*[\w]+[\s]+FLOAT[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+FLOAT|"
                                         r"[\(\,][\s]*[\w]+[\s]+BLOB[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+BLOB|"
                                         r"[\(\,][\s]*[\w]+[\s]+BYTES[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+BYTES|"
                                         r"[\(\,][\s]*[\w]+[\s]+TIME[\s]*\([\d]+\)|"
                                         r"[\(\,][\s]*[\w]+[\s]+TIME|"
                                         r"[\(\,][\s]*[\w]+[\s]+DATE[\s]*FORMAT|"
                                         r"[\(\,][\s]*[\w]+[\s]+DATE[\s]*NOT[\s]+|"
                                         r"[\(\,][\s]*[\w]+[\s]+DATE[\s]*\,",table_obj,flags=re.I)
                    # print(find_char)
                    for ele in find_char:
                        ele=re.sub(r'^[\(\,][\s]*','',ele,flags=re.I|re.DOTALL)
                        rplce_char_to_string=re.sub(r"\([\d]+\,[\d]+\)|\([\d]+\)","",ele,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+CHAR|[\s]+VARCHAR","STRING",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+DECIMAL","NUMERIC",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+NUMBER","NUMERIC",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+BYTEINT|[\s]+SMALLINT|[\s]+INTEGER|BIGINT","INT64",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+BYTES|[\s]+BLOB","BYTES",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+FLOAT","FLOAT64",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+TIMESTAMP","DATETIME",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+DATE[\s]*FORMAT|[\s]+DATE[\s]*NOT[\s]+","DATE",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+DATE","DATE",rplce_char_to_string,flags=re.I)
                        rplce_char_to_string=re.sub(r"[\s]+|\,","",rplce_char_to_string,flags=re.I)
                        # print("rplce_char_to_string",rplce_char_to_string)
                        if rplce_char_to_string.lower()=="asstring" or rplce_char_to_string.lower()=="asnumeric" or rplce_char_to_string.lower()=="asint64" or rplce_char_to_string=='' or "asbytes" in rplce_char_to_string.lower() or "asfloat64" in rplce_char_to_string.lower() or \
                                rplce_char_to_string.lower()=="betweendatetime" or rplce_char_to_string.lower()=="anddatetime" or rplce_char_to_string.lower()=="asdatetime" or rplce_char_to_string.lower()=="asdate" or rplce_char_to_string.lower()=="anddate" or rplce_char_to_string.lower()=="betweendate" or rplce_char_to_string.lower()=="attime" or rplce_char_to_string.lower()=="withtime" or rplce_char_to_string.lower()=="andtime" or \
                                rplce_char_to_string.lower() in ['betweendate','selectdate']:

                            pass
                        else:
                            BQ_input_cloumn.append(ele)
                            input_all_datatype.append(rplce_char_to_string.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                final_table_obj=''
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
                        if table_counter==1:
                            final_table_obj=final_table_obj+fileLine[0]
                    else:
                        if table_counter==0:
                            find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE|"
                                                  r"CREATE[\s]+SET[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE|"
                                                  r"CREATE[\s]+TABLE|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW",fileLine,flags=re.I)
                            if find_table:
                                semicolon_before_create=re.findall(r"[\s]*\;[\s]*CREATE[\s]+",fileLine,flags=re.I)
                                if ";" in fileLine and not semicolon_before_create:
                                    final_table_obj=final_table_obj+fileLine
                                else:
                                    final_table_obj=final_table_obj+fileLine
                                    table_counter=1

                        elif table_counter==1:
                            if ";" in fileLine:
                                final_table_obj=final_table_obj+fileLine
                                table_counter=0
                            else:
                                final_table_obj=final_table_obj+fileLine
                # print(final_table_obj)
                obj=final_table_obj.split(";")
                for table_obj in obj:
                    find_integer=re.findall(r"[\(\,][\s]*[\w]+[\s]+DATETIME[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+DATETIME|"
                                            r"[\(\,][\s]*[\w]+[\s]+TIME|"
                                            r"[\(\,][\s]*[\w]+[\s]+NUMERIC[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+NUMERIC[\s]*\([\d]+\,[\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+NUMERIC|"
                                            r"[\(\,][\s]*[\w]+[\s]+INT64[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+INT64|"
                                            r"[\(\,][\s]*[\w]+[\s]+STRING[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+STRING|"
                                            r"[\(\,][\s]*[\w]+[\s]+FLOAT64[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+FLOAT64|"
                                            r"[\(\,][\s]*[\w]+[\s]+BYTES[\s]*\([\d]+\)|"
                                            r"[\(\,][\s]*[\w]+[\s]+BYTES|"
                                            r"[\(\,][\s]*[\w]+[\s]+DATE",table_obj,flags=re.I)
                    for item in find_integer:
                        item=re.sub(r'^[\(\,][\s]*','',item,flags=re.I|re.DOTALL)
                        remove_space=re.sub(r"\n|[\s]*","",item,flags=re.I)
                        if remove_space.lower() == "asint64" or remove_space.lower() == "asbytes" or remove_space.lower()=="asnumeric" or  remove_space.lower()=="asfloat64" or  remove_space.lower()=="asstring" or remove_space.lower()=="asdatetime" or remove_space.lower()=="asdate" or \
                                remove_space.lower()=="andint64" or remove_space.lower() == "andbytes" or remove_space.lower()=="andnumeric" or  remove_space.lower()=="andfloat64" or  remove_space.lower()=="andstring" or remove_space.lower()=="anddatetime" or remove_space.lower()=="anddate" or \
                                remove_space.lower()=="byint64" or remove_space.lower() == "bybytes" or remove_space.lower()=="bynumeric" or  remove_space.lower()=="byfloat64" or  remove_space.lower()=="bystring" or remove_space.lower()=="bydatetime" or remove_space.lower()=="bydate" or \
                                remove_space.lower()=="thenint64" or remove_space.lower() == "thenbytes" or remove_space.lower()=="thennumeric" or  remove_space.lower()=="thenfloat64" or  remove_space.lower()=="thenstring" or remove_space.lower()=="thendatetime" or remove_space.lower()=="thendate" or \
                                remove_space.lower()=="whenint64" or remove_space.lower() == "whenwhentes" or remove_space.lower()=="whennumeric" or  remove_space.lower()=="whenfloat64" or  remove_space.lower()=="whenstring" or remove_space.lower()=="whendatetime" or remove_space.lower()=="whendate" or \
                                remove_space.lower()=="elseint64" or remove_space.lower() == "elseelsetes" or remove_space.lower()=="elsenumeric" or  remove_space.lower()=="elsefloat64" or  remove_space.lower()=="elsestring" or remove_space.lower()=="elsedatetime" or remove_space.lower()=="elsedate" or remove_space.lower()=="bytime" or remove_space.lower()=="astime" or \
                                remove_space.lower() in ['betweendate','selectdate']:
                            pass
                        else:
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

            input_all_datatype.sort()
            output_all_datatype.sort()

            # print("input_all_datatype =",input_all_datatype )
            # print("output_all_datatype=",output_all_datatype )

            if len(input_all_datatype)==0 and len(output_all_datatype)==0:
                log_result.append(Result_NA())
                result_Cs_DataType_Check.append("NA")
            elif input_all_datatype==output_all_datatype:
                # print("CS_DataType_Check has been done succesfully")
                log_result.append(Result_Yes(BQ_input_cloumn,BQ_output_cloumn))
                result_Cs_DataType_Check.append("Y")
            else:
                log_result.append(Result_No(BQ_input_cloumn,BQ_output_cloumn))
                result_Cs_DataType_Check.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Cs_DataType_Check.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Table_Column_Names_In_Lower_Case(log_result,filename,result_Cs_Lower_Check):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Chack CS Table Column Name In Lower Case"))
    All_lower_Column_list=[]
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_not_lower_lines=re.findall(r"[\w]+\.[\w]+[\s]*\=[\s]*\'[\w]+|[\w]+\.[\w]+[\s]*IN[\s]*\(\'",fileLines,flags=re.I|re.DOTALL)
                for item in find_not_lower_lines:
                    # print(item)
                    All_lower_Column_list.append(item)

        if len(All_lower_Column_list)==0 :
            log_result.append(Result_NA())
            result_Cs_Lower_Check.append("NA")
        elif len(All_lower_Column_list)>0:
            result_Cs_Lower_Check.append("N")
            log_result.append(Output_Result_No(All_lower_Column_list))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Cs_Lower_Check.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_star_columns(log_result,filename,result_CS_star_columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("CS star columns"))
    try:
        # if filename in inputFile:
        #     comment=0
        #     table_counter=0
        #     with open(inputFolderPath+filename, 'rb') as f:
        #         result = chardet.detect(f.read())
        #         e=result['encoding']
        #     with open(inputFolderPath+filename, "r",encoding=e) as f:
        #         fileLines = f.read()
        #         f.close()
        #         find_col_ip=re.findall(r"(SELECT[\s]*.*\;)",fileLines,flags=re.I|re.DOTALL)
        #         # print("find_col_ip:", find_col_ip)

        # if filename in outputFile:
        #     sel_op=0
        #     with open(OutputFolderPath+filename, 'rb') as f:
        #         result = chardet.detect(f.read())
        #         e=result['encoding']
        #     with open(OutputFolderPath+filename, "r",encoding=e) as f:
        #         fileLines = f.read()
        #         f.close()
        #         find_col_op=re.findall(r"(SELECT[\s]*.*\;)",fileLines,flags=re.I|re.DOTALL)
        #         for ele in find_col_op:
        #             find_col_sel=re.findall(r"SELECT[\s]+\*[\s]*FROM",ele,flags=re.I|re.DOTALL)
        #             # print("find_col_sel", find_col_sel)
        #             if find_col_sel:
        #                 sel_op=1
        #
        #
        # if sel_op==1:
        #     log_result.append(Result_No(find_col_ip,find_col_sel))
        #     result_CS_star_columns.append("N")
        # else:
        #     log_result.append(Result_NA())
        #     result_CS_star_columns.append("NA")

        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        input_select_star=[]
        find_select_star=re.findall(r'SELECT[\s]+\*[\s]+FROM[\s]+\`*([\w]+\.[\w]+)\`*',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_select_star:
            ele = "SELECT * FROM "+ele
            input_select_star.append(ele)
        # print(input_select_star)


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'FROM\`','FROM `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'JOIN\`','JOIN `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'extract[\s]*\(.*?\)','',OutputFileObj,flags=re.I)
        find_select_star=re.findall(r'SELECT[\s]+\*[\s]+FROM[\s]+\`*[a-zA-Z0-9-_]+\.([\w]+\.[\w]+)\`*|'
                                    r'SELECT[\s]+\*[\s]+FROM[\s]+\`*([\w]+\.[\w]+)\`*',OutputFileObj,flags=re.I|re.DOTALL)
        # print(find_select_star)
        output_select_star=[]
        for ele in find_select_star:
            ele = ["SELECT * FROM "+i for i in ele if i != '']
            output_select_star.extend(ele)
        # print(output_select_star)

        if len(input_select_star)==0 and len(output_select_star)==0:
            log_result.append(Result_NA())
            result_CS_star_columns.append("NA")
        elif input_select_star==output_select_star:
            log_result.append(Result_Yes(input_select_star,output_select_star))
            result_CS_star_columns.append("Y")
        else:
            log_result.append(Result_No(input_select_star,output_select_star))
            result_CS_star_columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_CS_star_columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_View_Source_Object_Alias_Check(log_result,filename,result_CS_View_Source_Object_Alias_Check):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("CS TABLE Alias"))
    input_alias_lst=[]
    output_alias_lst=[]
    input_log=[]
    output_log=[]
    # print(filename)
    # try:
    #     # if 1==1:
    #     with open(inputFolderPath+filename, 'rb') as f:
    #         result = chardet.detect(f.read())
    #         e=result['encoding']
    #     with open(inputFolderPath+filename, "r",encoding=e) as f:
    #         fileLines = f.readlines()
    #         comment=0
    #         obj=''
    #         for fileLine in fileLines:
    #             cmt=re.findall(r"^[\s]*--|_current",fileLine, flags=re.I)
    #             if "/*" in fileLine:
    #                 if '*/' in fileLine:
    #                     comment=0
    #                     pass
    #                 else:
    #                     comment=1
    #                     pass
    #             elif comment==1:
    #                 if "*/" in fileLine:
    #                     comment=0
    #                     pass
    #                 else:
    #                     pass
    #             elif cmt :
    #                 pass
    #             elif "--" in fileLine:
    #                 fileLine=fileLine.split("--")
    #                 obj=obj+fileLine[0]
    #             else:
    #                 obj=obj+fileLine
    #         # print("obj:", obj)
    #         obj=re.sub(r"WHEN.*?ELSE|WHEN.*?THEN", "",obj, flags=re.I|re.DOTALL)
    #         find_alias_in=re.findall(r"FROM[\s]+[\w]+\.[\w]+[\s]*[\w]*|JOIN[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+", obj, flags=re.I)
    #
    #         for ele in find_alias_in:
    #             ele=re.sub(r"\n|WHERE", '', ele)
    #             ele=re.sub(r"[\s]+", ' ', ele)
    #             input_log.append(ele)
    #             if " AS " in ele:
    #                 ele=re.sub(r"[\w]+\.[\w]+[\s]+AS[\s]+", '', ele)
    #                 input_alias_lst.append(ele.lower())
    #
    #             elif ele in re.findall(r"FROM[\s]+[\w]+\.[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+", ele):
    #                 ele=ele=re.sub(r"[\w]+\.[\w]+[\s]+", '', ele)
    #                 ele=ele.lstrip()
    #                 ele=ele.rstrip()
    #                 input_alias_lst.append(ele.lower())
    #
    #             else:
    #                 ele=re.sub(r"FROM[\s]+[\w]+\.", 'FROM ', ele)
    #                 ele=ele.lstrip()
    #                 ele=ele.rstrip()
    #                 input_alias_lst.append(ele.lower())
    #
    #
    #
    #     if filename in outputFile:
    #         with open(OutputFolderPath+filename, 'rb') as f:
    #             result = chardet.detect(f.read())
    #             e=result['encoding']
    #         with open(OutputFolderPath+filename, "r",encoding=e) as f:
    #             fileLines = f.readlines()
    #             counter=0
    #             comment=0
    #             obj=''
    #             for fileLine in fileLines:
    #                 cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*|_current",fileLine, flags=re.I)
    #                 if "/*" in fileLine:
    #                     if '*/' in fileLine:
    #                         comment=0
    #                         pass
    #                     else:
    #                         comment=1
    #                         pass
    #                 elif comment==1:
    #                     if "*/" in fileLine:
    #                         comment=0
    #                         pass
    #                     else:
    #                         pass
    #                 elif cmt :
    #                     pass
    #                 elif "--" in fileLine:
    #                     fileLine=fileLine.split("--")
    #                     obj=obj+fileLine[0]
    #                 else:
    #                     obj=obj+fileLine
    #             obj=re.sub(r"\`.*?\`|\`.*?\`", "", obj, flags=re.I|re.DOTALL)
    #             find_alias_op=re.findall(r"FROM[\s]+AS[\s]+[\w]+|JOIN[\s]+AS[\s]+[\w]+|FROM[\s]+[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+[\s]+[\w]+", obj, flags=re.I)
    #
    #             for ele in find_alias_op:
    #                 ele=re.sub(r"[\s]+AS[\s]+|[\s]+ON|[\s]+LEFT|[\s]+RIGHT|[\s]+", ' ', ele)
    #                 ele=ele.lstrip()
    #                 ele=ele.rstrip()
    #                 output_alias_lst.append(ele.lower())
    #                 output_log.append(ele)
    #
    #     input_alias_lst.sort()
    #     output_alias_lst.sort()
    #     input_alias_lst=list(set(input_alias_lst))
    #     output_alias_lst=list(set(output_alias_lst))
    #     # print("input_all_datatype =", input_alias_lst)
    #     # print("output_all_datatype=", output_alias_lst)
    #     if len(input_alias_lst)==0 and len(output_alias_lst)==0:
    #         log_result.append(Result_NA())
    #         result_CS_Alias_Check.append("NA")
    #     elif input_alias_lst==output_alias_lst:
    #         log_result.append(Result_Yes(input_log,output_log))
    #         # print("YYY")
    #         result_CS_Alias_Check.append("Y")
    #     else:
    #         log_result.append(Result_No(input_log,output_log))
    #         result_CS_Alias_Check.append("N")
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        pass_lst=['on','join','left','right','full','outer','where','group']
        find_alice=re.findall(r'FROM[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|FROM[\s]+[\w]+\.[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+',InputFileObj,flags=re.I|re.DOTALL)
        # print(find_alice)
        for ele in find_alice:
            if not ele.lower().endswith(tuple(pass_lst)):
            # if not ele.lower().endswith('where'):
                ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
                input_log.append(ele)
                # print(ele)
                new_ele=re.sub('[\s]+AS[\s]+',' ',ele,flags=re.I|re.DOTALL)
                input_alias_lst.append(new_ele.lower())

        pass_lst=['on','join','left','right','full','outer','where','group',',',')',';']
        table_without_alice=re.findall(r'FROM[\s]+[\w]+\.[\w]+[\s]+[\w]+|FROM[\s]+[\w]+\.[\w]+[\s]*[\,\)\;]|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]*[\,\)\;]',InputFileObj,flags=re.I|re.DOTALL)
        for ele in table_without_alice:
            if ele.lower().endswith(tuple(pass_lst)):
                new_ele=re.sub(r'[\s]+[\w]+$|[\s]*[\,\)\;]','',ele,flags=re.I|re.DOTALL)
                find_table_name=re.findall(r'[\w]+$',new_ele,flags=re.I|re.DOTALL)[0].lower()
                input_log.append(new_ele)
                input_alias_lst.append(new_ele.lower()+' '+find_table_name)

        from_join_lst=['FROM','JOIN']
        for obj in from_join_lst:
            cross_from_and_join_obj=re.findall(r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)',\
                                               InputFileObj,flags=re.I|re.DOTALL)
            for item in cross_from_and_join_obj:
                for ele in item:
                    if ele.strip() != '':
                        ele=ele.split(',')
                        ele = [i for i in ele if i != '']
                        for i in ele:
                            # print(i)
                            i=obj+i
                            i=re.sub('[\s]+',' ',i,flags=re.I|re.DOTALL)
                            i=re.sub('[\s]+\.[\s]+','.',i,flags=re.I|re.DOTALL)
                            i=re.sub('[\s]+AS[\s]+',' ',i,flags=re.I|re.DOTALL)
                            input_log.append(i)
                            find_table_alice=re.findall(r'[\s]+[\w]+$',i,flags=re.I|re.DOTALL)
                            if find_table_alice:
                                input_alias_lst.append(i.lower())
                            else:
                                find_table_name=re.findall(r'[\w]+$',i,flags=re.I|re.DOTALL)[0]
                                if find_table_name:
                                    input_alias_lst.append(i.lower()+' '+find_table_name)
                        break
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'FROM\`','FROM `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'JOIN\`','JOIN `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'extract[\s]*\(.*?\)','',OutputFileObj,flags=re.I)
        find_alice=re.findall(r'FROM[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+AS[\s]+[\w]+|FROM[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+[\w]+|JOIN[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+AS[\s]+[\w]+|JOIN[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+[\w]+|'
                              r'FROM[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|FROM[\s]+[\w]+\.[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+',OutputFileObj,flags=re.I|re.DOTALL)
        # print(find_alice)
        pass_lst=['on','join','left','right','full','outer','where','group']
        for ele in find_alice:
            if not ele.lower().endswith(tuple(pass_lst)):
                ele=re.sub(r'[\s]+',' ',ele,flags=re.I|re.DOTALL)
                output_log.append(ele)
                new_ele=re.sub('[\s]+\.[\s]+','.',ele,flags=re.I|re.DOTALL)
                new_ele=re.sub('\`','',new_ele,flags=re.I|re.DOTALL)
                find_table=re.findall(r'^[\w]+[\s]+(.*?)[\s]+',new_ele,flags=re.I|re.DOTALL)[0].split('.')
                if len(find_table) == 3:
                    new_ele=new_ele.replace(find_table[0]+'.','')
                # print(find_table,new_ele)
                new_ele=re.sub('[\s]+AS[\s]+',' ',new_ele,flags=re.I|re.DOTALL)
                output_alias_lst.append(new_ele.lower())

        table_without_alice=re.findall(r'FROM[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+[\w]+|FROM[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]*[\,\)\;]|JOIN[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]+[\w]+|JOIN[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+\`*[\s]*[\,\)\;]|'
                                       r'FROM[\s]+[\w]+\.[\w]+[\s]+[\w]+|FROM[\s]+[\w]+\.[\w]+[\s]*[\,\)\;]|JOIN[\s]+[\w]+\.[\w]+[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]*[\,\)\;]',OutputFileObj,flags=re.I|re.DOTALL)
        pass_lst=['on','join','left','right','full','outer','where','group',',',')',';']
        for ele in table_without_alice:
            if ele.lower().endswith(tuple(pass_lst)):
                # print(ele)
                new_ele=re.sub(r'[\s]+[\w]+$|[\s]*[\,\)\;]|\`','',ele,flags=re.I|re.DOTALL)
                new_ele=re.sub('[\s]+\.[\s]+','.',new_ele,flags=re.I|re.DOTALL)
                find_table_name=re.findall(r'[\w]+$',new_ele,flags=re.I|re.DOTALL)[0].lower()
                find_project_id=re.sub(r'[\w]+[\s]+','',new_ele,flags=re.I|re.DOTALL).split('.')
                if len(find_project_id)== 3:
                    new_ele=new_ele.replace(find_project_id[0]+'.','')
                # is_three_level=re.findall(r'')
                output_log.append(new_ele)
                output_alias_lst.append(new_ele.lower()+' '+find_table_name)

        from_join_lst=['FROM','JOIN']
        for obj in from_join_lst:
            cross_from_and_join_obj=re.findall(r''+obj+'[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+[\w]+(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]*)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+[\w]+(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\.[\w]+[\s]*\.[\s]*[\w]+\`*[\s]*)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+AS[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]+[\w]+(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+)+)|'
                                               r''+obj+'[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)',\
                                               OutputFileObj,flags=re.I|re.DOTALL)
            # print(cross_from_and_join_obj)
            for item in cross_from_and_join_obj:
                for ele in item:
                    if ele.strip() != '':
                        ele=ele.split(',')
                        ele = [i for i in ele if i != '']
                        for i in ele:
                            # print(i)
                            i=obj+i
                            output_log.append(i)
                            i=re.sub('\`','',i,flags=re.I|re.DOTALL)
                            i=re.sub('[\s]+',' ',i,flags=re.I|re.DOTALL)
                            i=re.sub('[\s]+\.[\s]+','.',i,flags=re.I|re.DOTALL)
                            i=re.sub('[\s]+AS[\s]+',' ',i,flags=re.I|re.DOTALL)
                            remove_project_id=re.findall(r''+obj+'[\s]+([a-zA-Z0-9-_]+\.([\w]+\.[\w]+))',i,flags=re.I|re.DOTALL)
                            if remove_project_id:
                                i=i.replace(remove_project_id[0][0],remove_project_id[0][1])
                            find_table_alice=re.findall(r'[\s]+[\w]+$',i,flags=re.I|re.DOTALL)
                            if find_table_alice:
                                output_alias_lst.append(i.lower())
                            else:
                                find_table_name=re.findall(r'[\w]+$',i,flags=re.I|re.DOTALL)[0]
                                if find_table_name:
                                    output_alias_lst.append(i.lower()+' '+find_table_name)
                        break
        input_alias_lst.sort()
        output_alias_lst.sort()
        # print("II",input_alias_lst)
        # print("OO",output_alias_lst)
        if len(input_alias_lst)==0 and len(output_alias_lst)==0:
            log_result.append(Result_NA())
            result_CS_View_Source_Object_Alias_Check.append("NA")
        elif input_alias_lst==output_alias_lst:
            # print("YYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_CS_View_Source_Object_Alias_Check.append("Y")
        else:
            log_result.append(Result_No(input_log,output_log))
            result_CS_View_Source_Object_Alias_Check.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_CS_View_Source_Object_Alias_Check.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def find_view_column_name(sql_obj):
    column_lst=[]
    parsed = sqlparse.parse(sql_obj)
    identifier_obj=''
    # print(parsed)
    for ele in parsed:
        for item in ele.tokens:
            # print(item)
            if str(item).upper() == 'FROM':
                break
            else:
                identifier_obj+=str(item)
    # identifier_obj=re.sub(r'[\s]+AS','AS',identifier_obj,flags=re.I|re.DOTALL)
    # print(identifier_obj)
    raw_column_lst=[]
    column_str=''
    bracket_cnt=0
    for i in identifier_obj:
        if i == '(':
            bracket_cnt+=1
        elif i == ')':
            bracket_cnt-=1
        if i==',' and bracket_cnt == 0:
            bracket_cnt=0
            raw_column_lst.append(column_str.strip())
            column_str=''
        else:
            column_str+=i
    raw_column_lst.append(column_str.strip())
    for item in raw_column_lst:
        try:
            col_name=''
            find_name=re.findall(r'[\w]+$',item)
            if find_name:
                col_name=find_name[0].lower()
            else:
                find_name=re.findall(r'^[\w]+',item)
                if find_name:
                    col_name=find_name[0].lower()
            if col_name != '':
                if re.findall(r'^[\s]*PERIOD[\s]*\(',item,flags=re.I|re.DOTALL):
                    column_lst.append(col_name+'_eff_dt')
                    column_lst.append(col_name+'_expr_dt')
                else:
                    column_lst.append(col_name)
        except Exception as e:
            pass


    return column_lst

def CS_View_Column_Names_Check(log_result, filename, result_view_columns_order_in_input_and_output,result_view_columns_in_input_and_output,result_view_column_count):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'\\\'|\\\"','.',InputFileObj,flags=re.I|re.DOTALL)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;|REPLACE[\s]+.*?VIEW.*?\;',InputFileObj,flags=re.I|re.DOTALL)
        # print(find_view)
        input_column_lst=[]
        for view in find_view:
            view=re.sub('\$','_DOLLAR',view,flags=re.I|re.DOTALL)
            view=re.sub('\#','_NUMBER',view,flags=re.I|re.DOTALL)
            select_col_name=re.findall(r'VIEW[\s]+[\w]+\.[\w]+[\s]*\((.*?)\)',view,flags=re.I|re.DOTALL)
            if select_col_name:
                select_col_name=select_col_name[0]
                find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_col_name,flags=re.I|re.DOTALL)
                # print(find_alice_in_quotes)
                for ele in find_alice_in_quotes:
                    new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                    # print(ele)
                    # print(new_ele)
                    select_col_name=select_col_name.replace(ele,new_ele)
                select_col_name=re.sub(r'[\s]+','',select_col_name.lower(),flags=re.I|re.DOTALL).split(',')
                # print(select_col_name)
            if select_col_name:
                input_column_lst=select_col_name
            else:
                # view=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',view,flags=re.I|re.DOTALL)
                # view=re.sub(r'substring[\s]*\(.*?[\W]FROM[\s]+','(',view,flags=re.I|re.DOTALL)
                find_sel=re.findall(r'[\W]SEL[\W]',view,flags=re.I|re.DOTALL)
                for ele in find_sel:
                    new_ele=re.sub(r'SEL','SELECT',ele,flags=re.I|re.DOTALL)
                    view=view.replace(ele,new_ele)
                find_select_to_from=re.findall(r'[\W]SEL[\W].*|[\W]SELECT[\W].*',view,flags=re.I|re.DOTALL)
                # for select_from in find_select_to_from:
                if find_select_to_from:
                    select_from=find_select_to_from[0].strip()
                    select_from=re.sub(r'^[\W]SELECT[\W]|[\W]SEL[\W]|FROM$','',select_from,flags=re.I|re.DOTALL)
                    select_from=re.sub(r'[\s]+',' ',select_from,flags=re.I|re.DOTALL)
                    find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_from,flags=re.I|re.DOTALL)
                    for ele in find_alice_in_quotes:
                        new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                        new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                        new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                        # print(ele)
                        # print(new_ele)
                        select_from=select_from.replace(ele,new_ele)
                    select_from=re.sub('[\s]+\(','(',select_from,flags=re.I|re.DOTALL)
                    input_column_lst=find_view_column_name(select_from)
                    # print(select_from)
                    # input_raw_column_lst=[]
                    # column_str=''
                    # b_o=0
                    # b_c=0
                    # find_from=''
                    # for i in select_from:
                    #     find_special_char=re.findall(r'[\W]',i)
                    #     if find_special_char:
                    #         find_from=''
                    #     else:
                    #         find_from+=i.upper()
                    #
                    #     if i == '(':
                    #         b_o+=1
                    #     elif i == ')':
                    #         b_c+=1
                    #     if b_o == b_c and find_from == 'FROM':
                    #         column_str=re.sub(r'FRO$','',column_str,flags=re.I|re.DOTALL)
                    #         # print("CCCCCCC",column_str)
                    #         break
                    #     if i==',' and b_o == b_c:
                    #         b_o=0
                    #         b_c=0
                    #         input_raw_column_lst.append(column_str)
                    #         column_str=''
                    #     else:
                    #         column_str+=i
                    # input_raw_column_lst.append(column_str)

                    # for ele in input_raw_column_lst:
                    #     find_col=re.findall(r'([\w]+)[\W]*$',ele,flags=re.I|re.DOTALL)
                    #     if find_col:
                    #         if re.findall(r'^[\s]*PERIOD[\s]*\(',ele,flags=re.I|re.DOTALL):
                    #             input_column_lst.append(find_col[0].lower()+"_EFF_DT".lower())
                    #             input_column_lst.append(find_col[0].lower()+"_EXPR_DT".lower())
                    #         else:
                    #             input_column_lst.append(find_col[0].lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'\\\'|\\\"','.',OutputFileObj,flags=re.I|re.DOTALL)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;',OutputFileObj,flags=re.I|re.DOTALL)
        output_column_lst=[]
        for view in find_view:
            view=re.sub('[\s]+\(','(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'substring[\s]*\(.*?[\W]FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            find_select_to_from=re.findall(r'[\W]SEL[\W].*|[\W]SELECT[\W].*',view,flags=re.I|re.DOTALL)
            # for select_from in find_select_to_from:
            if find_select_to_from:
                select_from=find_select_to_from[0].strip()
                select_from=re.sub(r'^[\W]SELECT[\W]|[\W]SEL[\W]|FROM$','',select_from,flags=re.I|re.DOTALL)
                select_from=re.sub(r'[\s]+',' ',select_from,flags=re.I|re.DOTALL)
                find_alice_in_quotes=re.findall(r'\".*?\"|\'.*?\'',select_from,flags=re.I|re.DOTALL)
                for ele in find_alice_in_quotes:
                    new_ele=re.sub('\"[\s]*|\'[\s]*|[\s]*\"|[\s]*\'','',ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('[\s]+|-','_',new_ele,flags=re.I|re.DOTALL)
                    new_ele=re.sub('\(|\)|\,|\/','_',new_ele,flags=re.I|re.DOTALL)
                    select_from=select_from.replace(ele,new_ele)
                # output_raw_column_lst=[]
                # column_str=''
                # b_o=0
                # b_c=0
                # find_from=''
                # for i in select_from:
                #     find_special_char=re.findall(r'[\W]',i)
                #     if find_special_char:
                #         find_from=''
                #     else:
                #         find_from+=i.upper()
                #
                #     if i == '(':
                #         b_o+=1
                #     elif i == ')':
                #         b_c+=1
                #     if b_o == b_c and find_from == 'FROM':
                #         column_str=re.sub(r'FRO$','',column_str,flags=re.I|re.DOTALL)
                #         # print(column_str)
                #         break
                #     if i==',' and b_o == b_c:
                #         b_o=0
                #         b_c=0
                #         output_raw_column_lst.append(column_str)
                #         column_str=''
                #     else:
                #         column_str+=i
                # output_raw_column_lst.append(column_str)
                # for ele in output_raw_column_lst:
                #     find_col=re.findall(r'([\w]+)[\W]*$',ele,flags=re.I|re.DOTALL)
                #     if find_col:
                #         output_column_lst.append(find_col[0].lower())
                output_column_lst=find_view_column_name(select_from)
        log_result.append(Common("Check View Columns Order"))

        input_column_for_log_order=[]
        output_column_for_log_order=[]
        if input_column_lst != output_column_lst:
            for ele in input_column_lst:
                if ele not in output_column_lst:
                    input_column_for_log_order.append(ele+" : Column Not Found In Output File")
                else:
                    input_column_for_log_order.append(ele)

            for ele in output_column_lst:
                if ele not in input_column_lst:
                    output_column_for_log_order.append(ele+" : Column Not Found In Input File")
                else:
                    output_column_for_log_order.append(ele)
        if len(input_column_lst)==0 and len(output_column_lst)==0:
            log_result.append(Result_NA())
            result_view_columns_order_in_input_and_output.append("NA")
        elif input_column_lst==output_column_lst:
            log_result.append(Result_Yes(input_column_lst,output_column_lst))
            result_view_columns_order_in_input_and_output.append("Y")
        else:
            log_result.append(Result_No(input_column_for_log_order,output_column_for_log_order))
            result_view_columns_order_in_input_and_output.append("N")

        input_column_lst.sort()
        output_column_lst.sort()
        input_log_column_lst=[]
        output_log_column_lst=[]
        if input_column_lst != output_column_lst:
            for ele in input_column_lst:
                if ele not in output_column_lst:
                    input_log_column_lst.append(ele+" : Column Not Found In Output File")
                else:
                    input_log_column_lst.append(ele)

            for ele in output_column_lst:
                if ele not in input_column_lst:
                    output_log_column_lst.append(ele+" : Column Not Found In Input File")
                else:
                    output_log_column_lst.append(ele)

        # print(output_column_lst)
        # print(len(output_column_lst))
        # print(find_select_to_from[0])
        # print(find_view)
        # print("input_current_timestamp =", len(input_current_timestamp), input_current_timestamp)
        # print("output_current_datetime=", len(output_current_datetime), output_current_datetime)
        log_result.append(Common("Check View Column Names"))
        if len(input_column_lst)==0 and len(output_column_lst)==0:
            log_result.append(Result_NA())
            result_view_columns_in_input_and_output.append("NA")
        elif input_column_lst==output_column_lst:
            # print("YYY")
            log_result.append(Result_Yes(input_column_lst,output_column_lst))
            result_view_columns_in_input_and_output.append("Y")
        else:
            log_result.append(Result_No(input_log_column_lst,output_log_column_lst))
            result_view_columns_in_input_and_output.append("N")

        if len(input_column_lst)==0 and len(output_column_lst)==0:
            # log_result.append(Result_NA())
            result_view_column_count.append("NA")
        elif len(input_column_lst)==len(output_column_lst):
            # print("YYY")
            # log_result.append(Result_Yes(input_column_lst,output_column_lst))
            result_view_column_count.append("Y("+str(len(input_column_lst))+"/"+str(len(output_column_lst))+")")
        else:
            # log_result.append(Result_No(input_column_lst,output_column_lst))
            result_view_column_count.append("N("+str(len(input_column_lst))+"/"+str(len(output_column_lst))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_view_columns_in_input_and_output.append("Encoding")
        result_view_column_count.append("Encoding")
        result_view_columns_order_in_input_and_output.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_View_Objects_Compare(log_result, filename, result_from_and_join_in_view,result_from_and_join_count_in_view):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Objects In View"))
    input_from_join_table=[]
    output_from_join_table=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',InputFileObj,flags=re.I|re.DOTALL)
        # InputFileObj=re.sub(r'extract[\s]*\(','EXTRACT(',InputFileObj,flags=re.I|re.DOTALL)
        # find_extract=regex.findall(r'\((?:[^()]|\((?:[^()]|(?R))*\))*\)',InputFileObj)
        # for ele in find_extract:
        #     InputFileObj=InputFileObj.replace('EXTRACT'+ele,'')

        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;|REPLACE[\s]+.*?VIEW.*?\;',InputFileObj,flags=re.I|re.DOTALL)
        for view in find_view:
            # view=re.sub(r'extract[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)','',view,flags=re.I|re.DOTALL)
            view=re.sub(r'[\s]+AS[\s]+[\w]+','',view,flags=re.I|re.DOTALL)
            from_and_join_obj=re.findall(r'FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+|JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+',view,flags=re.I|re.DOTALL)
            for ele in from_and_join_obj:
                ele=re.sub(r'^FROM[\s]+|^JOIN[\s]+|[\s]+|\`','',ele.lower(),flags=re.I|re.DOTALL)
                input_from_join_table.append(ele)
                # print(ele)
            # print(from_and_join_obj)
            cross_from_and_join_obj=re.findall(r'FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]+[\w]*[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]+[\w]*[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)', \
                                               view,flags=re.I|re.DOTALL)
            for item in cross_from_and_join_obj:
                for ele in item:
                    if ele.strip() != '':
                        ele=re.sub(r'[\s]+\.[\s]+','.',ele,flags=re.I|re.DOTALL)
                        ele=re.sub(r'[\s]+[\w]+[\s]*,|[\s]+[\w]+[\s]*$',',',ele,flags=re.I|re.DOTALL)
                        ele=re.sub(r'[\s]+|[\s]+AS[\s]+','',ele.lower(),flags=re.I|re.DOTALL)
                        ele=ele.split(',')
                        ele = [i for i in ele if i != '']
                        input_from_join_table.extend(ele)
                        break
        input_from_join_table.sort()
        # print(input_from_join_table)
        # print(len(input_from_join_table))
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'FROM\`','FROM `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'JOIN\`','JOIN `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',OutputFileObj,flags=re.I|re.DOTALL)
        # OutputFileObj=re.sub(r'extract[\s]*\(','EXTRACT(',OutputFileObj,flags=re.I|re.DOTALL)
        # extract_col=[]
        # find_extract=regex.findall(r'\((?:[^()]|\((?:[^()]|(?R))*\))*\)',OutputFileObj)
        # for ele in find_extract:
        #     inside_extract=regex.findall(r'\((?:[^()]|\((?:[^()]|(?R))*\))*\)',ele)
        #     print(inside_extract)
        #     extract_col.append(ele)
        #     extract_col.extend(inside_extract)
        # for ele in extract_col:
        #     print("RRRR",ele)
        #     OutputFileObj=OutputFileObj.replace('EXTRACT'+ele,'')
        # OutputFileObj=re.sub(r'extract[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)','',OutputFileObj,flags=re.I|re.DOTALL)
        # OutputFileObj=re.sub(r'extract[\s]*\(.*?\)','',OutputFileObj,flags=re.I)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;',OutputFileObj,flags=re.I|re.DOTALL)
        for view in find_view:
            view=re.sub(r'[\s]+AS[\s]+[\w]+','',view,flags=re.I|re.DOTALL)
            from_and_join_obj=re.findall(r'FROM[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*|JOIN[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*|FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+|JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+',view,flags=re.I|re.DOTALL)
            for ele in from_and_join_obj:
                ele=re.sub(r'^FROM[\s]+|^JOIN[\s]+|\`|[\s]+','',ele.lower(),flags=re.I|re.DOTALL)
                is_projectid_present=len(re.findall(r'\.',ele,flags=re.I|re.DOTALL))
                if is_projectid_present == 2:
                    split_ele=ele.split('.')
                    ele=split_ele[1]+'.'+split_ele[2]
                output_from_join_table.append(ele)
            # print(view)
            cross_from_and_join_obj=re.findall(r'FROM[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*)+)|'
                                               r'JOIN[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*)+)|'
                                               r'FROM[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]+[\w]+[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]+[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]+[\w]+[\s]*(([\s]*\,[\s]*\`*[a-zA-Z0-9-_]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]*\.[\s]*\`*[\w]+\`*[\s]+[\w]+[\s]*)+)|'
                                               r'FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)',view,flags=re.I|re.DOTALL)
            for item in cross_from_and_join_obj:
                for ele in item:
                    if ele.strip() != '':
                        ele=re.sub(r'[\s]+[\w]+','',ele.lower(),flags=re.I|re.DOTALL)
                        ele=re.sub(r'[\s]+|\`','',ele.lower(),flags=re.I|re.DOTALL)
                        ele_lst=ele.split(',')
                        ele_lst.remove('')
                        for ele in ele_lst:
                            is_projectid_present=len(re.findall(r'\.',ele,flags=re.I|re.DOTALL))
                            if is_projectid_present == 2:
                                split_ele=ele.split('.')
                                ele=split_ele[1]+'.'+split_ele[2]
                            output_from_join_table.append(ele)
                        break

        output_from_join_table.sort()
        # print(output_from_join_table)
        # print(len(output_from_join_table))
        if len(input_from_join_table)==0 and len(output_from_join_table)==0:
            log_result.append(Result_NA())
            result_from_and_join_in_view.append("NA")
        elif input_from_join_table==output_from_join_table:
            log_result.append(Result_Yes(input_from_join_table,output_from_join_table))
            result_from_and_join_in_view.append("Y")
        else:
            log_result.append(Result_No(input_from_join_table,output_from_join_table))
            result_from_and_join_in_view.append("N")

        if len(input_from_join_table)==0 and len(output_from_join_table)==0:
            # log_result.append(Result_NA())
            result_from_and_join_count_in_view.append("NA")
        elif len(input_from_join_table)==len(output_from_join_table):
            # print("YYY")
            # log_result.append(Result_Yes(input_from_join_table,output_from_join_table))
            result_from_and_join_count_in_view.append("Y("+str(len(input_from_join_table))+"/"+str(len(output_from_join_table))+")")
        else:
            # log_result.append(Result_No(input_from_join_table,output_from_join_table))
            result_from_and_join_count_in_view.append("N("+str(len(input_from_join_table))+"/"+str(len(output_from_join_table))+")")

    except Exception as e:
        # print("EEEEe",e)
        print("Unexpected error:", sys.exc_info()[0])
        result_from_and_join_in_view.append("Encoding")
        result_from_and_join_count_in_view.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Join_Type_Count(log_result, filename,result_inner_join_count,result_cross_join_count,result_left_join_count,result_right_join_count):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    # log_result.append(Common("Check Join Types Count In View"))
    word_regex="[a-zA-Z0-9_\-\$\{\}]+"
    pass_join=['LEFT','RIGHT','OUTER','INNER','CROSS']
    input_inner_join_lst=[]
    input_cross_join_lst=[]
    input_left_join_lst=[]
    input_right_join_lst=[]
    output_inner_join_lst=[]
    output_cross_join_lst=[]
    output_left_join_lst=[]
    output_right_join_lst=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]+\.[\s]+','.',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',InputFileObj,flags=re.I|re.DOTALL)
        # InputFileObj=re.sub(r'extract[\s]*\(','EXTRACT(',InputFileObj,flags=re.I|re.DOTALL)
        # find_extract=regex.findall(r'\((?:[^()]|\((?:[^()]|(?R))*\))*\)',InputFileObj)
        # for ele in find_extract:
        #     InputFileObj=InputFileObj.replace('EXTRACT'+ele,'')

        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;|REPLACE[\s]+.*?VIEW.*?\;',InputFileObj,flags=re.I|re.DOTALL)
        # left_join_types=['LEFT[\s]+OUTER[\s]+JOIN','LEFT[\s]+JOIN']
        for view in find_view:
            # view=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'extract[\s]*\(','EXTRACT(',view,flags=re.I|re.DOTALL)
            # view=re.sub(r'extract[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)','',view,flags=re.I|re.DOTALL)
            view=re.sub(r'substring[\s]*\(.*?[\W]FROM[\s]+','(',view,flags=re.I|re.DOTALL)
            view=re.sub(r'[\s]+',' ',view.upper(),flags=re.I|re.DOTALL)
            inner_join_types=re.findall(r'INNER[\s]+JOIN[\s]+',view,flags=re.I|re.DOTALL)
            cross_join_types=re.findall(r'CROSS[\s]+JOIN[\s]+',view,flags=re.I|re.DOTALL)
            left_join_types=re.findall(r'LEFT[\s]+[\w]*[\s]*JOIN[\s]+',view,flags=re.I|re.DOTALL)
            right_join_types=re.findall(r'RIGHT[\s]+[\w]*[\s]*JOIN[\s]+',view,flags=re.I|re.DOTALL)
            input_inner_join_lst.extend(inner_join_types)
            input_cross_join_lst.extend(cross_join_types)
            input_left_join_lst.extend(left_join_types)
            input_right_join_lst.extend(right_join_types)

            # print(view)
            find_only_join=re.findall(r'[\w]+[\s]+JOIN|[\W]+[\s]*JOIN',view,flags=re.I|re.DOTALL)
            for ele in find_only_join:
                # print(ele)
                if not ele.startswith(tuple(pass_join)):
                    input_inner_join_lst.append(ele)

            cross_from_and_join_obj=re.findall(r'[\W]FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'[\W]FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]FROM[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]FROM[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'[\W]FROM[\s]+[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]FROM[\s]+[\w]+[\s]+[\w]*[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]+AS[\s]+[\w]+[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+AS[\s]+[\w]+[\s]*)+)|'
                                               r'[\W]JOIN[\s]+[\w]+[\s]+[\w]*[\s]*(([\s]*\,[\s]*[\w]+[\s]*\.[\s]*[\w]+[\s]+[\w]+[\s]*)+)', \
                                               view,flags=re.I|re.DOTALL)
            # print(cross_from_and_join_obj)
            for item in cross_from_and_join_obj:
                for ele in item:
                    if ele.strip() != '':
                        ele=re.sub(r'[\s]+','',ele,flags=re.I|re.DOTALL).split(',')
                        for i in ele:
                            if i != '':
                                input_inner_join_lst.append(','+i)
                        break
            find_from_to_where_or_on=re.sub(r'SELECT[\s]+.*?[\W]FROM[\W]|SEL[\s]+.*?[\W]FROM[\W]',' SELECT_REMOVED ',view,flags=re.I|re.DOTALL)
            # find_from_to_where_or_on=re.findall(r'[\W]FROM.*?WHERE|[\W]FROM.*?[\W]ON[\W]',find_from_to_where_or_on,flags=re.I|re.DOTALL)
            # obj_from_to_where_or_on=''
            # for ele in find_from_to_where_or_on:
            #     obj_from_to_where_or_on+=ele
            # print(find_from_to_where_or_on)
            # find_cross_join_to_inner_join=re.findall(r'\)[\s]*\,[\s]*\([\s]*SELECT[\s]+|\)[\s]*\,[\s]*\([\s]*SEL[\s]+|'
            #                                          r'\)[\s]*AS[\s]+[\w]+[\s]*\,[\s]*\([\s]*SELECT[\s]+|\)[\s]*AS[\s]+[\w]+[\s]*\,[\s]*\([\s]*SEL[\s]+|'
            #                                          r'\)[\s]*[\w]+[\s]*\,[\s]*\([\s]*SELECT[\s]+|\)[\s]*[\w]+[\s]*\,[\s]*\([\s]*SEL[\s]+',find_from_to_where_or_on,flags=re.I|re.DOTALL)
            find_cross_join_to_inner_join=re.findall(r'\)[\s]*\,[\s]*\([\s]*SELECT_REMOVED[\s]+|'
                                                     r'\)[\s]*AS[\s]+[\w]+[\s]*\,[\s]*\([\s]*SELECT_REMOVED[\s]+|'
                                                     r'\)[\s]*[\w]+[\s]*\,[\s]*\([\s]*SELECT_REMOVED[\s]+',find_from_to_where_or_on,flags=re.I|re.DOTALL)
            # print(find_cross_join_to_inner_join)
            for ele in find_cross_join_to_inner_join:
                input_inner_join_lst.append(ele.replace('SELECT_REMOVED','SELECT'))
            # print("INNER :",inner_join_types)
            # print("CROSS :",cross_join_types)
            # print("LEFT  :",left_join_types)
            # print("RIGHT :",right_joinput_inner_join_lstin_types)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]+\.[\s]+','.',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'FROM\`','FROM `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'JOIN\`','JOIN `',OutputFileObj,flags=re.I)
        OutputFileObj=re.sub(r'extract[\s]*\([\s]*[\w]+[\s]+FROM[\s]+','(',OutputFileObj,flags=re.I|re.DOTALL)
        # OutputFileObj=re.sub(r'extract[\s]*\(','EXTRACT(',OutputFileObj,flags=re.I|re.DOTALL)
        # find_extract=regex.findall(r'\((?:[^()]|\((?:[^()]|(?R))*\))*\)',OutputFileObj)
        # for ele in find_extract:
        #     OutputFileObj=OutputFileObj.replace('EXTRACT'+ele,'')
        # OutputFileObj=re.sub(r'extract[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\)','',OutputFileObj,flags=re.I|re.DOTALL)
        # OutputFileObj=re.sub(r'extract[\s]*\(.*?\)','',OutputFileObj,flags=re.I)
        find_view=re.findall(r'CREATE[\s]+.*?VIEW.*?\;',OutputFileObj,flags=re.I|re.DOTALL)
        for view in find_view:
            view=re.sub(r'[\s]+',' ',view.upper(),flags=re.I|re.DOTALL)
            inner_join_types=re.findall(r'INNER[\s]+JOIN[\s]+',view,flags=re.I|re.DOTALL)
            cross_join_types=re.findall(r'CROSS[\s]+JOIN[\s]+',view,flags=re.I|re.DOTALL)
            left_join_types=re.findall(r'LEFT[\s]+[\w]*[\s]*JOIN[\s]+',view,flags=re.I|re.DOTALL)
            right_join_types=re.findall(r'RIGHT[\s]+[\w]*[\s]*JOIN[\s]+',view,flags=re.I|re.DOTALL)
            output_inner_join_lst.extend(inner_join_types)
            output_cross_join_lst.extend(cross_join_types)
            output_left_join_lst.extend(left_join_types)
            output_right_join_lst.extend(right_join_types)

            find_only_join=re.findall(r'[\w]+[\s]+JOIN|[\W]+[\s]*JOIN',view,flags=re.I|re.DOTALL)
            for ele in find_only_join:
                if not ele.startswith(tuple(pass_join)):
                    output_inner_join_lst.append(ele)



            # find_select_to_from=re.findall(r'[\W]SEL[\W].*?FROM|[\W]SELECT[\W].*?FROM',view,flags=re.I|re.DOTALL)
            # # for select_from in find_select_to_from:
            # if find_select_to_from:
            #     find_sub_query_in_cross_join=view.replace(find_select_to_from[0],'')
            #     find_sub_query_in_cross_join=re.sub(r'\([\s]*SEL[\s]+|\([\s]*SELECT[\s]+','(SELECT',view,flags=re.I|re.DOTALL)
            #     find_select=re.findall(r'\(SELECT',find_sub_query_in_cross_join,flags=re.I|re.DOTALL)
            #     for sel in find_select:
            #         print(sel)
            # print("INNER :",inner_join_types)
            # print("CROSS :",cross_join_types)
            # print("LEFT  :",left_join_types)
            # print("RIGHT :",right_join_types)
        # print("OOO",output_inner_join_lst)
        # print("CCC",output_cross_join_lst)
        join_result_dict={"INNER":[result_inner_join_count,input_inner_join_lst,output_inner_join_lst], \
                          "CROSS":[result_cross_join_count,input_cross_join_lst,output_cross_join_lst], \
                          "LEFT":[result_left_join_count,input_left_join_lst,output_left_join_lst], \
                          "RIGHT":[result_right_join_count,input_right_join_lst,output_right_join_lst], \
                          }
        # print(join_result_dict)
        for join_type,result in join_result_dict.items():
            log_result.append(Common("Check "+join_type+" Join Count In View"))
            if len(result[1])==0 and len(result[2])==0:
                log_result.append(Result_NA())
                result[0].append("NA")
            elif len(result[1])==len(result[2]):
                log_result.append(Result_Yes(result[1],result[2]))
                result[0].append("Y("+str(len(result[1]))+"/"+str(len(result[2]))+")")
            else:
                log_result.append(Result_No(result[1],result[2]))
                result[0].append("N("+str(len(result[1]))+"/"+str(len(result[2]))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_inner_join_count.append("Encoding")
        result_cross_join_count.append("Encoding")
        result_left_join_count.append("Encoding")
        result_cross_join_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Check_Union_And_Union_All(log_result, filename,result_union_and_union_all):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check UNION And UNION ALL Count"))
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'

        input_union_and_union_all=[]
        input_union_and_union_all_log=[]
        find_union=re.findall(r'\bUNION\b',InputFileObj,flags=re.I|re.DOTALL)
        find_union_all=re.findall(r'\bUNION\b[\s]+ALL',InputFileObj,flags=re.I|re.DOTALL)
        for i in range(len(find_union_all)):
            find_union.pop()
        for ele in find_union:
            input_union_and_union_all_log.append(ele)
            ele+=' DISTINCT'
            input_union_and_union_all.append(ele.upper())
        for ele in find_union_all:
            ele=re.sub(r'[\s]+',' ',ele.upper())
            input_union_and_union_all_log.append(ele)
            input_union_and_union_all.append(ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        output_union_and_union_all=[]
        output_union_and_union_all_log=[]
        find_union=re.findall(r'\bUNION\b[\s]+DISTINCT',OutputFileObj,flags=re.I|re.DOTALL)
        find_union_all=re.findall(r'\bUNION\b[\s]+ALL',OutputFileObj,flags=re.I|re.DOTALL)
        for ele in find_union+find_union_all:
            ele=re.sub(r'[\s]+',' ',ele.upper())
            output_union_and_union_all_log.append(ele)
            output_union_and_union_all.append(ele)

        # print(input_union_and_union_all)
        # print(output_union_and_union_all)
        if len(input_union_and_union_all)==0 and len(output_union_and_union_all)==0:
            log_result.append(Result_NA())
            result_union_and_union_all.append("NA")
        elif input_union_and_union_all == output_union_and_union_all:
            log_result.append(Result_Yes(input_union_and_union_all_log,output_union_and_union_all_log))
            result_union_and_union_all.append("Y("+str(len(input_union_and_union_all))+"/"+str(len(output_union_and_union_all))+")")
        else:
            log_result.append(Result_No(input_union_and_union_all_log,output_union_and_union_all_log))
            result_union_and_union_all.append("N("+str(len(input_union_and_union_all))+"/"+str(len(output_union_and_union_all))+")")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_union_and_union_all.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_No_Of_Lines_In_File(log_result, filename,result_cs_file_line_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("CS Number Of Lines In File"))
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                    elif fileline.strip() == '':
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'[\s]*\n[\s]*','\n',InputFileObj,flags=re.I|re.DOTALL)
        input_no_of_lines=len(InputFileObj.splitlines(True))


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                    elif fileline.strip() == '':
                        pass
                    else:
                        if '--' in fileline:
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'[\s]*\n[\s]*','\n',OutputFileObj,flags=re.I|re.DOTALL)
        output_no_of_lines=len(OutputFileObj.splitlines(True))
        if output_no_of_lines > input_no_of_lines*2 or output_no_of_lines*1.5 < input_no_of_lines:
            log_result.append(Result_No(["No of Lines In Input : "+str(input_no_of_lines)],["No of Lines In Output : "+str(output_no_of_lines)]))
            result_cs_file_line_count.append("N("+str(input_no_of_lines)+"/"+str(output_no_of_lines)+")")
        else:
            log_result.append(Result_Yes(["No of Lines In Input : "+str(input_no_of_lines)],["No of Lines In Output : "+str(output_no_of_lines)]))
            result_cs_file_line_count.append("Y("+str(input_no_of_lines)+"/"+str(output_no_of_lines)+")")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_file_line_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Compare_Hardcoded_Values(log_result, filename,result_cs_compare_hardcoded_value):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Hardcoded Values "))
    input_hardcoded_values=[]
    output_hardcoded_values=[]
    input_hardcoded_values_log=[]
    output_hardcoded_values_log=[]
    fun_lst=['LAST_DATE','TO_DATE','FORMAT_DATE','DATE_TRUNC','TRUNC','PARSE_DATE','PARSE_DATETIME','TO_CHAR','FORMAT_DATETIME','PARSE_TIMESTAMP']
    fun_regex_obj=''
    for fun in fun_lst:
        fun_regex_obj+=fun+'\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)|'
    fun_regex_obj=re.sub(r'\|$','',fun_regex_obj)
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r''+fun_regex_obj,'',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'AS[\s]+[\'\"].*?[\'\"]','',InputFileObj,flags=re.I|re.DOTALL)
        # print(find_fun_lst)
        InputFileObj=re.sub('YYYY-MM-DD|MM\/DD\/YYYY|AMERICA\/NEW_YORK|%M\/%D\/%Y','',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub('FORMAT[\s]+[\'\"].*?[\'\"]|INTERVAL[\s]+[\'\"].*?[\'\"]','',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'\'\'','double_quotes_replaced',InputFileObj)
        find_hardcoded_value=re.findall(r'[\'\"].*?[\'\"]',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_hardcoded_value:

            ele=ele.replace('double_quotes_replaced','\'\'')
            input_hardcoded_values_log.append(ele)
            ele=ele.replace("''","\\\'")
            input_hardcoded_values.append(ele.upper())

        input_hardcoded_values.sort()

        # print(input_aggregator_fun)
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r''+fun_regex_obj,'',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'AS[\s]+[\'\"].*?[\'\"]','',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub('FORMAT\([\s]*[\'\"].*?[\'\"]|FORMAT_TIMESTAMP\([\s]*[\'\"].*?[\'\"]','',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub('YYYY-MM-DD|MM\/DD\/YYYY|AMERICA\/NEW_YORK|\%M\/\%D\/\%Y|\%Y\%M\%D|\%F \%H\:\%M\:\%E6S','',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub('FORMAT[\s]+[\'\"].*?[\'\"]|INTERVAL[\s]+[\'\"].*?[\'\"]','',OutputFileObj,flags=re.I|re.DOTALL)
        special_char=['|','$','^']
        OutputFileObj=re.sub(r'\'\'|\"\"','',OutputFileObj)
        OutputFileObj=re.sub(r'\\\'','slash_quotes_replaced',OutputFileObj)
        find_hardcoded_value=re.findall(r'[\'\"].*?[\'\"]',OutputFileObj,flags=re.I|re.DOTALL)
        for ele in find_hardcoded_value:
            ele=ele.replace('slash_quotes_replaced','\\\'')
            new_ele=ele
            for i in special_char:
                new_ele=new_ele.replace(i,'\\'+i)
            if ele.isupper() or ele.islower() or re.findall(r'LOWER\('+new_ele+'\)|UPPER\('+new_ele+'\)',OutputFileObj):
                ele=ele.upper()
            else:
                if re.findall(r'[\>\<\=][\S]*'+new_ele+'',OutputFileObj):
                    pass
                else:
                    ele=ele.upper()
            # input_ele=ele.replace('\\\'','\'\'')
            output_hardcoded_values_log.append(ele)
            output_hardcoded_values.append(ele)
        output_hardcoded_values.sort()
        # print(input_hardcoded_values)
        # print(output_hardcoded_values)
        # print(output_hardcoded_values)
        if len(input_hardcoded_values)==0 and len(output_hardcoded_values)==0:
            log_result.append(Result_NA())
            result_cs_compare_hardcoded_value.append("NA")
        elif input_hardcoded_values==output_hardcoded_values:
            log_result.append(Result_Yes(input_hardcoded_values_log,output_hardcoded_values_log))
            result_cs_compare_hardcoded_value.append("Y")
        else:
            log_result.append(Result_No(input_hardcoded_values_log,output_hardcoded_values_log))
            result_cs_compare_hardcoded_value.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_compare_hardcoded_value.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def parse_sql(sql_obj,group_by_column_count,group_by_column):
    # print("UUUUU",sql_obj)
    parsed = sqlparse.parse(sql_obj)
    if parsed:
        parsed=parsed[0]
        # print(parsed.tokens)
        group_by_found=0
        for ele in parsed.tokens:
            if str(ele).strip() != '':
                # print(ele.ttype,ele)
                if group_by_found==0:
                    # print("SSSS",ele.ttype,ele)
                    if isinstance(ele, sqlparse.sql.Parenthesis) or ele.ttype is None and re.findall(r'^[\s]*\([\s]*SELECT|^[\s]*\([\s]*SEL[\s]+',str(ele),flags=re.I|re.DOTALL):
                        ele=re.sub(r'^[\s]*\(|\)[\s]*[\s]*[\w]*$','',str(ele))
                        # print("TTTTTT",ele)
                        parse_sql(str(ele),group_by_column_count,group_by_column)
                    elif re.findall(r'^GROUP[\s]+BY$',str(ele),flags=re.I|re.DOTALL):
                        group_by_found=1
                elif group_by_found == 1:
                    group_by_found=0
                    ele=str(ele)
                    ele=re.sub(r'[\s]+',' ',ele.upper(),flags=re.I|re.DOTALL)
                    ele=re.sub(r'[\s]*\,[\s]*',',',ele,flags=re.I|re.DOTALL)
                    ele=re.sub(r'QUALIFY[\s]*$','',ele,flags=re.I|re.DOTALL)
                    fun_to_remove=['LOWER','UPPER']
                    for fun in fun_to_remove:
                        find_fun_to_remove=re.findall(r'('+fun+'[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\))',ele,flags=re.I|re.DOTALL)
                        for item in find_fun_to_remove:
                            ele=ele.replace(item[0],item[1])
                    # for fun in fun_to_remove:
                    #     str_i=re.sub(r''+fun+'[\s]*\(',fun+'(',str_i,flags=re.I|re.DOTALL)
                    #     find_fun=re.findall(r''+fun+'\(',str_i,flags=re.I|re.DOTALL)
                    #     # remove_fun=str_i
                    #     for ele in find_fun:
                    #         # print(str_i)
                    #         # print(ele)
                    #         find_index=str_i.find(ele)
                    #         if find_index == -1:
                    #             break
                    #         b_o=0
                    #         b_c=0
                    #         fun_str=''
                    #         for i in str_i[find_index:]:
                    #             fun_str+=i
                    #             if i == '(':
                    #                 b_o+=1
                    #             elif i == ')':
                    #                 b_c+=1
                    #             if b_o == b_c:
                    #                 remove_fun=re.sub(r'^'+fun+'\(|\)$','',fun_str)
                    #                 str_i=str_i.replace(fun_str,remove_fun)
                    # print(str_i)
                    column_lst=[]
                    column_str=''
                    b_o=0
                    b_c=0
                    for i in ele:
                        if i == '(':
                            b_o+=1
                        elif i == ')':
                            b_c+=1
                        if i==',' and b_o == b_c:
                            b_o=0
                            b_c=0
                            column_lst.append(column_str)
                            column_str=''
                        else:
                            column_str+=i
                    column_lst.append(column_str)
                    column_lst.sort()
                    new_str=','
                    new_str=new_str.join(column_lst)
                    group_by_column_count.append(len(column_lst))
                    group_by_column.append(""+str(len(column_lst))+" Columns In GROUP BY "+new_str)

def CS_Check_Group_By_In_File(log_result, filename, result_cs_group_by):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check GROUP BY Column Count"))
    input_group_by_column=[]
    output_group_by_column=[]
    input_group_by_column_count=[]
    output_group_by_column_count=[]
    # try:
    if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        # InputFileObj=re.sub(r';[\s]*;',';',InputFileObj)
        # InputFileObj=re.sub(r'\([\s]*CASE[\s]+WHEN','CASE WHEN',InputFileObj,flags=re.I|re.DOTALL)
        # InputFileObj=re.sub(r'[\s]+END[\s]*\)',' END',InputFileObj,flags=re.I|re.DOTALL)
        group_by_column_count=[]
        group_by_column=[]
        for sql in sqlparse.split(InputFileObj):
            parse_sql(sql,group_by_column_count,group_by_column)
        input_group_by_column_count=group_by_column_count
        input_group_by_column=group_by_column
        input_group_by_column.sort()
        # print(input_group_by_column)
        # print(len(input_group_by_column))
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFileObj=re.sub(r'\([\s]*CASE[\s]+WHEN','CASE WHEN',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'[\s]+END[\s]*\)',' END',OutputFileObj,flags=re.I|re.DOTALL)
        group_by_column_count=[]
        group_by_column=[]
        for sql in sqlparse.split(OutputFileObj):
            parse_sql(sql,group_by_column_count,group_by_column)
        output_group_by_column_count=group_by_column_count
        output_group_by_column=group_by_column
        output_group_by_column.sort()
        # print(output_group_by_column)
        # print(len(output_group_by_column))
        if len(input_group_by_column)==0 and len(output_group_by_column)==0:
            log_result.append(Result_NA())
            result_cs_group_by.append("NA")
        elif input_group_by_column==output_group_by_column:
            # print("YYY")
            log_result.append(Result_Yes(input_group_by_column,output_group_by_column))
            result_cs_group_by.append("Y("+str(len(input_group_by_column_count))+"/"+str(len(output_group_by_column_count))+")")
        else:
            log_result.append(Result_No(input_group_by_column,output_group_by_column))
            result_cs_group_by.append("N("+str(len(input_group_by_column_count))+"/"+str(len(output_group_by_column_count))+")")

    # except Exception as e:
    #     print(e)
    #     print("Unexpected error:", sys.exc_info()[0])
    #     result_cs_group_by.append("Encoding")
    #     log_result.append(Result_Encoding())
    #     print("Error in processing: ", filename)

def CS_Check_Aggregator_Functions(log_result, filename, result_cs_aggregator_function):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Aggregate Function "))
    input_aggregator_fun=[]
    output_aggregator_fun=[]
    input_aggregator_dict={}
    output_aggregator_dict={}
    final_result="NA"
    aggregator_fun_lst=["COUNT", "SUM", "AVERAGE", "MIN", "MAX", "ABSOLUTE", "CEIL", "FLOOR", "TRUNCATE", "MOD", "LOWER", "UPPER", "CONCAT", "TRIM", "DATE", "TIME", "EXTRACT", "STRTOK", "RANK", "PERCENT", "CONVERT", "COALESCE", "LENGTH", "ABS", "SIGN", "GREATEST", "LEAST"]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        for fun in aggregator_fun_lst:
            find_fun_count=len(re.findall(r'[\W]'+fun+'[\s]*\(',InputFileObj,flags=re.I|re.DOTALL))
            input_aggregator_fun.append(fun+" : "+str(find_fun_count))
            input_aggregator_dict[fun]=find_fun_count

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        for fun in aggregator_fun_lst:
            find_fun_count=len(re.findall(r'[\W]'+fun+'[\s]*\(',OutputFileObj,flags=re.I|re.DOTALL))
            output_aggregator_fun.append(fun+" : "+str(find_fun_count))
            output_aggregator_dict[fun]=find_fun_count

        # print(input_aggregator_dict)
        # print(output_aggregator_dict)

        for fun,count in input_aggregator_dict.items():
            if output_aggregator_dict[fun] == 0 and count == 0 and final_result != 'Y':
                final_result="NA"
            elif output_aggregator_dict[fun] >= count:
                final_result="Y"
            else:
                final_result="N"
                break
        # output_aggregator_fun.sort()
        if final_result == "NA":
            log_result.append(Result_NA())
            result_cs_aggregator_function.append("NA")
        elif final_result == "N":
            log_result.append(Result_No(input_aggregator_fun,output_aggregator_fun))
            result_cs_aggregator_function.append("N")
        else:
            log_result.append(Result_Yes(input_aggregator_fun,output_aggregator_fun))
            result_cs_aggregator_function.append("Y")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_aggregator_function.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Parenthesis_Count_In_Input_And_Output(log_result, filename, result_cs_parenthesis_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Parenthisis Count "))
    input_parenthesis=[]
    output_parenthesis=[]
    # aggregator_fun_lst=['SUM','COUNT','AVG','MIN','MAX']
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        input_parenthesis_count=re.findall(r'\(|\)',InputFileObj,flags=re.I|re.DOTALL)

        # print(input_aggregator_fun)
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        output_parenthesis_count=re.findall(r'\(|\)',OutputFileObj,flags=re.I|re.DOTALL)

        if len(input_parenthesis_count)==0 and len(output_parenthesis_count)==0:
            log_result.append(Result_NA())
            result_cs_parenthesis_count.append("NA")
        elif len(input_parenthesis_count)==len(output_parenthesis_count):
            # print("YYY")
            log_result.append(Result_Yes(['In Input '+str(len(input_parenthesis_count))+' Parenthesis Present'],['In Output '+str(len(output_parenthesis_count))+' Parenthesis Present']))
            result_cs_parenthesis_count.append("Y("+str(len(input_parenthesis_count))+"/"+str(len(output_parenthesis_count))+")")
        else:
            log_result.append(Result_No(['In Input '+str(len(input_parenthesis_count))+' Parenthesis Present'],['In Output '+str(len(output_parenthesis_count))+' Parenthesis Present']))
            result_cs_parenthesis_count.append("N("+str(len(input_parenthesis_count))+"/"+str(len(output_parenthesis_count))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_parenthesis_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def find_where_condition(parsed,fun_and_count,fun_or_count,fun_between_count):
    for item in parsed.tokens:
        # print(item.ttype,item)
        if isinstance(item, sqlparse.sql.Where):
            where_str=str(item)
            # print("EEE",where_str)
            where_str=re.sub(r'\>\=',' >= ',where_str)
            where_str=re.sub(r'\<\=',' <= ',where_str)
            fun_and_count.extend(re.findall(r'[\W]AND[\W]',where_str,flags=re.I|re.DOTALL))
            fun_or_count.extend(re.findall(r'[\W]OR[\W]',where_str,flags=re.I|re.DOTALL))
            fun_between_count.extend(re.findall(r'[\W]BETWEEN[\W]|[\W]\>\=[\W]|[\W]\<\=[\W]',where_str,flags=re.I|re.DOTALL))
            # print(fun_between_count)
            find_where_condition(item,fun_and_count,fun_or_count,fun_between_count)
        elif isinstance(item, sqlparse.sql.Parenthesis) or item.ttype == None:
            find_where_condition(item,fun_and_count,fun_or_count,fun_between_count)
            # print(item)
            # print("AA",and_count,or_count)
    # return and_count,or_count

def CS_CLAUSES_Count_In_INPUT_And_Output(log_result, filename, result_cs_clause_count):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check CLAUSES Count In Input And Output "))
    input_clauses_count=[]
    output_clauses_count=[]
    input_clauses_lst=[]
    output_clauses_lst=[]
    # clauses_lst=['FROM','SELECT','WHERE','ORDER[\s]+BY','HAVING']
    clauses_lst=['WHERE','HAVING','QUALIFY']
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFileObj=re.sub(r'EXTRACT[\s]*\([\s]*[\w]+[\s]+FROM','EXTRACT(DATE ',InputFileObj,flags=re.I|re.DOTALL)
        InputFileObj=re.sub(r'\(sel[\s]+','( SELECT ',InputFileObj,flags=re.I|re.DOTALL)
        find_sel=re.findall(r'[\W]sel[\s]+',InputFileObj,flags=re.I|re.DOTALL)
        for ele in set(find_sel):
            new_ele=re.sub(r'sel','SELECT',ele,flags=re.I|re.DOTALL)
            InputFileObj=InputFileObj.replace(ele,new_ele)
        for clause in clauses_lst:
            find_clause=re.findall(r'[\W]'+clause+'[\W]',InputFileObj,flags=re.I|re.DOTALL)
            for i in find_clause:
                i=re.sub('^[\W]|[\W]$','',i.upper(),flags=re.DOTALL)
                i=re.sub(r'[\s]+',' ',i,flags=re.I|re.DOTALL)
                input_clauses_lst.append(i)
            clause=clause.replace('[\s]+',' ')
            input_clauses_count.append('Count of '+clause+' In Input :'+str(len(find_clause)))

        splited_sql=sqlparse.split(InputFileObj)
        fun_and_count=[]
        fun_or_count=[]
        fun_between_count=[]
        for sql_obj in splited_sql:
            parsed = sqlparse.parse(sql_obj)[0]
            find_where_condition(parsed,fun_and_count,fun_or_count,fun_between_count)
        where_cond_dict={'AND':fun_and_count,'OR':fun_or_count,'BETWEEN / >= / <=':fun_between_count}
        # print(where_cond_dict)
        between_count=0
        for clause,clause_lst in where_cond_dict.items():
            for i in clause_lst:
                i=re.sub('^[\W]|[\W]$','',i.upper(),flags=re.DOTALL)
                if i == 'BETWEEN':
                    input_clauses_lst.append('>=')
                    input_clauses_lst.append('<=')
                    between_count+=1
                else:
                    input_clauses_lst.append(i)
            input_clauses_count.append('Count of '+clause+' In Input :'+str(len(clause_lst)+between_count))
        input_clauses_lst.sort()
        # print(input_clauses_lst)
        # print(input_aggregator_fun)
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        # print("FFFFFFFFF")
        OutputFileObj=re.sub(r'EXTRACT[\s]*\([\s]*[\w]+[\s]+FROM','EXTRACT(DATE ',OutputFileObj,flags=re.I|re.DOTALL)
        for clause in clauses_lst:
            find_clause=re.findall(r'[\W]'+clause+'[\W]',OutputFileObj,flags=re.I|re.DOTALL)
            for i in find_clause:
                i=re.sub('^[\W]|[\W]$','',i.upper(),flags=re.DOTALL)
                i=re.sub(r'[\s]+',' ',i,flags=re.I|re.DOTALL)
                output_clauses_lst.append(i)
            clause=clause.replace('[\s]+',' ')
            output_clauses_count.append('Count of '+clause+' In Output :'+str(len(find_clause)))

        splited_sql=sqlparse.split(OutputFileObj)
        fun_and_count=[]
        fun_or_count=[]
        fun_between_count=[]
        for sql_obj in splited_sql:
            parsed = sqlparse.parse(sql_obj)[0]
            find_where_condition(parsed,fun_and_count,fun_or_count,fun_between_count)
        where_cond_dict={'AND':fun_and_count,'OR':fun_or_count,'BETWEEN / >= / <=':fun_between_count}
        # print(where_cond_dict)
        between_count=0
        for clause,clause_lst in where_cond_dict.items():
            for i in clause_lst:
                i=re.sub('^[\W]|[\W]$','',i.upper(),flags=re.DOTALL)
                if i == 'BETWEEN':
                    output_clauses_lst.append('>=')
                    output_clauses_lst.append('<=')
                    between_count+=1
                else:
                    output_clauses_lst.append(i)
            output_clauses_count.append('Count of '+clause+' In Input :'+str(len(clause_lst)+between_count))

        output_clauses_lst.sort()
        # print(output_clauses_lst)
        # print(input_clauses_lst)
        # print(output_clauses_lst)
        if len(input_clauses_lst)==0 and len(output_clauses_lst)==0:
            log_result.append(Result_NA())
            result_cs_clause_count.append("NA")
        elif input_clauses_lst==output_clauses_lst:
            # print("YYY")
            log_result.append(Result_Yes(input_clauses_count,output_clauses_count))
            result_cs_clause_count.append("Y("+str(len(input_clauses_lst))+"/"+str(len(output_clauses_lst))+")")
        else:
            log_result.append(Result_No(input_clauses_count,output_clauses_count))
            result_cs_clause_count.append("N("+str(len(input_clauses_lst))+"/"+str(len(output_clauses_lst))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_clause_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Check_Date_Functions(log_result, filename, result_cs_date_functions):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Date Functions "))
    input_current_date_log=[]
    output_current_date_log=[]
    input_current_date=[]
    output_current_date=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        find_curren_date_and_to_date=re.findall(r'CURRENT_DATE[\s]*[\-\+][\s]*TO_DATE[\s]*\(',InputFileObj,flags=re.I|re.DOTALL)
        date_sub_count=0
        date_add_count=0
        for ele in find_curren_date_and_to_date:
            if '-' in ele:
                date_sub_count+=1
            else:
                date_add_count+=1
        find_current_date=re.findall(r'[\W]CURRENT_DATE[\s]*[\-\+]|[\W]DATE[\s]*[\-\+]',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_current_date:
            ele=re.sub(r'^[\W]','',ele)
            input_current_date_log.append(ele)
            if ele.endswith('-'):
                if date_sub_count == 0:
                    input_current_date.append('DATE_SUB(CURRENT_DATE("AMERICA/NEW_YORK"),INTERVAL')
                else:
                    date_sub_count-=1
            else:
                if date_add_count == 0:
                    input_current_date.append('DATE_ADD(CURRENT_DATE("AMERICA/NEW_YORK"),INTERVAL')
                else:
                    date_add_count-=1

        find_last_day_and_to_date=re.findall(r'LAST_DAY[\s]*\(|TO_DATE[\s]*\(|TO_CHAR[\s]*\(',InputFileObj,flags=re.I|re.DOTALL)
        for ele in find_last_day_and_to_date:
            input_current_date_log.append(ele)
            ele=re.sub(r'TO_DATE[\s]*\(','PARSE_DATE(',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'LAST_DAY[\s]*\(','LAST_DAY(',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'TO_CHAR[\s]*\(','FORMAT_DATE(',ele,flags=re.I|re.DOTALL)
            input_current_date.append(ele)


        # print(find_current_date)
        input_current_date.sort()

        # print(input_aggregator_fun)
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        find_current_date=re.findall(r'[\W]DATE_SUB[\s]*\([\s]*CURRENT_DATE[\s]*\,[\s]*INTERVAL|[\W]DATE_ADD[\s]*\([\s]*CURRENT_DATE[\s]*\,[\s]*INTERVAL|'
                                     r'[\W]DATE_SUB[\s]*\([\s]*CURRENT_DATE[\s]*\([\s]*[\'\"][\s]*America/New_York[\s]*[\'\"]\)[\s]*\,[\s]*INTERVAL|'
                                     r'[\W]DATE_ADD[\s]*\([\s]*CURRENT_DATE[\s]*\([\s]*[\'\"][\s]*America/New_York[\s]*[\'\"]\)[\s]*\,[\s]*INTERVAL',OutputFileObj,flags=re.I|re.DOTALL)
        for ele in find_current_date:
            ele=re.sub(r'^[\W]','',ele)
            output_current_date_log.append(ele)
            if "AMERICA/NEW_YORK" not in ele.upper():
                ele=re.sub(r'\([\s]*CURRENT_DATE','(CURRENT_DATE("America/New_York")',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'[\s]+','',ele,flags=re.I|re.DOTALL)
            output_current_date.append(ele.upper())

        find_current_date=re.findall(r'[\W]CURRENT_DATE[\s]*[\-\+]|[\W]DATE[\s]*[\-\+]',OutputFileObj,flags=re.I|re.DOTALL)
        for ele in find_current_date:
            ele=re.sub(r'^[\W]','',ele)
            output_current_date_log.append(ele)
            if ele.endswith('-'):
                output_current_date.append('DATE_SUB(CURRENT_DATE("AMERICA/NEW_YORK"),INTERVAL')
            else:
                output_current_date.append('DATE_ADD(CURRENT_DATE("AMERICA/NEW_YORK"),INTERVAL')

        find_last_day_and_to_date=re.findall(r'LAST_DAY[\s]*\(|PARSE_DATE[\s]*\(|PARSE_DATETIME[\s]*\(|FORMAT_DATE[\s]*\(|FORMAT_DATETIME[\s]*\(',OutputFileObj,flags=re.I|re.DOTALL)
        for ele in find_last_day_and_to_date:
            output_current_date_log.append(ele)
            ele=re.sub(r'[\s]+','',ele.upper(),flags=re.I|re.DOTALL)
            ele=re.sub(r'PARSE_DATETIME[\s]*\(','PARSE_DATE(',ele,flags=re.I|re.DOTALL)
            ele=re.sub(r'FORMAT_DATETIME[\s]*\(','FORMAT_DATE(',ele,flags=re.I|re.DOTALL)
            output_current_date.append(ele)

        output_current_date.sort()
        # find_current_date=re.findall(r'[\W]CURRENT_DATE[\s]*[\-\+]|[\W]DATE[\s]*[\-\+]',OutputFileObj,flags=re.I|re.DOTALL)
        # print(find_current_date)
        # print(input_current_date_log)
        # print(output_current_date_log)
        # print(input_current_date)
        # print(output_current_date)
        if len(input_current_date)==0 and len(output_current_date)==0:
            log_result.append(Result_NA())
            result_cs_date_functions.append("NA")
        elif input_current_date==output_current_date:
            log_result.append(Result_Yes(input_current_date_log,output_current_date_log))
            result_cs_date_functions.append("Y")
        else:
            log_result.append(Result_No(input_current_date_log,output_current_date_log))
            result_cs_date_functions.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_date_functions.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Check_Date_Format(log_result, filename,result_cs_check_date_format):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Date Format "))
    log_input_date_format=[]
    log_output_date_format=[]
    input_date_format=[]
    output_date_format=[]
    fun_name_lst=['TO_DATE','TO_CHAR','PARSE_DATE','PARSE_DATETIME','PARSE_TIMESTAMP','FORMAT_DATE','FORMAT_DATETIME']
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        InputFunObj=''
        for ele in fun_name_lst:
            find_fun=re.findall(r''+ele+'\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)',InputFunObj,flags=re.I|re.DOTALL)
            for i in find_fun:
                OutputFileObj=OutputFileObj.replace(i,'',1)
                InputFunObj+=i
        input_format={"yyyymmdd":"%Y%m%d","mm/dd/yyyy":"%m/%d/%Y","yyyymm":"%Y%m","YYYY":"%Y"}
        input_regex=''
        for ele in input_format:
            input_regex+="[\'\"]"+ele+"[\'\"]|"
        input_regex=re.sub(r'\|$','',input_regex)
        find_date_format=re.findall(r''+input_regex,InputFunObj,flags=re.I|re.DOTALL)
        # print(find_date_format)
        for ele in find_date_format:
            log_input_date_format.append(ele)
            new_ele=ele.replace('\"','\'')
            for k,v in input_format.items():
                new_ele=re.sub(k,v,new_ele,flags=re.I|re.DOTALL)
            input_date_format.append(new_ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        OutputFunObj=''
        for ele in fun_name_lst:
            find_fun=re.findall(r''+ele+'\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)',OutputFileObj,flags=re.I|re.DOTALL)
            for i in find_fun:
                OutputFileObj=OutputFileObj.replace(i,'',1)
                OutputFunObj+=i
        output_format=["%Y%m%d","%m/%d/%Y","%Y%m","%Y"]
        output_regex=''
        for ele in output_format:
            output_regex+="[\'\"]"+ele+"[\'\"]|"
        output_regex=re.sub(r'\|$','',output_regex)
        find_date_format=re.findall(r''+output_regex,OutputFunObj,flags=re.I|re.DOTALL)
        # print(find_date_format)
        for ele in find_date_format:
            log_output_date_format.append(ele)
            new_ele=ele.replace('\"','\'')
            output_date_format.append(new_ele)

        input_date_format.sort()
        output_date_format.sort()
        # print(input_date_format)
        # print(output_date_format)
        if len(input_date_format)==0 and len(output_date_format)==0:
            log_result.append(Result_NA())
            result_cs_check_date_format.append("NA")
        elif input_date_format==output_date_format:
            log_result.append(Result_Yes(input_date_format,output_date_format))
            result_cs_check_date_format.append("Y")
        else:
            log_result.append(Result_No(input_date_format,output_date_format))
            result_cs_check_date_format.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_check_date_format.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Check_Backtick_In_BTEQ(log_result, filename,result_cs_check_backtick_in_BTEQ):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check '`' In File "))
    final_result='Y'
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        find_tilt=re.findall(r'\`',OutputFileObj,flags=re.I|re.DOTALL)
        if find_tilt:
            final_result="N"

        if final_result == "N":
            log_result.append(Output_Result_No(['` Present In Outputfile']))
            result_cs_check_backtick_in_BTEQ.append("N")
        else:
            log_result.append(Output_Result_Yes(['` Not Present In Outputfile']))
            result_cs_check_backtick_in_BTEQ.append("Y")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_check_backtick_in_BTEQ.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Count_Doller_Variable(log_result, filename,result_cs_count_doller_variable):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check $ Variable Count "))
    input_doller_variable=[]
    output_doller_variable=[]
    final_result='Y'
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        InputFileObj+=fileline
        InputFileObj+=';'
        find_input_doller_variable=re.findall(r'\${1,}[A-Za-z0-9_\-\{\}\$]+',InputFileObj,flags=re.I|re.DOTALL)
        input_doller_variable=find_input_doller_variable
        # print(input_doller_variable)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        find_output_doller_variable=re.findall(r'\${1,}[A-Za-z0-9_\-\{\}\$]+',OutputFileObj,flags=re.I|re.DOTALL)
        output_doller_variable=[var for var in find_output_doller_variable if var != Append_project_id]
        # print(output_doller_variable)

        if len(input_doller_variable)==0 and len(output_doller_variable)==0:
            log_result.append(Result_NA())
            result_cs_count_doller_variable.append("NA")
        elif len(output_doller_variable)>=len(input_doller_variable):
            log_result.append(Result_Yes(input_doller_variable,output_doller_variable))
            result_cs_count_doller_variable.append("Y("+str(len(input_doller_variable))+"/"+str(len(output_doller_variable))+")")
        else:
            log_result.append(Result_No(input_doller_variable,output_doller_variable))
            result_cs_count_doller_variable.append("N("+str(len(input_doller_variable))+"/"+str(len(output_doller_variable))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_count_doller_variable.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def CS_Check_IF_FALSE_In_Output(log_result, filename,result_cs_check_if_false_in_output):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check IF(false) In Output File "))
    final_result='Y'
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        find_if_false=re.findall(r'IF\([\s]*FALSE[\s]*\)',OutputFileObj,flags=re.I|re.DOTALL)
        if find_if_false:
            final_result="N"

        if final_result == "N":
            log_result.append(Output_Result_No(find_if_false))
            result_cs_check_if_false_in_output.append("N")
        else:
            log_result.append(Output_Result_Yes(['IF(false) Not Present In Output file']))
            result_cs_check_if_false_in_output.append("Y")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_check_if_false_in_output.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

join_type_lst=['inner join','left join','right join','left outer join','right outer join','join','cross join']
def split_sql_statements(fileobj):
    return sqlparse.split(fileobj)

def parse_sql_obj(sql_obj):
    try:
        return parse(sql_obj)
    except Exception as e:
        print("Exception_Raised",e)
        pass

def iterate_dict(parsed_dict,where_condition_lst,where_and_table_dict):
    for key,val in parsed_dict.items():
        if isinstance(val,dict):
            find_where_and_on_condition(val,where_condition_lst,where_and_table_dict)
        elif isinstance(val,list):
            for ele in val:
                if isinstance(ele,dict):
                    find_where_and_on_condition(ele,where_condition_lst,where_and_table_dict)

def find_from_in_subquery(ele,used_table_lst):
    if isinstance(ele,dict):
        for k,v in ele.items():
            if k == 'from':
                if isinstance(v,str):
                    used_table_lst.append(v)
                elif isinstance(v,str):
                    find_tables_used(v)
            elif isinstance(v,dict):
                find_from_in_subquery(v,used_table_lst)
            elif isinstance(v,list):
                for x in v:
                    find_from_in_subquery(x,used_table_lst)



def iterate_for_table(ele,used_table_lst):
    if isinstance(ele,dict):
        is_join=list(set(join_type_lst).intersection(set(ele)))
        if 'value' in ele:
            val_ele=ele['value']
            if isinstance(val_ele,str):
                used_table_lst.append(ele['value'])
            elif isinstance(val_ele,dict) and 'from' in val_ele:
                if isinstance(val_ele['from'],str):
                    used_table_lst.append(val_ele['from'])
                elif isinstance(val_ele['from'],dict):
                    find_tables_used(val_ele['from'],used_table_lst)
                val_ele.pop('from')
                find_from_in_subquery(val_ele,used_table_lst)
        elif is_join:
            join_val=ele[is_join[0]]
            if isinstance(join_val,str):
                used_table_lst.append(join_val)
            else:
                find_tables_used(join_val,used_table_lst)
        else:
            for k,k_v in ele.items():
                if k == 'from':
                    for x in k_v:
                        iterate_for_table(x,used_table_lst)


def find_tables_used(table_obj_dict,used_table_lst):
    if isinstance(table_obj_dict,dict):
        for k,ele in table_obj_dict.items():
            if k == 'value' and isinstance(ele,str):
                used_table_lst.append(ele)
            else:
                iterate_for_table(ele,used_table_lst)
    elif isinstance(table_obj_dict,list):
        for ele in table_obj_dict:
            iterate_for_table(ele,used_table_lst)


def find_where_and_on_condition(parsed_obj,where_condition_lst,where_and_table_dict):
    if 'where' in parsed_obj:
        where_condition_lst.append(parsed_obj['where'])
        if 'from' in parsed_obj:
            used_table_lst=[]
            from_obj=parsed_obj['from']
            find_tables_used(from_obj,used_table_lst)
            where_and_table_dict[len(where_and_table_dict)]=[parsed_obj['where'],used_table_lst]
    elif 'on' in parsed_obj:
        where_condition_lst.append(parsed_obj['on'])
        is_join=list(set(join_type_lst).intersection(set(parsed_obj)))
        if is_join:
            used_table_lst=[]
            from_obj=parsed_obj[is_join[0]]
            find_tables_used(from_obj,used_table_lst)
            where_and_table_dict[len(where_and_table_dict)]=[parsed_obj['on'],used_table_lst]
    iterate_dict(parsed_obj,where_condition_lst,where_and_table_dict)

def iterdict(d,correct_lst,wrong_lst,string_column_lst):
    if isinstance(d, dict):
        for k,v in d.items():
            if k in ['neq','eq']:
                # print("TTT",k,v)
                a={}
                a[k]=v
                if ('literal' in str(v) and not "'date'" in str(v) and not "'format'" in str(v)) or (any(ele in str(v[0]).lower() or ele in str(v[1]).lower() for ele in string_column_lst) ):
                    if 'lower' in str(v[0]).lower() and 'lower' in str(v[1]).lower() and 'trim' in str(v[0]).lower() and 'trim' in str(v[1]).lower():
                        correct_lst.append(mo_sql_format(a))
                    elif 'upper' in str(v[0]).lower() and 'upper' in str(v[1]).lower() and 'trim' in str(v[0]).lower() and 'trim' in str(v[1]).lower():
                        correct_lst.append(mo_sql_format(a))
                    else:
                        wrong_lst.append(mo_sql_format(a))
            if isinstance(v, dict):
                iterdict(v,correct_lst,wrong_lst,string_column_lst)
            elif isinstance(v, list):
                for i in v:
                    if isinstance(i, dict):
                        iterdict(i,correct_lst,wrong_lst,string_column_lst)

    elif isinstance(d, list):
        for i in d:
            if isinstance(i, dict):
                iterdict(i,correct_lst,wrong_lst,string_column_lst)

def CS_Check_Lower_Upper_Trim_In_Where(log_result, filename,result_cs_check_lower_upper_trim_in_where,DB_Conection):
    # print(filename)
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check LOWER/UPPER/TRIM In Output File "))
    all_correct_where_condition=[]
    all_wrong_where_condition=[]
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
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
                            fileline=fileline.split('--')[0]+'\n'
                        OutputFileObj+=fileline
        OutputFileObj+=';'
        where_and_table_dict={}
        counter=0
        splited_sql=split_sql_statements(OutputFileObj)
        for sql_obj in splited_sql:
            if sql_obj.strip() == '' or sql_obj.strip() == ';' :
                pass
            else:
                parsed_obj=parse_sql_obj(sql_obj)
                if parsed_obj is not None:
                    where_condition_lst=[]
                    find_where_and_on_condition(parsed_obj,where_condition_lst,where_and_table_dict)

        for k,where_table in where_and_table_dict.items():
            where_cond=where_table[0]
            tables_lst=where_table[1]
            cursor=DB_Conection.cursor()
            string_column_lst=[]
            query_to_run="SELECT name,data_type FROM 'column_e' WHERE ("
            if tables_lst:
                for table in tables_lst:
                    table=table.replace('..','.')
                    split_table=table.split('.')
                    if len(split_table) == 3:
                        split_table.pop(0)
                    if len(split_table) == 2:
                        query_to_run+=" lower(schema)=lower('"+split_table[0]+"') and lower(object_name)=lower('"+split_table[1]+"') OR "
                query_to_run=re.sub(r'OR $',") AND data_type in('CHAR','VARCHAR');",query_to_run)
                # print(query_to_run)
                cursor.execute(query_to_run)
                rows = cursor.fetchall()
                for row in rows:
                    # print("RRR",row)
                    string_column_lst.append(row[0].lower())
            string_column_lst=list(set(string_column_lst))
            # print("%%%%",string_column_lst)
            correct_lst=[]
            wrong_lst=[]
            iterdict(where_cond,correct_lst,wrong_lst,string_column_lst)
            all_correct_where_condition.extend(correct_lst)
            all_wrong_where_condition.extend(wrong_lst)
            # print("CCCC",correct_lst)
            # print("FFFF",wrong_lst)


        if len(all_correct_where_condition) == 0 and len(all_wrong_where_condition) == 0:
            log_result.append(Result_NA())
            result_cs_check_lower_upper_trim_in_where.append("NA")
        elif all_wrong_where_condition:
            log_result.append(Output_Result_No(all_wrong_where_condition))
            result_cs_check_lower_upper_trim_in_where.append("N")
        elif all_correct_where_condition:
            log_result.append(Output_Result_Yes(all_correct_where_condition))
            result_cs_check_lower_upper_trim_in_where.append("Y")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cs_check_lower_upper_trim_in_where.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def IF_Activity(log_result,filename,result_for_if_activity):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common(".IF ACTIVITYCOUNT"))
    flag=0
    try:
        # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--",fileLine)
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
        input_if_activity=re.findall(r'\.IF[\s]+ACTIVITYCOUNT',obj,flags=re.I|re.DOTALL)
        # print(input_if_activity)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*",fileLine)
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
        output_if_activity=re.findall(r'SET[\s]+activityCount_var[\s]*\=[\s]*\([\s]*SELECT[\s]+COUNT\(\*\)[\s]+AS[\s]+\`__count__\`',obj,flags=re.I|re.DOTALL)
        # print(output_if_activity)
        if len(input_if_activity)==0 and len(output_if_activity)==0:
            log_result.append(Result_NA())
            result_for_if_activity.append("NA")
        elif len(input_if_activity)==len(output_if_activity):
            log_result.append(Result_Yes(input_if_activity,output_if_activity))
            result_for_if_activity.append("Y")
        else:
            log_result.append(Result_No(input_if_activity,output_if_activity))
            result_for_if_activity.append("N")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_if_activity.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def null_and_not_null(log_result,filename,result_for_null,result_for_not_null):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("IS NULL"))
    flag=0
    # print(filename)
    try:
        # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.read()
            fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
            fileLines = fileLines.splitlines(True)
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--",fileLine)
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
        input_is_null=re.findall(r'IS[\s]+NULL[\s]*|ZEROIFNULL',obj,flags=re.I|re.DOTALL)
        input_is_not_null=re.findall(r'\@*\@*[\w]+[\s]+IS[\s]+NOT[\s]+NULL[\s]*|\)[\s]*IS[\s]+NOT[\s]+NULL',obj,flags=re.I|re.DOTALL)
        # print(len(input_is_not_null))

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                fileLines=re.sub(r'\/\*.*?\*/','',fileLines,flags=re.I|re.DOTALL)
                fileLines = fileLines.splitlines(True)
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--|^[\s]*BEGIN[\s]*$|^[\s]*END[\s]*$",fileLine)
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
        output_is_null=re.findall(r'IS[\s]+NULL[\s]*',obj,flags=re.I|re.DOTALL)
        zero_if_null=re.findall(r'IFNULL(\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\))',obj,flags=re.I|re.DOTALL)
        for ele in zero_if_null:
            find_ends_with_zero=re.findall(r'\,[\s]*0[\s]*\)$',ele,flags=re.I|re.DOTALL)
            if find_ends_with_zero:
                output_is_null.append("IFNULL"+ele)
        # print(output_is_null)
        output_is_not_null=re.findall(r'\@*\@*[\w]+[\s]+IS[\s]+NOT[\s]+NULL[\s]*|\)[\s]*IS[\s]+NOT[\s]+NULL',obj,flags=re.I|re.DOTALL)
        output_is_not_null1=[]
        for i in output_is_not_null:
            if "@@row_count" in i:
                pass
            else:
                output_is_not_null1.append(i)

        # print(len(output_is_not_null1))

        #for IS NULL
        if len(input_is_null)==0 and len(output_is_null)==0:
            log_result.append(Result_NA())
            result_for_null.append("NA")
        elif len(input_is_null)==len(output_is_null):
            log_result.append(Result_Yes(input_is_null,output_is_null))
            result_for_null.append("Y")
        else:
            log_result.append(Result_No(input_is_null,output_is_null))
            result_for_null.append("N")


        #For IS NOT NULL
        log_result.append(Common("IS NOT NULL"))
        if len(input_is_not_null)==0 and len(output_is_not_null1)==0:
            log_result.append(Result_NA())
            result_for_not_null.append("NA")
        elif len(input_is_not_null)==len(output_is_not_null1):
            log_result.append(Result_Yes(input_is_not_null,output_is_not_null1))
            result_for_not_null.append("Y")
        else:
            log_result.append(Result_No(input_is_not_null,output_is_not_null1))
            result_for_not_null.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_null.append("Encoding")
        result_for_not_null.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def on_after_join(log_result,filename,result_for_on_condition):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("ON Condition of JOIN Clause"))
    flag=0
    input_lst=[]
    output_lst=[]
    try:
        # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--",fileLine)
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
        input_obj_lst=obj.split(";")
        # print(input_obj_lst)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*",fileLine)
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
        output_obj_lst=obj.split(";")

        # print("Input  : ")
        for no in range(0,len(input_obj_lst)):
            #For Input File
            for_input_stat=[]
            if input_obj_lst[no]=="":
                pass
            elif "ON" in input_obj_lst[no].upper():
                # find_ele=input_obj_lst[no].upper().split("WHERE")
                find_ele=re.split("WHERE|SELECT|JOIN",input_obj_lst[no],flags=re.I|re.DOTALL)
                for ele in find_ele:
                    if "ON" in ele.upper():
                        find_join=re.findall(r"[\s]+ON[\s]+(.*)",ele,flags=re.I|re.DOTALL)
                        for i in find_join:
                            # if "SELECT" in i.upper():
                            #     pass
                            # else:
                            find_tbl_nm=re.findall(r"[\w]+[\s]*\.[\s]*([\w]+)",i,flags=re.I|re.DOTALL)
                            # print(i)
                            # print(find_tbl_nm)
                            for tbl in find_tbl_nm:
                                input_lst.append(tbl.upper())


        for no in range(0,len(output_obj_lst)):
            #For Output File
            for_output_stat=[]
            if output_obj_lst[no]=="":
                pass
            elif "ON" in output_obj_lst[no].upper():
                # find_ele=output_obj_lst[no].upper().split("WHERE")
                find_ele=re.split("WHERE|SELECT|JOIN",output_obj_lst[no],flags=re.I|re.DOTALL)
                for ele in find_ele:
                    if "ON" in ele.upper():
                        find_join=re.findall(r"[\s]+ON[\s]+(.*)",ele,flags=re.I|re.DOTALL)
                        for i in find_join:
                            # if "SELECT" in i.upper():
                            #     pass
                            # else:
                            find_tbl_nm=re.findall(r"[\w]+[\s]*\.[\s]*([\w]+)",i,flags=re.I|re.DOTALL)
                            # print(i)
                            # print(find_tbl_nm)
                            for tbl in find_tbl_nm:
                                output_lst.append(tbl.upper())


        input_lst.sort()
        output_lst.sort()
        # print("Input :",input_lst)
        # print("Output:",output_lst)

        if len(input_lst)==0 and len(output_lst)==0:
            log_result.append(Result_NA())
            result_for_on_condition.append("NA")
        elif len(input_lst)==0 and len(output_lst)>0:
            log_result.append(Result_CM("ON condition not present in input file but in output file ON condition present."))
            result_for_on_condition.append("CM")
        elif input_lst==output_lst:
            log_result.append(Result_Yes(input_lst,output_lst))
            result_for_on_condition.append("Y")
        else:
            log_result.append(Result_No(input_lst,output_lst))
            result_for_on_condition.append("N")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_on_condition.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Default_Value_For_TD(log_result,filename,result_for_default_value):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("DEFAULT Value"))
    try:
    # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            table_obj=''
            table_counter=0
            table_dict={}
            input_lst=[]
            input_log_lst=[]
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--",fileLine)
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
                # elif "--" in fileLine:
                #     fileLine=fileLine.split("--")
                #     obj=obj+fileLine[0]
                else:
                    if table_counter==0:
                        find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+SET[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                              r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                              r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]+[\w]+\.[\w]+[\s]+|"
                                              r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+|"
                                              r"CREATE[\s]+MULTISET[\s]+TABLE[\s]+[\w]+\.[\w]+[\s]+",fileLine,flags=re.I)
                        if find_table:
                            # print(find_table)
                            table_obj=fileLine
                            table_counter=1
                            # name1=re.findall(r'[\w]+\.([\w]+)',find_table[0],flags=re.I)
                            # name2=re.findall(r'TABLE[\s]+([\w]+)[\s]+',find_table[0],flags=re.I)
                            # if name1:
                            #     key_name=name1[0].lower()
                            # elif name2:
                            #     key_name=name2[0].lower()
                            # print(key_name)
                    elif table_counter==1:
                        table_obj=table_obj+fileLine
                        find_end=re.findall(r';[\s]*$',fileLine,flags=re.DOTALL)
                        if find_end:
                            table_counter=0
                            # print(table_obj)
                            table_obj=re.findall(r'\((.*)',table_obj,flags=re.DOTALL)[0]
                            table_obj=re.sub(r'UNIQUE[\s]+PRIMARY[\s]+INDEX[\s]+.*|PRIMARY[\s]+INDEX[\s]+.*|PARTITION[\s]+BY.*','',table_obj,flags=re.I|re.DOTALL)
                            # print(table_obj)
                            find_col=re.split("\,[\s]*\n",table_obj,flags=re.DOTALL)
                            # print(find_col)
                            for ele in find_col:
                                col_nm=re.split(r'[\s]+',ele.strip())[0]
                                # print(col_nm)
                                find_def=re.findall(r'[\s]+DEFAULT[\s]+',ele,flags=re.I|re.DOTALL)
                                find_def_as=re.findall(r'[\s]+DEFAULT[\s]+AS[\s]*',ele,flags=re.I|re.DOTALL)
                                # print(find_def_as,ele)
                                if find_def_as:
                                    pass
                                elif find_def:
                                    if "COMPRESS" in ele:
                                        find_def=re.findall(r'DEFAULT(.*)COMPRESS',ele,flags=re.I)[0].strip()
                                        # print(col_nm,find_def)
                                        input_lst.append(col_nm.upper()+"__"+find_def)
                                    else:
                                        find_def=re.findall(r'DEFAULT(.*)',ele,flags=re.I)[0].strip()
                                        # print(col_nm,find_def)
                                        input_lst.append(col_nm.upper()+"__"+find_def)
                                    input_log_lst.append(ele)
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            counter=0
            comment=0
            obj=''
            table_obj=''
            table_counter=0
            output_lst=[]
            output_log_lst=[]
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*",fileLine)
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
                    if table_counter==0:
                        table_obj=''
                        find_table=re.findall(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+[\W]+|"
                                              r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+[\W]+|"
                                              r"CREATE[\s]+TEMP[\s]+TABLE[\s]+[\w]+[\s]+|"
                                              r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\W]+|"
                                              r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+\`*[\w]+\.[\w]+\`*[\W]+|"
                                              r"CREATE[\s]+TABLE[\s]+\`*[\w]+\.[\w]+\`*[\W]+",fileLine,flags=re.I)
                        if find_table:
                            table_obj=fileLine
                            table_counter=1
                            # name1=re.findall(r'[\w]+\.([\w]+)',find_table[0],flags=re.I)
                            # name2=re.findall(r'EXISTS[\s]+([\w]+)[\s]+|TABLE[\s]+([\w]+)[\s]+',find_table[0],flags=re.I)
                            # if name1:
                            #     key_name=name1[0].lower()
                            # elif name2:
                            #     for a in name2:
                            #         for b in a:
                            #             if b=='' or b=='IF' or b=='if':
                            #                 pass
                            #             else:
                            #                 key_name=b.lower()
                    elif table_counter==1:
                        table_obj=table_obj+fileLine
                        find_end=re.findall(r';[\s]*$',fileLine,flags=re.DOTALL)
                        if find_end:
                            table_counter=0
                            # print(table_obj)
                            table_obj=re.findall(r'\((.*)',table_obj,flags=re.DOTALL)[0]
                            table_obj=re.sub(r'CLUSTER[\s]+BY.*|PARTITION[\s]+BY.*','',table_obj,flags=re.I|re.DOTALL)
                            # print(table_obj)
                            find_col=re.split("\,[\s]*\n",table_obj,flags=re.DOTALL)
                            # print(find_col)
                            for ele in find_col:
                                col_nm=re.split(r'[\s]+',ele.strip())[0]
                                # print(col_nm)
                                find_def=re.findall(r'[\s]+DEFAULT[\s]+',ele,flags=re.I|re.DOTALL)
                                if find_def:
                                    if "NOT NULL" in ele:
                                        find_after=re.findall(r'DEFAULT(.*)NOT[\s]+NULL',ele,flags=re.I)
                                        if find_after:
                                            output_lst.append(col_nm.upper()+"__"+find_after[0].strip())
                                        else:
                                            find_def=re.findall(r'DEFAULT(.*)',ele,flags=re.I)[0].strip()
                                            output_lst.append(col_nm.upper()+"__"+find_def)
                                    else:
                                        find_def=re.findall(r'DEFAULT(.*)',ele,flags=re.I)[0].strip()
                                        output_lst.append(col_nm.upper()+"__"+find_def)
                                    output_log_lst.append(ele)
        # print(input_lst)
        # print(output_lst)
        if input_lst and output_lst:
            input_lst[len(input_lst)-1]=re.sub(r'\)$','',input_lst[len(input_lst)-1]).strip()
            output_lst[len(output_lst)-1]=re.sub(r'\)$','',output_lst[len(output_lst)-1]).strip()
        # print(input_lst)
        # print(output_lst)

        if len(input_lst)==0 and len(output_lst)==0:
            log_result.append(Result_NA())
            result_for_default_value.append("NA")
        elif input_lst==output_lst:
            log_result.append(Result_Yes(input_log_lst,output_log_lst))
            result_for_default_value.append("Y")
        else:
            # print(input_lst==output_lst)
            log_result.append(Result_No(input_log_lst,output_log_lst))
            result_for_default_value.append("N")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_default_value.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Timezone(log_result,filename,result_Check_Timezone):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Timezone"))
    input_lst=[]
    output_lst=[]
    try:
        # if 1==1:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            comment=0
            obj=''
            for fileLine in fileLines:
                cmt=re.findall(r"^[\s]*--|_current",fileLine, flags=re.I)
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
            # print("obj:", obj)
            find_current_in=re.findall(r"CURRENT_TIME\(\"America\/New_York\"\)|CURRENT_TIME\(\'America\/New_York\'\)|CURRENT_DATETIME\(\"America\/New_York\"\)|CURRENT_DATETIME\(\'America\/New_York\'\)|"
                                       r"CURRENT_TIME[\s]*[\W]+|CURRENT_DATETIME[\s]*[\W]+|CURRENT_DATE[\s]*[\W]+|CURRENT_TIMESTAMP[\s]*[\W]+", obj, flags=re.I)
            # print("find_current_in", len(find_current_in))

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*|_current",fileLine, flags=re.I)
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
                find_current_op=re.findall(r"CURRENT_TIME\(\"America\/New_York\"\)|CURRENT_DATETIME\(\"America\/New_York\"\)|CURRENT_DATE\(\"America\/New_York\"\)|"
                                           r"CURRENT_TIME\(\'America\/New_York\'\)|CURRENT_DATETIME\(\'America\/New_York\'\)|CURRENT_DATE\(\'America\/New_York\'\)", obj, flags=re.I)
                # print("find_current_op", len(find_current_op))

        if len(find_current_in)==0 and len(find_current_op)==0:
            log_result.append(Result_NA())
            result_Check_Timezone.append("NA")
        elif len(find_current_in)==len(find_current_op):
            log_result.append(Result_Yes(find_current_in,find_current_op))
            result_Check_Timezone.append("Y")
        else:
            log_result.append(Result_No(find_current_in,find_current_op))
            result_Check_Timezone.append("N")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_Timezone.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

#CSWG
def Primary_Index_To_Cluster_By_CSWG(log_result,filename,result_Primary_Index_To_Cluster_By_CSWG):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Index Checking CSWG"))
    # try:
    Log_input=[]
    Log_output=[]
    input_list=[]
    output_list=[]
    index_col_list = []
    cluster_col_list = []
    if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                table_obj=''
                table_counter=0
                
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:  #table_counter
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
                    else:
                        if table_counter==0:
                            table_obj=''
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]*|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE[\s]*",fileLine,flags=re.I)
                            if find_table:
                                # print("find_table", find_table)
                                table_obj=table_obj+fileLine
                                table_counter=1

                        elif table_counter == 1:
                            if ";" in fileLine:
                                table_obj = table_obj+fileLine
                                table_counter = 0
                                # print("table_obj", table_obj)
                                find_primary_index = re.findall(r"PRIMARY[\s]+INDEX[\s]*\((.*?)\)", table_obj, flags=re.I|re.DOTALL)
                                # print(find_primary_index)
                                
                                if find_primary_index:
                                    for ele in find_primary_index[0].split(','):
                                        Log_input.append(ele.lower())
                                        ele = re.sub(r"\n|[\s]*", "", ele)
                                        index_col_list.append(ele.lower())

                            else:
                                table_obj = table_obj + fileLine

                input_list = index_col_list[:4]
                # print(input_list)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                table_obj=''
                table_counter=0
                
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:  #table_counter
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
                    else:
                        if table_counter==0:
                            # print(fileLine)
                            table_obj=''
                            find_table=re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+SET[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+[\s]*|"
                                                  r"CREATE[\s]+TEMP[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+TABLE[\s]*|"
                                                  r"CREATE[\s]+MULTISET[\s]+TABLE[\s]*",fileLine,flags=re.I)
                            if find_table:
                                # print("find_table", find_table)
                                table_obj = table_obj + fileLine
                                table_counter = 1

                        elif table_counter == 1:
                            if ";" in fileLine:
                                table_obj = table_obj+fileLine
                                table_counter = 0
                                # print("table_obj", table_obj)
                                find_cluster_by_col = re.findall(r"CLUSTER[\s]+BY(.*?)\;", table_obj, flags=re.I|re.DOTALL)
                                # print(find_cluster_by_col) 
                                
                                if find_cluster_by_col:
                                    for ele in find_cluster_by_col[0].split(','):
                                        Log_output.append(ele.lower())
                                        ele = re.sub(r"\n|[\s]*", "", ele)
                                        cluster_col_list.append(ele.lower())
                                
                            else:
                                table_obj = table_obj + fileLine

                output_list = cluster_col_list
                # print(output_list)

        # print("input_list", input_list)
        # print("output_list", output_list)
       
        if len(input_list) == 0 and len(output_list) == 0:
            log_result.append(Result_NA())
            result_Primary_Index_To_Cluster_By_CSWG.append("NA")
        elif input_list == output_list:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Primary_Index_To_Cluster_By_CSWG.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Primary_Index_To_Cluster_By_CSWG.append("N")

def Joins_CSWG(log_result,filename,result_Joins_CSWG):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Joins"))
    # try:
    Log_input=[]
    Log_output=[]
    input_list=[]
    output_list=[]
    index_col_list = []
    cluster_col_list = []
    if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                table_obj=''
                table_counter=0
                
                for fileLine in fileLines:
                    cmt=re.findall(r'^[\s]*--',fileLine)
                    if "/*" in fileLine:
                        if '*/' in fileLine:  #table_counter
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
                    else:
                        # print(fileLine)
                        find_joins = re.findall(r"LEFT[\s]+JOIN|RIGHT[\s]+JOIN|OUTER[\s]+JOIN|CROSS[\s]+JOIN|JOIN", fileLine, flags=re.I)
                        if find_joins:
                            for ele in find_joins:
                                Log_input.append(ele)
                                input_list.append(ele)
        # print(input_list)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                table_obj=''
                table_counter=0
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
                    else:
                        find_joins = re.findall(r"LEFT[\s]+JOIN|RIGHT[\s]+JOIN|OUTER[\s]+JOIN|CROSS[\s]+JOIN|JOIN", fileLine, flags=re.I)
                        # print("find_joins", find_joins)
                        if find_joins:
                            for ele in find_joins:
                                output_list.append(ele)
                                Log_output.append(ele)

        # print("input_list", input_list)
        # print("output_list", output_list)
       
        if len(input_list) == 0 and len(output_list) == 0:
            log_result.append(Result_NA())
            result_Joins_CSWG.append("NA")
        elif len(input_list) == len(output_list):
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Joins_CSWG.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Joins_CSWG.append("N")

def Check_No_Of_Statements_For_CSWG(log_result,filename,result_no_of_statement_cswg):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Number of Statements in Script"))
    with_Append_project_id_Final_input_sql_statements=[]
    with_Append_project_id_Final_output_sql_statements=[]
    all_input_file_statements=[]
    all_output_file_statements=[]
    EDW_BASE="`{{params.ProjectID}}.{{params.BaseDataset1}}`"
    EDW_STG="`{{params.ProjectID}}.{{params.StgDataset}}`"
    EDW_BASEQ_TEMPQ="`{{params.ProjectID}}.{{params.TempDataset}}`"
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                obj=''
                for fileLine in fileLines:
                    cmt=re.findall(r"^[\s]*--",fileLine)
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
                    elif cmt or fileLine.strip().upper()=="BEGIN":
                        pass
                    elif "--" in fileLine:
                        fileLine=fileLine.split("--")
                        obj=obj+fileLine[0]
                    else:
                        obj=obj+fileLine

                file_obj1=obj.split(";")
                file_obj=[]
                for ele in file_obj1:
                    file_obj.append(ele.strip())
                # print(file_obj)
                for element in file_obj:
                    # print("element:", element)
                    input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+[\s]*|CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE[\s]*[\w]+[\s]*"
                                                    r"|^WITH[\s]+CTE[\s]+[\w]+[\s]+|REPLACE[\s]+MACRO[\s]*[\w]+\.[\w]+|CREATE[\s]+VOLATILE[\s]+"
                                                    r"TABLE[\s]*[\w]+[\s]*|REPLACE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|CREATE[\s]+VIEW[\s]*[\w]+\.[\w]+"
                                                    r"|REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+\.[\w]+|^INSERT[\s]+INTO[\s]+[\w]+[\s]+|^INSERT[\s]+INTO[\s]+[\w]+\(|^INS[\s]+[\w]+\.[\w]+|^INS[\s]+[\w]+[\s]+|^INS[\s]+[\w]+\(|"
                                                    r"^UPDATE[\s]*[\w]+\.[\w]+|^UPDATE[\s]+[\w]+[\s]+|^UPD[\s]*[\w]+\.[\w]+|^UPD[\s]+[\w]+[\s]+"
                                                    r"|DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;|DELETE[\s]+FROM[\s]+[\w]+[\s]+|"
                                                    r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+FROM[\s]+[\w]+|DEL[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+[\s]*\;|DEL[\s]+FROM[\s]+[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]*[\w]+[\s]+|DROP[\s]+TABLE[\s]*[\w]+[\s]*|"
                                                    r"MERGE[\s]+[\w]+\.[\w]+|CREATE[\s]+SET[\s]+TABLE[\s]+[\w]+\.[\w]+|"
                                                    r"CREATE[\s]+MULTISET[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+PROCEDURE[\s]*[\w]+\.[\w]+|"
                                                    r"CREATE[\s]+FUNCTION[\s]*[\w]+\.[\w]+",element,flags=re.I)   #|CALL[\s]+[\w]+\.[\w]+
                    # print("input_sql_statements", input_sql_statements)
                    # if input_sql_statements:
                    obj=re.sub("[\s]+SELECT|SELECT[\s]+","SELECT ",element,flags=re.I)
                    if obj.upper().startswith("SELECT ") or obj.upper().startswith("SEL "):
                        # print(obj)
                        find_select=re.findall(r"^SELECT[\s]+|^SEL[\s]+",obj,flags=re.I)
                        if find_select:
                            # print(find_select[0])
                            if "SEL " in find_select[0].upper():
                                find_select[0]=re.sub(r"SEL","SELECT",find_select[0])
                            find_select=re.sub(r"[\s]+","",find_select[0])

                            with_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                        all_input_file_statements.append(obj.upper())
                    for item in input_sql_statements:
                        remove_space=re.sub(r"[\s]+"," ",item)
                        all_input_file_statements.append(remove_space.upper())
                        # print("item", item)
                        schema=re.findall(r'[\s]+([\w]+)\.[\w]+',item)
                        if schema:
                            if schema[0].upper() == "EDW_BASE" or schema[0].upper() == "EDW_BASEQ":
                                item=item.replace(schema[0],"{{params.ProjectID}}.{{params.BaseDataset1}}")
                            elif schema[0].upper() == "EDW_STG" or schema[0].upper() == "EDW_STGQ":
                                item=item.replace(schema[0],"{{params.ProjectID}}.{{params.StgDataset}}")
                            else:
                                item=item.replace(schema[0],"{{params.ProjectID}}.{{params.BaseDataset2}}")
                        # print(schema)

                        find_insert_for_volatile_table=re.findall(r"INSERT[\s]+INTO[\s]+[\w]+[\s]*", item, flags=re.I)
                        # print(find_insert_for_volatile_table)
                        if find_insert_for_volatile_table:
                            item=re.sub(r"INSERT[\s]+INTO[\s]+", 'INSERT INTO {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.', item, flags=re.I)
                        item=re.sub(r"\n","",item)
                        # print("item", item)
                        item=re.sub(r"CREATE[\s]+SET[\s]+TABLE[\s]+|CREATE[\s]+MULTISET[\s]+TABLE[\s]+|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE OR REPLACE TABLE {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.",item,flags=re.I)
                        item=re.sub(r"INSERT[\s]+INTO[\s]+|^INS[\s]+","INSERT INTO ",item,flags=re.I)
                        item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                        item=re.sub(r"^SEL[\s]+","SELECT ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                        item=re.sub(r"REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                        item=re.sub(r"CREATE[\s]+VIEW[\s]+","CREATE VIEW IF NOT EXISTS ",item,flags=re.I)
                        item=re.sub(r"DROP[\s]+TABLE","DROP TABLE IF EXISTS {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.",item,flags=re.I)
                        find_delete=re.findall(r"DELETE[\s]+FROM|DEL[\s]+FROM|DELETE[\s]+",item,flags=re.I)
                        if find_delete:
                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+\.[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+\.[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+|DELETE[\s]+[\w]+|DELETE[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                
                                if '{{' in item:
                                    pass
                                else:
                                    item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.",item,flags=re.I)

                            if "WHERE " in obj.upper() or "WHERE\n" in obj.upper() and re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+\.[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+\.[\w]+|DEL[\s]+[\w]+\.[\w]+",item,flags=re.I|re.DOTALL):
                                item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DELETE FROM ",item,flags=re.I)
                            elif re.findall(r"DEL[\s]+FROM[\s]+[\w]+[\s]+|DEL[\s]+FROM[\s]+[\w]+|DEL[\s]+[\w]+|DEL[\s]+FROM[\s]+[\w]+[\s]+",item,flags=re.I|re.DOTALL):
                                if '{{' in item:
                                    pass
                                else:
                                    item=re.sub(r"DEL[\s]+FROM[\s]+|DEL[\s]+","DELETE FROM {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.",item,flags=re.I)
                        item=re.sub(r"UPDATE|^UPD ","UPDATE ",item,flags=re.I)
                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                        # item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I) CREATE OR REPLACE TABLE {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.
                        item=re.sub(r"CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE|CREATE[\s]+VOLATILE[\s]+TABLE[\s]+","CREATE OR REPLACE TABLE {{PARAMS.PROJECTID}}.{{PARAMS.TEMPDATASET}}.",item,flags=re.I) 

                        item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                        item=re.sub(r"[\s]+|\;|\(|\`","",item)
                        item=re.sub(r"\.[\s]+",".",item)
                        # print(item)
                        with_Append_project_id_Final_input_sql_statements.append(item.upper())


        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                counter=0
                comment=0
                obj=''
                for fileLine in fileLines:
                    procedure_cmt=re.findall(r"CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+`.*?`\.[\w]+\(.*?\)\n",fileLine)
                    cmt=re.findall(r"^[\s]*--|^BEGIN[\s]*|^END[\s]*",fileLine)
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
                    elif procedure_cmt :
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
                # print(file_obj)
                parameter="\`\{\{[\w]+\.[\w]+\}\}\.{\{[\w]+\.[\w]+\}\}\`"
                for element in file_obj:
                    # print("element:", element)
                    output_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+"+parameter+"\.[\w]+"
                                                    r"|CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+"+parameter+"\.[\w]+"
                                                    r"|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+"+parameter+"\.[\w]+"
                                                    r"|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]+"+parameter+"\.[\w]+"
                                                    r"|CREATE[\s]+VIEW[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+"+parameter+"\.[\w]+"
                                                    r"|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]+"+parameter+"\.[\w]+"
                                                    r"|INSERT[\s]+INTO[\s]+"+parameter+"\.[\w]+"
                                                    r"|MERGE[\s]+INTO[\s]+"+parameter+"\.[\w]+"
                                                    r"|^UPDATE[\s]+"+parameter+"\.[\w]+"
                                                    r"|TRUNCATE[\s]+TABLE[\s]+"+parameter+"\.[\w]+"
                                                    r"|DELETE[\s]*FROM[\s]+"+parameter+"\.[\w]+"
                                                    r"|DROP[\s]+TABLE[\s]+"+parameter+"\.[\w]+"
                                                    r"|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]+"+parameter+"\.[\w]+"
                                                    r"|^[\s]*SELECT[\s]+", element, flags=re.I)
                    # print("output_sql_statements", output_sql_statements)
                    for item in output_sql_statements:
                        # print("item", item)
                        item = re.sub(r"TRUNCATE[\s]+TABLE[\s]+", "DELETE FROM ", item)
                        item = re.sub(r"MERGE[\s]+INTO[\s]+", "INSERT INTO ", item)
                        find_drop = re.findall(r"DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]+|DROP[\s]+TABLE[\s]+", item)
                        if find_drop:
                            if 'IF EXISTS' in find_drop[0]:
                                pass
                            else:
                                # print(item)
                                item = re.sub(r"DROP[\s]+TABLE[\s]+", "DROP TABLE IF EXISTS ", item)

                        remove_space=re.sub(r"[\s]+","",item)
                        if remove_space.upper()=="SELECT":
                            all_output_file_statements.append(element)
                        else:
                            all_output_file_statements.append(item.upper())
                        item=re.sub(r"\`","",item)
                        item=re.sub(r"[\s]+","",item)
                        with_Append_project_id_Final_output_sql_statements.append(item.upper())

        # print("all_input_file_statements", len(all_input_file_statements), all_input_file_statements)
        # print("all_output_file_statements", len(all_output_file_statements),all_output_file_statements)

        # print("input_list =", len(with_Append_project_id_Final_input_sql_statements), with_Append_project_id_Final_input_sql_statements)
        # print("output_list=", len(with_Append_project_id_Final_output_sql_statements),with_Append_project_id_Final_output_sql_statements)

        if len(with_Append_project_id_Final_input_sql_statements)==0 and len(with_Append_project_id_Final_output_sql_statements)==0 :
            result_no_of_statement_cswg.append("NA")
            log_result.append(Result_NA())
        elif len(with_Append_project_id_Final_input_sql_statements)>0:
            if with_Append_project_id_Final_input_sql_statements==with_Append_project_id_Final_output_sql_statements:
                # print("YYYY")
                result_no_of_statement_cswg.append("Y")
                log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
            else:
                result_no_of_statement_cswg.append("N")
                log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
        else:
            result_no_of_statement_cswg.append("N")
            log_result.append(Result_No(all_input_file_statements,all_output_file_statements))

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_no_of_statement_cswg.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Table_And_View_In_Upper_Case(log_result,filename,result_table_and_view_in_upper):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table and View In Upper Case"))
    try:
        if filename in outputFile:
            comment=0
            file_obj=''
            final_result="Y"
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine


            table_name=re.findall(r'UPDATE[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*|INTO[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*|'
                                  r'FROM[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*|'
                                  r'JOIN[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*|TABLE[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*|'
                                  r'EXISTS[\s]+\`*\{\{params\.ProjectID\}\}\`*\.\`*\{\{params\.[\w]+\}\}\`*\.\`*[\w]+\`*',file_obj,flags=re.I|re.DOTALL)
            # print(table_name)
            upper_lst=[]
            for ele in table_name:
                find_tbl=re.findall(r'\.\`*([\w]+)\`*$',ele,flags=re.I|re.DOTALL)[0]
                # print(find_tbl)
                if find_tbl.isupper():
                    pass
                else:
                    final_result="N"
                    upper_lst.append(ele)
                    # break
            if final_result == "N":
                result_table_and_view_in_upper.append("N")
                log_result.append(Output_Result_No(upper_lst))
            else:
                result_table_and_view_in_upper.append("Y")
                log_result.append(Output_Result_Yes(table_name))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_table_and_view_in_upper.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)
        
def BQ_Validation_For_CSWG(log_result,filename,result_bq_validation_cswg,BQ_JSON_PATH_CSWG,Parameter_JSON_File_Path_CSWG,all_other_table):
    log_datetime=DateTime()
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Validate on BQ"))
    errorFileCounter = 0
    clientBQ = bigquery.Client.from_service_account_json(json_credentials_path=BQ_JSON_PATH_CSWG)
    try:
        fileReopen = open (OutputFolderPath+filename, "r")
        completeFileContent = fileReopen.read()
        config_file = open(Parameter_JSON_File_Path_CSWG, "r")  # Json file path
        json_dict = json.load(config_file) # Load Json file
        for key,val in json_dict.items():
            # print(key,val)
            completeFileContent=completeFileContent.replace(key,val)
        # print("FFFFF",filename)
        # print(completeFileContent)
        find_other_table=re.findall(r'(\{\{params\.BaseDataset2\}\}\`*\.([\w]+))',completeFileContent,flags=re.I|re.DOTALL)
        for ele in set(find_other_table):
            table_found=False
            for table in all_other_table:
                if table_found == False and ele[1] in all_other_table[table]:
                    new_ele=ele[0].replace('{{params.BaseDataset2}}',table)
                    # print(ele[0],new_ele)
                    completeFileContent=completeFileContent.replace(ele[0],new_ele)
                    table_found=True
            if table_found == False:
                log_result.append(str(log_datetime)+" Schema Not Found for :"+ele[0]+"\n")
        # print(completeFileContent)
        job = clientBQ.query(completeFileContent)
        if job.result():
            result_bq_validation_cswg.append("Y")
            log_result.append(str(log_datetime)+" "+"Final_Result=Y")
    except Exception as e:
        # print("EEEE",e)
        errorFileCounter += 1
        result_bq_validation_cswg.append("N")
        log_result.append("Description: {}\n".format(sys.exc_info()[1]))
        # log_result.append("Unexpected error: {},Description: {},Line No: {}\n\n\n".format(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2].tb_lineno))
        log_result.append(str(log_datetime)+" "+"Final_Result=N")

def check_cast_to_safe_cast(log_result, filename, result_check_cast_to_safe_cast):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Cast To Safe Cast"))
    input_cast=[]
    output_cast=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
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
                        find_cast_ip=re.findall(r"CAST[\s]*\(", fileline, flags=re.I|re.DOTALL)
                        # print("1",find_cast_ip)
                        if find_cast_ip:
                            for ele in find_cast_ip:
                                # print(ele)
                                Log_input.append(ele)
                                input_cast.append(ele.lower())


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                comment=0
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
                        find_cast_op=re.findall(r"SAFE_CAST[\s]*\(", fileline, flags=re.I|re.DOTALL)
                        # print("2",find_cast_op)
                        for ele in find_cast_op:
                            Log_output.append(ele)
                            output_cast.append(ele.lower())


        # print("input_all_datatype =", len(input_cast), input_cast)
        # print("output_all_datatype=", len(output_cast), output_cast)

        if len(input_cast)==0 and len(output_cast)==0:
            log_result.append(Result_NA())
            result_check_cast_to_safe_cast.append("NA")
        elif len(input_cast)==len(output_cast):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_cast_to_safe_cast.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_cast_to_safe_cast.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_cast_to_safe_cast.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_procedure_added_In_Output_File(log_result, filename, result_check_procedure_added_In_Output_File):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check procedure in output"))
    input_procedure=[]
    output_procedure=[]
    Log_input=[]
    Log_output=[]
    try:
    # if 1== 1:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r", encoding=e) as f:
                fileLines = f.readlines()
                comment=0
                for fileline in fileLines:
                    cmt=re.findall(r'^[\s]*--', fileline)
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
                        find_procedure_op=re.findall(r"CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+`.*?`\.[\w]+\(.*?\)", fileline, flags=re.I|re.DOTALL)
                     
                        for ele in find_procedure_op:
                            Log_output.append(ele)
                            output_procedure.append(ele.lower())

        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                filename=os.path.basename(filename)
                filename=re.sub(r"translated_|\.[\w]+$","",filename,flags=re.I)
                final_parm_list=[]
                comment=0
                counter=0
                obj=''
                for fileLine in fileLines:
                    if "/*" in fileLine:
                        if '*/' in fileLine:
                            comment=0
                        else:
                            comment=1
                    elif comment==1:
                        if "*/" in fileLine:
                            comment=0
                        else:
                            pass

                    elif fileLine.startswith("--"):
                        pass
                    elif counter==0:
                        obj=''
                        fileLine=fileLine.lstrip()
                        if fileLine.upper().startswith(("RENAME TABLE\t","RENAME TABLE\n","RENAME TABLE ","DROP\t","DROP\n","DROP ","MERGE\t","MERGE\n","MERGE ","SELECT\t","SELECT ","SELECT\n","ALTER\t","ALTER\n","ALTER ","INSERT\t","INSERT\n","INSERT ","DELETE \n","DELETE\n","DELETE FROM\t","DELETE FROM\n","DELETE FROM ","DELETE  FROM\t","DELETE  FROM\n","DELETE  FROM ", "INS\t", "INS ","INS\n", "CREATE\t", "CREATE\n","CREATE ","UPDATE\t","UPDATE\n","UPDATE ","UPD\t","UPD\n","UPD ","SEL\t","SEL\n","SEL ","DEL FROM\t","DEL FROM\n","DEL FROM","DEL ")):
                            # print(fileLine)
                            counter=1
                            obj=obj+fileLine
                            ignore_pattern_end=re.findall(r"\;",fileLine,flags=re.I)
                            # print(ignore_pattern_end)
                            if ignore_pattern_end:
                                counter=0
                                find_param1=re.findall(r"\$[\s]*\{([\w]+)\}",obj,flags=re.I)
                                find_colan=re.findall(r"(\:[\w]+)",obj,flags=re.I)
                                find_param=find_param1+find_colan
                                # print(find_param)
                                for ele in find_param:
                                    if ele in final_parm_list:
                                        pass
                                    else:
                                        final_parm_list.append(ele)

                    elif counter==1 :
                        ignore_pattern_end=re.findall(r"\;",fileLine,flags=re.I)
                        if ignore_pattern_end:
                            counter=0
                            obj=obj+fileLine
                            # print(obj)
                            find_param1=re.findall(r"\$[\s]*\{([\w]+)\}",obj,flags=re.I)
                            find_colan=re.findall(r"(\:[\w]+)",obj,flags=re.I)
                            find_param=find_param1+find_colan
                            # print(find_param)
                            for ele in find_param:
                                if ele in final_parm_list:
                                    pass
                                else:
                                    final_parm_list.append(ele)
                        else:
                            obj=obj+fileLine

                param_list=''
                stmt="CREATE OR REPLACE PROCEDURE `{{params.ProjectID}}.{{params.BaseDataset1}}`."+filename
                # print("stmt", stmt)
                if len(final_parm_list)==0:
                    stmt=stmt+"()"
                else:
                    for ele in final_parm_list:
                        if 'timestamp' in ele.lower() or '_dt' in ele.lower() or 'date' in ele.lower():
                            param_list=param_list+ele+" DATE"+","
                        elif 'nm' in ele.lower() or 'num' in ele.lower():
                            param_list=param_list+ele+" INT64"+","
                        elif 'cd' in ele.lower():
                            param_list=param_list+ele+" STRING"+","
                        elif ':' in ele.lower():
                            param_list=param_list+ele+" STRING"+","

                    param_list=re.sub(r"\:|\,$","",param_list)
                    stmt=stmt+"("+param_list+")"

            Log_input.append(stmt)
            input_procedure.append(stmt.lower())

        # print("input_all_datatype =", len(input_procedure), input_procedure)
        # print("output_all_datatype=", len(output_procedure), output_procedure)

        if len(output_procedure)==0:
            log_result.append(Result_NA())
            result_check_procedure_added_In_Output_File.append("NA")
        elif input_procedure == output_procedure:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_procedure_added_In_Output_File.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_procedure_added_In_Output_File.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_procedure_added_In_Output_File.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_current_timestamp_to_current_datetime(log_result, filename, result_check_current_timestamp_to_current_datetime):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Cast To Safe Cast"))
    input_current_timestamp=[]
    output_current_datetime=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                comment=0
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
                        find_current_timestamp_ip=re.findall(r"CURRENT_TIMESTAMP", fileline, flags=re.I|re.DOTALL)
                        for ele in find_current_timestamp_ip:
                            Log_input.append(ele)
                            input_current_timestamp.append(ele.lower())


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                comment=0
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
                        find_current_datetime_op=re.findall(r"CURRENT_DATETIME", fileline, flags=re.I|re.DOTALL)
                       
                        for ele in find_current_datetime_op:
                            Log_output.append(ele)
                            output_current_datetime.append(ele.lower())


        # print("input_current_timestamp =", len(input_current_timestamp), input_current_timestamp)
        # print("output_current_datetime=", len(output_current_datetime), output_current_datetime)

        if len(input_current_timestamp)==0 and len(output_current_datetime)==0:
            log_result.append(Result_NA())
            result_check_current_timestamp_to_current_datetime.append("NA")
        elif len(input_current_timestamp)==len(output_current_datetime):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_current_timestamp_to_current_datetime.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_current_timestamp_to_current_datetime.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_current_timestamp_to_current_datetime.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_auther_comment_wells_fargo(log_result,filename,result_auther_comment_wells_fargo):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Auther Comment WF"))
    input_log=[]
    output_log=[]
    input_auther_comment=[]
    output_auther_comment=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_cmt_in=re.findall(r'\-\-.*?\n',fileLine)
                    if find_cmt_in:
                        input_log.append(find_cmt_in[0])
                        find_cmt_in=re.sub(r"[\s]*|\n", "", find_cmt_in[0])
                        input_auther_comment.append(find_cmt_in)

        # print("input_auther_comment", input_auther_comment)
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_cmt_out=re.findall(r'\-\-.*?\n',fileLine)
                    if find_cmt_out:
                        output_log.append(find_cmt_out[0])
                        find_cmt_out=re.sub(r"[\s]*|\n", "", find_cmt_out[0])
                        output_auther_comment.append(find_cmt_out)
                    
            # print("input_auther_comment", input_auther_comment)
            # print("input_auther_comment", input_auther_comment)

            if len(input_auther_comment)==0 and len(output_auther_comment) == 0:
                result_auther_comment_wells_fargo.append("NA")
                log_result.append(Result_NA())
            elif len(input_auther_comment)>0 and len(output_auther_comment)>0:
                if input_auther_comment==output_auther_comment:
                    result_auther_comment_wells_fargo.append("Y")
                    log_result.append(Result_Yes(input_log,output_log))
                else:
                    result_auther_comment_wells_fargo.append("N")
                    log_result.append(Result_No(input_log,output_log))
            else:
                result_auther_comment_wells_fargo.append("N")
                log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_auther_comment_wells_fargo.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Lower_For_Special_Characters(log_result,filename,result_lower_for_special_character):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Lower In Special Characters"))
    try:
        if filename in outputFile:
            comment=0
            file_obj=''
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine

            find_lower=re.findall(r"LOWER[\s]*\([\s]*'[\d]+'[\s]*\)|LOWER[\s]*\([\s]*'[\s]*'[\s]*\)|LOWER[\s]*\([\s]*'[\W]+'[\s]*\)",file_obj,flags=re.I|re.DOTALL)
            # print(find_lower)
            if find_lower:
                result_lower_for_special_character.append("N")
                log_result.append(Output_Result_No(find_lower))
            else:
                result_lower_for_special_character.append("NA")
                log_result.append(Result_NA())


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_lower_for_special_character.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_DATEDIFF_Function_In_Output_File(log_result,filename,result_datediff_function):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check DATEDIFF()"))
    try:
        if filename in outputFile:
            comment=0
            file_obj=''
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine

            find_datediff=re.findall(r"DATEDIFF[\s]*\(",file_obj,flags=re.I|re.DOTALL)
            # print(find_datediff)
            if find_datediff:
                result_datediff_function.append("N")
                log_result.append(Output_Result_No(find_datediff))
            else:
                result_datediff_function.append("NA")
                log_result.append(Result_NA())


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_datediff_function.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Group_By_Columns_In_Insert_Statement(log_result,filename,result_group_by_columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check GROUP BY Columns In INSERT Statement"))
    try:
        if filename in inputFile:
            comment=0
            file_obj=''
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine

            # print(file_obj)
            find_insert_stat=re.findall(r'INSERT[\s]+INTO[\s]+.*?;',file_obj,flags=re.I|re.DOTALL)
            input_group_by=[]
            input_show_data=[]
            for insert_stat in find_insert_stat:
                input_show_data.append(re.findall(r'INSERT[\s]+INTO[\s]+.*?\(|INSERT[\s]+INTO[\s]+.*?SELECT',insert_stat,flags=re.I|re.DOTALL)[0])
                # print(input_show_data)
                find_group_by=re.findall(r'GROUP[\s]+BY[\s]+.*?\)|GROUP[\s]+BY[\s]+.*?\;',insert_stat,flags=re.I|re.DOTALL)
                # print(find_group_by)
                colunm_count=0
                if find_group_by:
                    for ele in find_group_by:
                        input_show_data.append(ele)
                        # print(ele)
                        colunm_count+=len(ele.split(','))
                        # print(colunm_count)
                    input_show_data.append("In Input "+str(colunm_count)+" Column Present In GROUP BY")
                else:
                    input_show_data.append("GROUB BY Not Present For Above Statement")
                input_group_by.append(colunm_count+len(find_group_by))
                # print(insert_stat)

            # find_datediff=re.findall(r"DATEDIFF[\s]*\(",file_obj,flags=re.I|re.DOTALL)
        if filename in outputFile:
            comment=0
            file_obj=''
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
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
                    file_obj=file_obj+fileLine[0]+'\n'

                else:
                    file_obj=file_obj+fileLine

            find_insert_stat=re.findall(r'INSERT[\s]+INTO[\s]+.*?;',file_obj,flags=re.I|re.DOTALL)
            output_group_by=[]
            output_show_data=[]
            for insert_stat in find_insert_stat:
                output_show_data.append(re.findall(r'INSERT[\s]+INTO[\s]+.*?\(|INSERT[\s]+INTO[\s]+.*?SELECT',insert_stat,flags=re.I|re.DOTALL)[0])
                # print(output_show_data)
                find_group_by=re.findall(r'GROUP[\s]+BY[\s]+.*?\)|GROUP[\s]+BY[\s]+.*?\;',insert_stat,flags=re.I|re.DOTALL)
                # print(find_group_by)
                colunm_count=0
                if find_group_by:
                    for ele in find_group_by:
                        output_show_data.append(ele)
                        # print(ele)
                        colunm_count+=len(ele.split(','))
                        # print(colunm_count)
                    output_show_data.append("In Output "+str(colunm_count)+" Column Present In GROUP BY")
                else:
                    output_show_data.append("GROUB BY Not Present For Above Statement")
                output_group_by.append(colunm_count+len(find_group_by))

            if len(input_group_by) == 0 and len(output_group_by) == 0:
                result_group_by_columns.append("NA")
                log_result.append(Result_NA())
            elif input_group_by == output_group_by:
                result_group_by_columns.append("Y")
                log_result.append(Result_Yes(input_show_data,output_show_data))
            else:
                result_group_by_columns.append("N")
                log_result.append(Result_No(input_show_data,output_show_data))


    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_group_by_columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)



#For KSH Files
def check_trap_comment(log_result,filename,result_of_trap_comment):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Trap is commented or not"))
    try:
        comment_added=[]
        comment_not_added=[]
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            for fileLine in fileLines:
                find_uncommented_trap=re.findall(r'^[\s]*TRAP',fileLine,flags=re.I)
                if find_uncommented_trap:
                    comment_not_added.append(fileLine.strip())
                find_commented_trap=re.findall(r'^[\s]*\#[\s]*TRAP',fileLine,flags=re.I)
                if find_commented_trap:
                    comment_added.append(fileLine.strip())


        # print(comment_not_added)
        # print(comment_added)


        if len(comment_added)==0 and len(comment_not_added)==0:
            log_result.append(Result_NA())
            result_of_trap_comment.append("NA")
        elif comment_not_added:
            log_result.append(Output_Result_No(comment_not_added))
            result_of_trap_comment.append("N")
        elif comment_added:
            log_result.append(Output_Result_Yes(comment_added))
            result_of_trap_comment.append("Y")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_of_trap_comment.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_bteq_comment(log_result,filename,result_of_bteq_comment):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("bteq <<  is commented or not"))
    try:
        # if 1==1:
        comment_added=[]
        comment_not_added=[]
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            bteq_word=[]
            for fileLine in fileLines:
                find_uncommented_bteq=re.findall(r'^[\s]*\-*\-*[\s]*bteq[\s]*\<\<([\w]+)',fileLine,flags=re.I)
                if find_uncommented_bteq:
                    bteq_word.append(find_uncommented_bteq[0])
                    comment_not_added.append(fileLine.strip())
                find_commented_bteq=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*bteq[\s]*\<\<([\w]+)',fileLine,flags=re.I)
                if find_commented_bteq:
                    bteq_word.append(find_commented_bteq[0])
                    comment_added.append(fileLine.strip())

                find_uncommented_end=re.findall(r'^[\s]*\-*\-*[\s]*([\w]+)[\s]*$',fileLine)
                if find_uncommented_end:
                    if find_uncommented_end[0] in bteq_word:
                        comment_not_added.append(fileLine.strip())
                find_commented_end=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*([\w]+)[\s]*$',fileLine)
                if find_commented_end:
                    if find_commented_end[0] in bteq_word:
                        comment_added.append(fileLine.strip())

        # print(bteq_word)
        # print(comment_not_added)
        # print(comment_added)


        if len(comment_added)==0 and len(comment_not_added)==0:
            log_result.append(Result_NA())
            result_of_bteq_comment.append("NA")
        elif comment_not_added:
            log_result.append(Output_Result_No(comment_not_added))
            result_of_bteq_comment.append("N")
        elif comment_added:
            log_result.append(Output_Result_Yes(comment_added))
            result_of_bteq_comment.append("Y")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_of_bteq_comment.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_error_check_added_or_not_in_file(log_result,filename,result_for_error_check_added_or_not_in_file):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("echo added or not in file"))
    try:
        # if 1==1:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.read()
            ignore_count=0
            bteq_echo_counter=0
            readobj=''
            cat_obj=''
            bteq_echo=''
            fileLines=re.sub('echo[\s]*"[\s]*\n','echo "',fileLines,flags=re.I|re.DOTALL)
            fileLines=fileLines.split("\n")
            for lines in fileLines:
                if ignore_count==0:
                    ignore_pattern=re.findall(r"cat[\s]*\>.*[\s]*\<\<[\s]*([\w]+)",lines,flags=re.I)
                    if ignore_pattern:
                        ignore_count=1
                        cat_obj=cat_obj+lines
                    # elif bteq_echo_counter==0:
                    #     readobj=readobj+lines
                elif ignore_count==1 :
                    ignore_pattern_end=re.findall(r""+ignore_pattern[0]+"[\s]*$",lines,flags=re.I)
                    if ignore_pattern_end:
                        ignore_count=0
                        cat_obj=cat_obj+lines
                    elif bteq_echo_counter==0:
                        cat_obj=cat_obj+lines

                if bteq_echo_counter==0:
                    find_echo_bteq=re.findall(r'echo[\s]*\"[\s]*\#*[\s]*\-*\-*[\s]*bteq[\s]*\<\<[\s]*([\w]+)',lines,flags=re.I)
                    if find_echo_bteq:
                        bteq_echo_counter=1
                        bteq_echo=bteq_echo+lines
                    # elif ignore_count==0:
                    #     readobj=readobj+lines
                elif bteq_echo_counter==1 :
                    find_echo_end=re.findall(r""+find_echo_bteq[0]+"[\s]*$",lines,flags=re.I)
                    if find_echo_end:
                        bteq_echo_counter=0
                        bteq_echo=bteq_echo+lines
                    elif ignore_count==0:
                        bteq_echo=bteq_echo+lines
                if ignore_count==0 and bteq_echo_counter==0:
                    # print(lines)
                    readobj=readobj+lines+"\n"
            # print("DD:",readobj)
            start='TEMP_SQL=\$\{TEMP_DIR\}\/\$\{ETL_INTF_CD\}_\$\{PROGBASE\}\.sql[\s]+echo[\s]*"[\s]*BEGIN'
            end='"[\s]*\>[\s]*\$\{TEMP_SQL\}[\s]+f_execute_bq[\s]*"\$TEMP_SQL"[\s]*"F"[\s]+RC\=\$\?[\s]+echo \$BQ_OUTPUT[\s]+if \[\[ \$RC \-ne 0 \]\][\s]+then[\s]+echo "ERROR\!\!\! Error in TEMP_SQL "[\s]+echo "Exiting with exit status as 1 "[\s]+exit 1[\s]+fi[\s]+echo \"SUCCESSFULLY EXECUTED BIGQUERY COMMAND\"'
            find_temporary=re.findall(r'TEMPORARY[\s]+TABLE[\s]+',readobj,flags=re.I|re.DOTALL)
            find_begin_trancation=re.findall(r'\n[\s]*BEGIN[\s]+TRANSACTION',readobj,flags=re.I|re.DOTALL)
            find_bteq=re.findall(r'\-\-[\s]*bteq[\s]*<<[\s]*[\w]+',readobj,flags=re.I)
            find_export=re.findall(r'\.export[\s]+[\w]*[\s]*FILE[\s]*\=',readobj,flags=re.I)
            find_end_in_cat=re.findall(r''+end+'',cat_obj,flags=re.I|re.DOTALL)
            find_end_in_bteq_echo=re.findall(r''+end+'',bteq_echo,flags=re.I|re.DOTALL)
            readobj=re.sub(r'"[\s]*\>','">',readobj,flags=re.DOTALL)
            readobj=re.sub(r'"[\s]*\>\>','">>',readobj,flags=re.DOTALL)
            readobj=re.sub(r'\n[\s]*','\n',readobj,flags=re.DOTALL)
            if find_end_in_cat:
                log_result.append(Output_Result_No(["echo and error check added in cat block"]))
                result_for_error_check_added_or_not_in_file.append("N")
            elif find_end_in_bteq_echo:
                log_result.append(Output_Result_No(["echo and error check added in bteq block which is already in echo"]))
                result_for_error_check_added_or_not_in_file.append("N")
            elif find_temporary and find_export:
                readobj=re.sub(r'\n[\s]*','\n',readobj,flags=re.DOTALL)
                # print(readobj)
                fileLine=readobj.split('\n')
                counter=0
                correct_lst=[]
                wrong_lst=[]
                echo_stmt=''
                bteq_counter=0
                # print("TEMp")
                for lines in fileLine:
                    # print(lines,bteq_counter)
                    if bteq_counter==0:
                        find_bteq=re.findall(r'\-\-[\s]*bteq[\s]*\<\<[\s]*([\w]+)',lines,flags=re.I)
                        if find_bteq:
                            # print(find_bteq)
                            bteq_counter=1
                    elif bteq_counter==1 :
                        find_bteqof=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*'+find_bteq[0]+'',lines,flags=re.I)
                        if find_bteqof:
                            bteq_counter=0
                        else:
                            # print(lines)
                            lines=lines+' '
                            if counter==0:
                                find_echo_stat=re.findall(r'echo[\s]+\"[\s]*BEGIN[\s]+|echo[\s]+\"[\s]*DECLARE[\s]+|echo[\s]+\"[\s]*SET[\s]+activityCount_var|echo[\s]+\"[\s]*CALL[\s]+|echo[\s]+\"[\s]*RENAME[\s]+TABLE[\s]+|echo[\s]+\"[\s]*DROP[\s]+|echo[\s]+\"[\s]*MERGE[\s]+|echo[\s]+\"[\s]*SELECT[\s]+|echo[\s]+\"[\s]*ALTER[\s]+|echo[\s]+\"[\s]*INSERT[\s]+|echo[\s]+\"[\s]*DELETE[\s]+|echo[\s]+\"[\s]*DELETE[\s]+FROM|echo[\s]+\"[\s]*INS[\s]+|echo[\s]+\"[\s]*CREATE[\s]+|echo[\s]+\"[\s]*UPDATE[\s]+|echo[\s]+\"[\s]*UPD[\s]+|echo[\s]+\"[\s]*SEL[\s]+|echo[\s]+\"[\s]*DEL[\s]+FROM|echo[\s]+\"[\s]*TRUNCATE[\s]+',lines,flags=re.I|re.DOTALL)
                                find_stat=re.findall(r'^SET[\s]+activityCount_var[\s]+\=[\s]+\(SELECT[\s]+COUNT\(\*\)[\s]+AS[\s]+\`\_\_count\_\_\`|^CALL[\s]+|^RENAME[\s]+TABLE[\s]+|^DROP[\s]+|^MERGE[\s]+|^SELECT[\s]+|^ALTER[\s]+|^INSERT[\s]+|^DELETE[\s]+|^DELETE[\s]+FROM|^INS[\s]+|^CREATE[\s]+|^UPDATE[\s]+|^UPD[\s]+|^SEL[\s]+|^DEL[\s]+FROM|^TRUNCATE[\s]+',lines,flags=re.I|re.DOTALL)
                                # print(find_echo_stat)
                                if find_echo_stat:
                                    echo_stmt=lines
                                    obj=lines
                                    counter=1
                                    find_begin=re.findall(r'^[\s]*BEGIN[\s]*$',lines,flags=re.I|re.DOTALL)
                                    if find_echo_stat[0].strip().upper().endswith('DECLARE'):
                                        pass
                                    elif "SET activityCount_var" in lines:
                                        counter=2
                                    elif find_begin or find_echo_stat[0].strip().upper().endswith('BEGIN'):
                                        counter=3
                                    else:
                                        find_end=re.findall(r'\;[\s]*$',lines)
                                        if find_end:
                                            counter=4


                                            # print("EEEEEEEEEEEEEEEEEEE")
                                    # print(find_echo_stat)
                                elif find_stat:
                                    # print("Wrong",find_stat)
                                    wrong_lst.append(lines)
                            elif counter==1:
                                find_end=re.findall(r'\;[\s]*$',lines)
                                if lines.upper().startswith('DECLARE'):
                                    pass
                                elif "SET activityCount_var" in lines:
                                    counter=2
                                elif find_end:
                                    counter=4
                                    # print(lines)
                            elif counter==2:
                                find_end=re.findall(r'END[\s]+IF[\s]*\;[\s]*$',lines,flags=re.I)
                                if find_end:
                                    # print(lines,'FFFF')
                                    counter=4
                            elif counter==3:
                                find_end=re.findall(r'END[\s]*\;[\s]*$',lines,flags=re.I)
                                if find_end:
                                    # print(lines,'FFFF')
                                    counter=4
                            elif counter==4:
                                if '">> ${TEMP_SQL}' in lines or '"> ${TEMP_SQL}' in lines:
                                    # print('CCCCCCCCCCCCCC')
                                    counter=0
                                    correct_lst.append(echo_stmt)
                                else:
                                    # print('WWWWWWW',lines,bteq_counter)
                                    counter=0
                                    wrong_lst.append(echo_stmt)

                if len(correct_lst)==0 and len(wrong_lst)==0:
                    log_result.append(Result_NA())
                    result_for_error_check_added_or_not_in_file.append("NA")
                elif wrong_lst:
                    log_result.append(Output_Result_No(wrong_lst))
                    result_for_error_check_added_or_not_in_file.append("N")
                elif correct_lst:
                    log_result.append(Output_Result_Yes(correct_lst))
                    result_for_error_check_added_or_not_in_file.append("Y")
                # find_stat=re.
                # print("YYYYYYYYYYYYYYY")
                # log_result.append(Output_Result_No(["elif find_temporary and find_export:"]))
                # result_for_error_check_added_or_not_in_file.append("N")


            elif find_temporary and find_begin_trancation and len(find_bteq)==0:
                # print("Y")
                find_echo=re.findall(r''+start+'',readobj,flags=re.I|re.DOTALL)
                find_error_check=re.findall(r'END[\s]*\;[\s]+'+end+'',readobj,flags=re.I|re.DOTALL)
                # print(find_echo)
                # print(find_error_check)
                if find_echo and find_error_check:
                    log_result.append(Output_Result_Yes([find_echo[0],find_error_check[0]]))
                    result_for_error_check_added_or_not_in_file.append("Y")
                else:
                    result_for_error_check_added_or_not_in_file.append("N")
                    if find_echo:
                        log_result.append(Output_Result_No(["Error Check Not Added"]))
                    elif find_error_check:
                        log_result.append(Output_Result_No(["ECHO Not Added"]))


            elif find_begin_trancation and len(find_bteq)==0:
                find_begin=re.findall(r'BEGIN[\s]*\n',readobj,flags=re.I|re.DOTALL)
                find_echo=re.findall(r''+start+'|echo[\s]*"[\s]*BEGIN',readobj,flags=re.I|re.DOTALL)
                find_error_check=re.findall(r'END[\s]*\;[\s]+'+end+'',readobj,flags=re.I|re.DOTALL)
                if len(find_begin) == len(find_echo) and len(find_begin) == len(find_error_check):
                    log_result.append(Output_Result_Yes([find_echo[0],find_error_check[0]]))
                    result_for_error_check_added_or_not_in_file.append("Y")
                else:
                    log_result.append(Output_Result_No(["Echo or Error Check Not Added"]))
                    result_for_error_check_added_or_not_in_file.append("N")
            elif find_bteq and find_export:
                input_lst=[]
                wrong_lst=[]
                final_result='NA'
                fileLine=readobj.split('\n')
                obj=''
                object_counter=0
                for lines in fileLine:
                    cmt=re.findall(r'^[\s]*--%[\s]+\#',lines)
                    if cmt:
                        pass
                    elif object_counter==0:
                        find_bteq=re.findall(r'\-\-[\s]*bteq[\s]*<<[\s]*([\w]+)',lines,flags=re.I)
                        if find_bteq:
                            obj=lines+'\n'
                            object_counter=1
                    elif object_counter==1 :
                        find_bteqof=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*'+find_bteq[0]+'',lines,flags=re.I)

                        if find_bteqof:
                            object_counter=0
                            obj=obj+lines+'\n'
                            # print(obj)
                            split_obj=obj.split('\n')
                            export_count=0
                            export_end=0
                            sql_end=0
                            export_obj=''
                            for obj_line in split_obj:
                                obj_line=obj_line+' '
                                if export_count==0:
                                    # print("LL",obj_line)
                                    find_export_in_obj=re.findall(r'\.export[\s]+[\w]*[\s]*FILE[\s]*\=',obj_line,flags=re.I)
                                    if find_export_in_obj and sql_end==0:
                                        export_count=1
                                        export_obj=obj_line+'\n'
                                    else:
                                        if sql_end==0:
                                            find_echo_stat=re.findall(r'echo[\s]+\"[\s]*BEGIN[\s]+|echo[\s]+\"[\s]*DECLARE[\s]+|echo[\s]+\"[\s]*SET[\s]+activityCount_var|echo[\s]+\"[\s]*CALL[\s]+|echo[\s]+\"[\s]*RENAME[\s]+TABLE[\s]+|echo[\s]+\"[\s]*DROP[\s]+|echo[\s]+\"[\s]*MERGE[\s]+|echo[\s]+\"[\s]*SELECT[\s]+|echo[\s]+\"[\s]*ALTER[\s]+|echo[\s]+\"[\s]*INSERT[\s]+|echo[\s]+\"[\s]*DELETE[\s]+|echo[\s]+\"[\s]*DELETE[\s]+FROM|echo[\s]+\"[\s]*INS[\s]+|echo[\s]+\"[\s]*CREATE[\s]+|echo[\s]+\"[\s]*UPDATE[\s]+|echo[\s]+\"[\s]*UPD[\s]+|echo[\s]+\"[\s]*SEL[\s]+|echo[\s]+\"[\s]*DEL[\s]+FROM|echo[\s]+\"[\s]*TRUNCATE[\s]+',obj_line,flags=re.I|re.DOTALL)
                                            find_stat=re.findall(r'^CALL[\s]+|^RENAME[\s]+TABLE[\s]+|^DROP[\s]+|^MERGE[\s]+|^SELECT[\s]+|^ALTER[\s]+|^INSERT[\s]+|^DELETE[\s]+|^DELETE[\s]+FROM|^INS[\s]+|^CREATE[\s]+|^UPDATE[\s]+|^UPD[\s]+|^SEL[\s]+|^DEL[\s]+FROM|^TRUNCATE[\s]+',obj_line,flags=re.I|re.DOTALL)
                                            if find_echo_stat:
                                                input_lst.append(find_echo_stat[0])
                                                sql_end=1
                                            elif find_stat:
                                                wrong_lst.append(find_stat[0])
                                                final_result='N'
                                                break
                                        elif sql_end==1:
                                            find_export_in_obj=re.findall(r'\.export[\s]+[\w]*[\s]*FILE[\s]*\=',obj_line,flags=re.I)
                                            if 'echo "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"' in obj_line:
                                                final_result='Y'
                                                sql_end=0
                                            elif find_bteqof[0] in obj_line or find_export_in_obj:
                                                final_result='N'
                                                break
                                elif export_count==1:
                                    # print("L1",obj_line)
                                    if export_end==0:
                                        find_create=re.findall(r'echo \"CREATE OR REPLACE TABLE \$\{SRC_DB\}\.',obj_line,flags=re.I)
                                        if find_create:
                                            input_lst.append(find_create[0])
                                            # print(find_create)
                                            export_end=1
                                        export_obj=export_obj+obj_line+'\n'
                                    elif export_end==1:
                                        find_end=re.findall(r'\;[\s]*$',obj_line)
                                        if find_end:
                                            export_end=2
                                            export_obj=export_obj+obj_line+'\n'
                                            find_echo=re.findall(r'echo \"CREATE OR REPLACE TABLE \$\{SRC_DB\}\.',export_obj)
                                            if len(find_echo) > 1:
                                                for ele in find_echo:
                                                    wrong_lst.append(ele)
                                                final_result='N'
                                                break
                                            # find_export_end=re.findall()
                                        else:
                                            export_obj=export_obj+obj_line+'\n'
                                    elif export_end==2:
                                        if '"> ${TEMP_SQL}' in obj_line:
                                            final_result='Y'
                                            export_end=0
                                            export_count=0
                                        else:
                                            final_result='N'
                                            break
                        else:
                            obj=obj+lines+'\n'
                # if final_result=='NA':
                #     log_result.append(Result_NA())
                #     result_for_error_check_added_or_not_in_file.append("NA")
                # elif final_result=='Y':
                #     log_result.append(Output_Result_Yes(input_lst))
                #     result_for_error_check_added_or_not_in_file.append("Y")
                # else:
                #     wrong_lst.append("Echo or Error Check Not Added or Added Multiple Times")
                #     log_result.append(Output_Result_No(wrong_lst))
                #     result_for_error_check_added_or_not_in_file.append("N")

                if len(input_lst)==0 and len(wrong_lst)==0:
                    log_result.append(Result_NA())
                    result_for_error_check_added_or_not_in_file.append("NA")
                elif wrong_lst:
                    wrong_lst.append("Echo or Error Check Not Added or Added Multiple Times")
                    log_result.append(Output_Result_No(wrong_lst))
                    result_for_error_check_added_or_not_in_file.append("N")
                elif input_lst:
                    log_result.append(Output_Result_Yes(input_lst))
                    result_for_error_check_added_or_not_in_file.append("Y")


            elif find_bteq:
                input_lst=[]
                wrong_lst=[]
                final_result=''
                fileLine=readobj.split('\n')
                obj=''
                object_counter=0
                final_result='NA'
                for lines in fileLine:
                    lines=lines+' '
                    cmt=re.findall(r'^[\s]*--%[\s]+\#',lines)
                    if cmt:
                        pass
                    elif object_counter==0:
                        find_bteq=re.findall(r'\-\-[\s]*bteq[\s]*<<[\s]*([\w]+)',lines,flags=re.I)
                        if find_bteq:
                            obj=lines+'\n'
                            object_counter=1
                    elif object_counter==1 :
                        find_bteqof=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*'+find_bteq[0]+'',lines,flags=re.I)

                        if find_bteqof:
                            object_counter=0
                            obj=obj+lines+'\n'
                            # print("OOOOOOOOOOO",obj)
                            find_echo=re.findall(r'echo[\s]+\"[\s]*DECLARE[\s]+|echo[\s]+\"[\s]*BEGIN[\s]+|echo[\s]+\"[\s]*CALL[\s]+|echo[\s]+\"[\s]*RENAME[\s]+TABLE[\s]+|echo[\s]+\"[\s]*DROP[\s]+|echo[\s]+\"[\s]*MERGE[\s]+|echo[\s]+\"[\s]*SELECT[\s]+.*|echo[\s]+\"[\s]*ALTER[\s]+|echo[\s]+\"[\s]*INSERT[\s]+|echo[\s]+\"[\s]*DELETE[\s]+|echo[\s]+\"[\s]*DELETE[\s]+FROM|echo[\s]+\"[\s]*INS[\s]+|echo[\s]+\"[\s]*CREATE[\s]+|echo[\s]+\"[\s]*UPDATE[\s]+|echo[\s]+\"[\s]*UPD[\s]+|echo[\s]+\"[\s]*SEL[\s]+|echo[\s]+\"[\s]*DEL[\s]+FROM',obj,flags=re.I)
                            find_error_check=re.findall(r''+end+'',obj,flags=re.I|re.DOTALL)
                            # print(find_echo)
                            # print(find_error_check)
                            if len(find_echo)==1 and len(find_error_check)==1:
                                final_result='Y'
                                input_lst.append(find_echo[0])
                                input_lst.append(find_error_check[0])
                            elif len(find_echo)>=1:
                                wrong_lst.append(find_echo[0])
                            else:
                                find_stat=re.findall(r'^[\s]*CALL[\s]+|^[\s]*RENAME[\s]+TABLE[\s]+|^[\s]*DROP[\s]+|^[\s]*MERGE[\s]+|^[\s]*SELECT[\s]+|^[\s]*ALTER[\s]+|^[\s]*INSERT[\s]+|^[\s]*DELETE[\s]+|^[\s]*DELETE[\s]+FROM|INS[\s]+|^[\s]*CREATE[\s]+|^[\s]*UPDATE[\s]+|^[\s]*UPD[\s]+|^[\s]*SEL[\s]+|^[\s]*DEL[\s]+FROM',obj,flags=re.I|re.DOTALL)
                                if find_stat:
                                    wrong_lst.append(find_stat[0])
                                    # final_result='N'
                                    # break
                                # else:
                                #     final_result='NA'

                        else:
                            obj=obj+lines+'\n'
                # print(input_lst)
                if len(input_lst)==0 and len(wrong_lst)==0:
                    log_result.append(Result_NA())
                    result_for_error_check_added_or_not_in_file.append("NA")
                elif wrong_lst:
                    wrong_lst.append("Echo or Error Check Not Added or Added Multiple Times")
                    log_result.append(Output_Result_No(wrong_lst))
                    result_for_error_check_added_or_not_in_file.append("N")
                elif input_lst:
                    log_result.append(Output_Result_Yes(input_lst))
                    result_for_error_check_added_or_not_in_file.append("Y")
                # if final_result=='NA':
                #     log_result.append(Result_NA())
                #     result_for_error_check_added_or_not_in_file.append("NA")
                # elif final_result=='Y':
                #     log_result.append(Output_Result_Yes(input_lst))
                #     result_for_error_check_added_or_not_in_file.append("Y")
                # else:
                #     log_result.append(Output_Result_No(["Echo or Error Check Not Added or Added Multiple Times"]))
                #     result_for_error_check_added_or_not_in_file.append("N")
            else:
                # print("YYYYYYYYYY")
                input_lst_yes=[]
                input_lst_no=[]
                final_result='NA'
                fileLine=readobj.split('\n')
                obj=''
                object_counter=0
                counter=0
                final_obj=[]
                for lines in fileLine:
                    lines=lines+' '
                    if counter==0:
                        find_echo=re.findall(r'echo[\s]+\"[\s]*DECLARE[\s]+|echo[\s]+\"[\s]*BEGIN[\s]+|echo[\s]+\"[\s]*CALL[\s]+|echo[\s]+\"[\s]*RENAME[\s]+TABLE[\s]+|echo[\s]+\"[\s]*DROP[\s]+|echo[\s]+\"[\s]*MERGE[\s]+|echo[\s]+\"[\s]*SELECT[\s]+.*|echo[\s]+\"[\s]*ALTER[\s]+|echo[\s]+\"[\s]*INSERT[\s]+|echo[\s]+\"[\s]*DELETE[\s]+|echo[\s]+\"[\s]*DELETE[\s]+FROM|echo[\s]+\"[\s]*INS[\s]+|echo[\s]+\"[\s]*CREATE[\s]+|echo[\s]+\"[\s]*UPDATE[\s]+|echo[\s]+\"[\s]*UPD[\s]+|echo[\s]+\"[\s]*SEL[\s]+|echo[\s]+\"[\s]*DEL[\s]+FROM',lines,flags=re.I)
                        if find_echo:
                            input_lst_yes.append(lines)
                            counter=1
                        else:
                            final_obj.append(lines)
                    elif counter==1:
                        find_end=re.findall(r'\;[\s]*$',lines)
                        if find_end:
                            counter=0
                for lines in final_obj:
                    find_echo=re.findall(r'^CALL[\s]+|^RENAME[\s]+TABLE[\s]+|^DROP[\s]+|^MERGE[\s]+|^SELECT[\s]+|^ALTER[\s]+|^INSERT[\s]+|^DELETE[\s]+|^DELETE[\s]+FROM|^INS[\s]+|^CREATE[\s]+|^UPDATE[\s]+|^UPD[\s]+|^SEL[\s]+|^DEL[\s]+FROM',lines,flags=re.I|re.DOTALL)
                    if find_echo:
                        input_lst_no.append(lines)
                        final_result="N"
                        pass

                if final_result=='NA':
                    log_result.append(Result_NA())
                    result_for_error_check_added_or_not_in_file.append("NA")
                elif len(input_lst_no)==0 and len(input_lst_yes)>=1:
                    log_result.append(Output_Result_Yes(input_lst_yes))
                    result_for_error_check_added_or_not_in_file.append("Y")
                else:
                    log_result.append(Output_Result_No(input_lst_no))
                    result_for_error_check_added_or_not_in_file.append("N")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_error_check_added_or_not_in_file.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_sed_added_in_function_call_or_not(log_result,filename,result_for_sed_added_in_function_call_or_not):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("sed added in function call or not"))
    try:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            input_obj = f.read()
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            f.seek(0)
            final_obj=f.read()
            obj=''
            correct_lst=[]
            wrong_lst=[]
            fun_name=''
            fun_sed=''
            fun_counter=0
            for line in fileLines:
                if fun_counter==0:
                    find_fun=re.findall(r'^[\s]*([\w]+)[\s]*\(\)[\s]*\{',line,flags=re.I|re.DOTALL)
                    if find_fun:
                        fun_name=find_fun[0]
                        fun_counter=1
                elif fun_counter==1:
                    find_end=re.findall(r'^[\s]*\}[\s]*$',line)
                    if find_end:
                        fun_counter=0
                    else:
                        find_bteq=re.findall(r"bteq[\s]*\<\<[\s]*[\w]+",line,flags=re.I)
                        if find_bteq and "sed" in line:
                            find_sed=re.findall(r'\|[\s]*sed.*',line)
                            find_fun_call=re.findall(r'\n[\s]*\#*[\s]*[\w]+[\s]*\=[\s]*\$\('+fun_name+'\)',input_obj,flags=re.I)
                            if find_fun_call:
                                split_call=find_fun_call[0].split("=")
                                find_fun=re.findall('[\s]+'+fun_name.strip()+'[\s]*'+split_call[0].strip()+'[\s]*=[\s]*\$\(echo \"\$\{BQ_OUTPUT\}\" \| sed 1d.*',final_obj)
                                if find_fun:
                                    correct_lst.append(find_fun[0])
                                else:
                                    wrong_lst.append(find_fun_call[0])


            if len(correct_lst)==0 and len(wrong_lst)==0:
                log_result.append(Result_NA())
                result_for_sed_added_in_function_call_or_not.append("NA")
            elif wrong_lst:
                log_result.append(Output_Result_No(wrong_lst))
                result_for_sed_added_in_function_call_or_not.append("N")
            elif correct_lst:
                log_result.append(Output_Result_Yes(correct_lst))
                result_for_sed_added_in_function_call_or_not.append("Y")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_sed_added_in_function_call_or_not.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_create_table_added_after_os_and_export(log_result,filename,result_for_create_table_added_after_os_and_export):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Create table and file executed added after .Export"))
    try:
        # if 1==1:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            readobj = f.read()
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            file_obj = f.read()
            # print(file_obj)
            obj=''
            input_lst=[]
            correct_lst=[]
            wrong_lst=[]
            final_result="NA"
            bteq_counter=0
            find_export=re.findall(r'\.export[\s]+[\w]*[\s]*FILE[\s]*\=.*',file_obj,flags=re.I)
            if find_export:
                fileLines=file_obj.split("\n")
                export_line=''
                for lines in fileLines:
                    lines=lines+' '
                    # print(lines)
                    if bteq_counter==0:
                        find_bteq=re.findall(r'^[\s]*\#*[\s]*\-\-[\s]*bteq[\s]*\<\<[\s]*([\w]+)',lines,flags=re.I)
                        if find_bteq:
                            bteq_counter=1

                    if bteq_counter==1:
                        find_bteq_end=re.findall(r'^[\s]*\#*[\s]*\-\-[\s]*'+find_bteq[0]+'',lines,flags=re.I)
                        if find_bteq_end:
                            bteq_counter=0
                            export_line=''
                        find_export=re.findall(r'\.export[\s]+[\w]*[\s]*FILE[\s]*\=.*',lines,flags=re.I)
                        if find_export:
                            export_line=lines
                            split_ele=export_line.split("=")
                            if "/" not in split_ele[1]:
                                find_word=re.findall(r'\$\{*([\w]+)\}*',split_ele[1])
                                find_path=re.findall(r'\n[\s]*'+find_word[0]+'[\s]*(\=.*)',file_obj,flags=re.I)
                                if find_path:
                                    export_line=split_ele[0].strip()+find_path[0].strip()
                                else:
                                    wrong_lst.append("Path not found for"+split_ele[1])
                                    # print("Path not found")
                        if export_line!='':
                            # print(export_line)
                            export_line=export_line.replace(';','')
                            export_line=export_line.replace('"','')
                            new_export=export_line.split("=")
                            t=new_export[1].split("/")[-1]
                            t1=t.replace(".","_")
                            t2=t1.replace(".","_")
                            t3=re.split("=",export_line)
                            file_path=re.split("\/",t3[1])
                            folder_path=''
                            for i in file_path[:-1]:
                                i=i.replace('\\','')
                                folder_path=folder_path+i.strip()+'/'
                            folder_path=re.sub('\/[\s]*$','',folder_path,flags=re.I|re.DOTALL)
                            t4=t.split(".")
                            if len(t4) == 2:
                                stat2='f_bq_extract_csv_without_header "${SRC_DB}" "'+t2.strip()+'" "'+folder_path.strip()+'" "'+t4[0].strip()+'" "'+t4[1].strip()+'"'
                            else:
                                stat2='f_bq_extract_csv_without_header "${SRC_DB}" "'+t2.strip()+'" "'+folder_path.strip()+'" "'+t4[0].strip()+'" ""'
                            # stat3='\nif [[ $? -ne 0 ]]\nthen\necho "ERROR!!! Error in fn_bq_extract_csv_without_header"\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n\n'
                            find_create=re.findall(r'^[\s]*e*c*h*o*[\s]*\"*CREATE[\s]*OR[\s]*REPLACE[\s]*TABLE[\s]*\$\{SRC_DB\}\.',lines)
                            # print("t1",t1)
                            find_sel=re.findall(r'^[\s]*SELECT[\s]+',lines)
                            # print(find_create)
                            # print(stat2)
                            if find_create and t1.strip() in lines:
                                # print("YYYY")
                                export_line=''
                                if stat2 in file_obj:
                                    correct_lst.append(lines)
                                    correct_lst.append(stat2)
                                else:
                                    wrong_lst.append('f_bq_extract_csv_without_header not added for '+t1+' table or not correct')
                            elif find_sel:
                                # print("echo not added ")
                                wrong_lst.append(lines)
                                export_line=''
            # print(correct_lst,wrong_lst)
            if len(correct_lst) ==0 and len(wrong_lst) == 0:
                log_result.append(Result_NA())
                result_for_create_table_added_after_os_and_export.append("NA")
            elif correct_lst and len(wrong_lst)==0:
                log_result.append(Output_Result_Yes(correct_lst))
                result_for_create_table_added_after_os_and_export.append("Y")
            elif wrong_lst:
                log_result.append(Output_Result_No(wrong_lst))
                result_for_create_table_added_after_os_and_export.append("N")
    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_create_table_added_after_os_and_export.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_email_logic(log_result,filename,result_for_email_logic):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Email Logic"))
    try:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            readobj = f.read()
            final_result='NA'
            final_lst=[]
            find_uuencode=re.findall(r'uuencode[\s]+\-m',readobj,flags=re.I|re.DOTALL)
            if find_uuencode:
                find_uuencode1=re.findall(r'uuencode[\s]*\-m.*',readobj,flags=re.I)
                for find_uuencode in find_uuencode1:
                    find_name=re.findall(r'uuencode[\s]*\-m[\s]*\$(.*)\$\(basename[\s]*\$',find_uuencode,flags=re.I|re.DOTALL)
                    find_string='#'+find_uuencode+'\nbase64 < '+find_name[0]
                    if find_string in readobj:
                        final_lst.append(find_string)
                        final_result='Y'
                    else:
                        final_lst.append(find_uuencode)
                        final_result='N'
                        break

            find_sendmail=re.findall(r'\|[\s]*sendmail[\s]*\-t',readobj,flags=re.I|re.DOTALL)
            if find_sendmail:
                for i in find_sendmail:
                    final_lst.append(i)
                final_result='N'
            else:
                find_sendmail=re.findall(r'\|[\s]*\/usr\/sbin\/sendmail[\s]*\-t',readobj,flags=re.I|re.DOTALL)
                if find_sendmail:
                    for i in find_sendmail:
                        final_lst.append(i)
                    final_result='Y'


        # print(final_result)
        if final_result=='NA':
            log_result.append(Result_NA())
            result_for_email_logic.append("NA")
        elif final_result=='Y':
            log_result.append(Output_Result_Yes(final_lst))
            result_for_email_logic.append("Y")
        else:
            log_result.append(Output_Result_No(final_lst))
            result_for_email_logic.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_email_logic.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_f_get_aedw_open_batches(log_result,filename,result_for_f_get_aedw_open_batches):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("f_get_aedw_open_batches"))
    try:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            readobj = f.read()
            final_result='NA'
            final_lst=[]
            counter=0
            if "\nf_get_aedw_open_batches" in readobj and "\nchk_rtn_cd" in readobj:
                find_start=re.findall(r'AEDW_OPEN_BATCHES_FILE\=\$\{TEMP_DIR\}\/\$\{ETL_INTF_CD\}_OPEN_BATCHES_\$\{YMD_DATE\}_\$\{HMS_TIME\}\.txt[\s]*\n[\s]*f_get_aedw_open_batches',readobj,flags=re.I|re.DOTALL)
                find_end=re.findall(r'\nchk_rtn_cd.*\necho \"\$\{BQ_OUTPUT\}\" \| sed \'1d\' \> \$\{AEDW_OPEN_BATCHES_FILE\}',readobj,flags=re.I|re.DOTALL)
                if find_start and find_end:
                    final_lst.append(find_start[0])
                    final_lst.append(find_end[0])
                    final_result='Y'
                else:
                    final_lst.append('f_get_aedw_open_batches')
                    final_lst.append('chk_rtn_cd')
                    final_result='N'


        if final_result=='NA':
            log_result.append(Result_NA())
            result_for_f_get_aedw_open_batches.append("NA")
        elif final_result=='Y':
            log_result.append(Output_Result_Yes(final_lst))
            result_for_f_get_aedw_open_batches.append("Y")
        else:
            log_result.append(Output_Result_No(final_lst))
            result_for_f_get_aedw_open_batches.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_f_get_aedw_open_batches.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_f_execute_bteq_calling(log_result,filename,result_for_f_execute_bteq_calling):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("f_execute_bteq_calling"))
    try:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            inputobj = f.read()
            inputobj=re.sub(r'\n[\s]*','\n',inputobj,flags=re.I|re.DOTALL)
            final_result='NA'
            if "\nf_execute_bteq" in inputobj and "\nchk_rtn_cd" in inputobj:
                with open(OutputFolderPath+filename, 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open(OutputFolderPath+filename, "r",encoding=e) as f:
                    readobj = f.read()
                    final_lst=[]
                    find_start=re.findall(r'f_execute_bq "\$TEMPSQL_DIR\/\${PROGBASE}_\${STEP_NUMBER}.sql" "F"[\s]+RC=\$\?[\s]+echo \$BQ_OUTPUT[\s]+if \[\[ \$RC \-ne 0 \]\][\s]+then[\s]+echo "ERROR!!! Error in TEMP_SQL "[\s]+echo "Exiting with exit status as 1 "[\s]+exit 1[\s]+fi[\s]+echo "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"',readobj,flags=re.I|re.DOTALL)
                    if find_start :
                        final_lst.append(find_start[0])
                        final_result='Y'
                    else:
                        final_lst.append('f_execute_bteq')
                        final_lst.append('chk_rtn_cd')
                        final_result='N'


        if final_result=='NA':
            log_result.append(Result_NA())
            result_for_f_execute_bteq_calling.append("NA")
        elif final_result=='Y':
            log_result.append(Output_Result_Yes(final_lst))
            result_for_f_execute_bteq_calling.append("Y")
        else:
            log_result.append(Output_Result_No(final_lst))
            result_for_f_execute_bteq_calling.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_f_execute_bteq_calling.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_fn_sql(log_result,filename,result_for_fn_sql):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("fn_sql"))
    try:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            inputobj=f.read()
            final_result='NA'
            final_lst=[]
            find_pattern=re.findall(r'[\w]+[\s]*\=[\s]*\`fn\_sql[\s]*\"\$\{.*\}\"\`',inputobj,flags=re.I)
            if find_pattern:
                with open(OutputFolderPath+filename, 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open(OutputFolderPath+filename, "r",encoding=e) as f:
                    outputobj=f.read()
                    find_output=re.findall(r'[\w]+[\s]*\=[\s]*\`fn\_sql[\s]*\"\$\{.*\}\"\`',outputobj,flags=re.I)
                    if find_output:
                        final_lst.append(find_output[0])
                        final_result='N'
                    else:
                        find_table=re.findall(r'\"\$\{(.*)\}\"',find_pattern[0],flags=re.I)
                        find_parameter=re.findall(r'([\w]+)[\s]*\=',find_pattern[0],flags=re.I)
                        # obj='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table[0]+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n'+find_parameter[0]+'=$(echo "$BQ_OUTPUT | sed 1d")'
                        # if obj in outputobj:
                        # print("SS",find_table[0],"SS")
                        find_obj=re.findall(r'f_execute_bq "\${'+find_table[0]+'}[\s]*"[\s]+RC\=\$\?[\s]+echo \$BQ_OUTPUT[\s]+if \[\[ \$RC \-ne 0 \]\][\s]+then[\s]+echo "ERROR\!\!\! Error in \$\{'+find_table[0]+'}[\s]*"[\s]+echo "Exiting with exit status as 1 "[\s]+exit 1[\s]+fi[\s]+echo "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"',outputobj,flags=re.I|re.DOTALL)
                        # find_obj=re.findall(r'f_execute_bq "\$\{'+find_table[0]+'\}[\s]*"[\s]+RC\=\$\?[\s]+echo \$BQ_OUTPUT[\s]+if \[\[ \$RC \-ne 0 \]\][\s]+then[\s]+echo "ERROR\!\!\! Error in "\$\{'+find_table[0]+'\}[\s]*" [\s]+echo "Exiting with exit status as 1 "[\s]+exit 1[\s]+fi[\s]+echo "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"[\s]+'+find_parameter[0]+'=\$\(echo "\$BQ_OUTPUT \| sed 1d"\)',outputobj,flags=re.I|re.DOTALL)
                        if find_obj:
                            final_lst.append(find_obj[0])
                            final_result='Y'
                        else:
                            final_lst.append(find_pattern[0])
                            final_result='N'

        if final_result=='NA':
            log_result.append(Result_NA())
            result_for_fn_sql.append("NA")
        elif final_result=='Y':
            log_result.append(Output_Result_Yes(final_lst))
            result_for_fn_sql.append("Y")
        else:
            log_result.append(Output_Result_No(final_lst))
            result_for_fn_sql.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_fn_sql.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_Seq_MstSeq(log_result,filename,result_for_Seq_MstSeq):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Seq_MstSeq to DSTAGEPROJ"))
    try:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            inputobj=f.read()
            final_result='NA'
            final_lst=[]
            find_pattern=re.findall(r'\/Seq\_MstSeq\_.*\$DSTAGEPROJ',inputobj,flags=re.I)
            if find_pattern:
                with open(OutputFolderPath+filename, 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open(OutputFolderPath+filename, "r",encoding=e) as f:
                    outputobj=f.read()
                    find_output=re.findall(r"\/exec_interface\.ksh \$\{ETL_INTF_CD\} 'Y'",outputobj,flags=re.I)
                    if len(find_pattern) == len(find_output):
                        for ele in find_output:
                            final_lst.append(ele)
                        final_result='Y'
                    else:
                        find_pattern_out=re.findall(r'\/Seq\_MstSeq\_.*\$DSTAGEPROJ',outputobj,flags=re.I)
                        for ele in find_pattern_out:
                            final_lst.append(ele)
                        final_result='N'

        if final_result=='NA':
            log_result.append(Result_NA())
            result_for_Seq_MstSeq.append("NA")
        elif final_result=='Y':
            log_result.append(Output_Result_Yes(final_lst))
            result_for_Seq_MstSeq.append("Y")
        else:
            log_result.append(Output_Result_No(final_lst))
            result_for_Seq_MstSeq.append("N")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_Seq_MstSeq.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_comment_for_usrid_pwd_tdpid_logon(log_result,filename,result_for_comment_for_usrid_pwd_tdpid_logon):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("comment_for_usrid_pwd_tdpid_logon"))
    try:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            filelines=f.readlines()
            correct_lst=[]
            wrong_lst=[]
            for line in filelines:
                find_cmt_wrong=re.findall(r'^[\s]*usrid[\s]*\=|^[\s]*pwd[\s]*\=|^[\s]*tdpid[\s]*\=|^[\s]*logon[\s]*\=',line,flags=re.I)
                find_cmt_right=re.findall(r'^[\s]*\#[\s]*usrid[\s]*\=|^[\s]*\#[\s]*pwd[\s]*\=|^[\s]*\#[\s]*tdpid[\s]*\=|^[\s]*\#[\s]*logon[\s]*\=',line,flags=re.I)
                if find_cmt_wrong:
                    wrong_lst.append(line)
                elif find_cmt_right:
                    correct_lst.append(line)


        if len(correct_lst)==0 and len(wrong_lst)==0:
            log_result.append(Result_NA())
            result_for_comment_for_usrid_pwd_tdpid_logon.append("NA")
        elif wrong_lst:
            log_result.append(Output_Result_No(wrong_lst))
            result_for_comment_for_usrid_pwd_tdpid_logon.append("N")
        elif correct_lst:
            log_result.append(Output_Result_Yes(correct_lst))
            result_for_comment_for_usrid_pwd_tdpid_logon.append("Y")

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_for_comment_for_usrid_pwd_tdpid_logon.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def cheak_fun_cheak_function_call(log_result,filename,result_cheak_function_call):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Function   Call"))
    try:
        input_function_list=[]
        input_for_sql_function=[]
        correct_lst=[]
        wrong_lst=[]
        fun_start=[]
        fun_end=[]
        if 1==1:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:

                fileLine1 = f.readlines()
                f.seek(0)
                final_obj=f.read()
                fun_name=''
                fun_sed=''
                fun_counter=0
                fun_obj=''
                final_result="NA"
                for line in fileLine1:
                    if fun_counter==0:
                        find_fun=re.findall(r'^[\s]*([\w]+)[\s]*\(\)[\s]*\{',line,flags=re.I|re.DOTALL)
                        if find_fun:
                            fun_name=find_fun[0]
                            fun_counter=1
                            fun_obj=line
                    elif fun_counter==1:
                        find_end=re.findall(r'^[\s]*\}[\s]*$',line)
                        if find_end:
                            # print(fun_obj)
                            fun_counter=0
                            fun_obj=fun_obj+line
                            find_stat=re.findall(r'\n[\s]*CALL[\s]+|\n[\s]*RENAME[\s]+TABLE[\s]+|\n[\s]*DROP[\s]+|\n[\s]*MERGE[\s]+|\n[\s]*SELECT[\s]+|\n[\s]*ALTER[\s]+|\n[\s]*INSERT[\s]+|\n[\s]*DELETE[\s]+|\n[\s]*DELETE[\s]+FROM|\n[\s]*INS[\s]+|\n[\s]*CREATE[\s]+|\n[\s]*UPDATE[\s]+|\n[\s]*UPD[\s]+|\n[\s]*SEL[\s]+|\n[\s]*DEL[\s]+FROM|\n[\s]*TRUNCATE[\s]+|echo[\s]+\"CALL[\s]+|echo[\s]+\"RENAME[\s]+TABLE[\s]+|echo[\s]+\"DROP[\s]+|echo[\s]+\"MERGE[\s]+|echo[\s]+\"SELECT[\s]+|echo[\s]+\"ALTER[\s]+|echo[\s]+\"INSERT[\s]+|echo[\s]+\"DELETE[\s]+|echo[\s]+\"DELETE[\s]+FROM|echo[\s]+\"INS[\s]+|echo[\s]+\"CREATE[\s]+|echo[\s]+\"UPDATE[\s]+|echo[\s]+\"UPD[\s]+|echo[\s]+\"SEL[\s]+|echo[\s]+\"DEL[\s]+FROM|echo[\s]+\"TRUNCATE[\s]+',fun_obj,flags=re.I|re.DOTALL)
                            if fun_sed!='' and find_stat:
                                # print("%%%%%%%%%%%",find_stat)
                                find_bteq=re.findall(r"bteq[\s]*\<\<[\s]*([\w]+)",fun_sed,flags=re.I)
                                find_sed=re.findall(r''+find_bteq[0]+'([\s]*\|[\s]*.*)',fun_sed)
                                # find_fun_call=re.findall(r'\n[\s]*\#*[\s]*[\w]+[\s]*\=[\s]*\$\('+fun_name+'\)',final_obj,flags=re.I)
                                # if find_fun_call:
                                #     correct_lst.append(find_fun_call[0])
                                #     split_call=find_fun_call[0].split("=")
                                #     function_name=find_fun_call[0],'\n'+fun_name+split_call[0]+"""=$(echo "${BQ_OUTPUT}" | sed 1d  |sed -E 's/(^")|("$)//g'"""+find_sed[0]+')'
                                #     print("*****",function_name)
                                #     input_function_list.append(function_name)
                                find_dynamic_fun=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\`'+fun_name+'.*\`',final_obj,flags=re.I)
                                for ele in find_dynamic_fun:

                                    correct_lst.append(ele)
                                    find_fun_Parameter=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\`'+fun_name+'(.*)\`',ele,flags=re.I)
                                    split_call=ele.split("=")
                                    function_name1=fun_name+find_fun_Parameter[0]+split_call[0]+"""=$(echo "${BQ_OUTPUT}" | sed 1d  |sed -E 's/(^")|("$)//g'"""+find_sed[0]+')'
                                    print("^^^^^^^^^^^^^",function_name1)
                                    input_function_list.append(function_name1)

                                find_dynamic_fun_with_echo=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\`[\s]*echo[\s]*\$[\s]*\([\s]*'+fun_name+'.*\`',final_obj,flags=re.I)
                                for ele in find_dynamic_fun_with_echo:
                                    correct_lst.append(ele)
                                    find_fun_Parameter=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\`[\s]*echo[\s]*\$[\s]*\([\s]*'+fun_name+'(.*)\)[\s]*\|',ele,flags=re.I)
                                    find_sed_in_fun_call=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\`[\s]*echo[\s]*\$[\s]*\([\s]*'+fun_name+'.*\)*[\s]*(\|.*)\`',ele,flags=re.I)

                                    split_call=ele.split("=")
                                    function_name2=fun_name+find_fun_Parameter[0]+split_call[0]+"""=$(echo "${BQ_OUTPUT}" | sed 1d  |sed -E 's/(^")|("$)//g'"""+find_sed[0]+find_sed_in_fun_call[0]+')'
                                    input_function_list.append(function_name2)


                                fun_sed=''

                        else:
                            fun_obj=fun_obj+line
                            find_bteq=re.findall(r"bteq[\s]*\<\<[\s]*([\w]+)",line,flags=re.I)
                            if find_bteq and "sed" in line:
                                fun_sed=line

            function_list=["fn_sql_exec","fn_sql_q" ,"fn_sql_export" ,"fn_sql" ,"fn_count_sql" ,"fn_sql_process" ,"fn_sql_activity_count" ,"exec_sql" ,"fn_sql_update" ,"fn_sql_run" ,"fun_sql"]
            for function_name in function_list:


                with open(inputFolderPath+filename, "r", encoding=e) as f:
                    fileLines = f.readlines()
                    fn_sql_counter=0
                    final_obj=""
                    obj=""

                    for fileLine in fileLines:
                        find_fn_sql=re.findall(r"^[\s]*"+function_name+"[\s]*\"CALL[\s]*|^[\s]*"+function_name+"[\s]*\"RENAME[\s]*TABLE[\s]*|^[\s]*"+function_name+"[\s]*\"DROP[\s]*|^[\s]*"+function_name+"[\s]*\"MERGE[\s]*|^[\s]*"+function_name+"[\s]*\"SELECT[\s]*|^[\s]*"+function_name+"[\s]*\"ALTER[\s]*|^[\s]*"+function_name+"[\s]*\"INSERT[\s]*|^[\s]*"+function_name+"[\s]*\"DELETE[\s]*|^[\s]*"+function_name+"[\s]*\"DELETE[\s]*FROM|^[\s]*"+function_name+"[\s]*\"INS[\s]*|^[\s]*"+function_name+"[\s]*\"CREATE[\s]*|^[\s]*"+function_name+"[\s]*\"UPDATE[\s]*|^[\s]*"+function_name+"[\s]*\"UPD[\s]*|^[\s]*"+function_name+"[\s]*\"SEL[\s]*|^[\s]*"+function_name+"[\s]*\"DEL[\s]*FROM|^[\s]*"+function_name+"[\s]*\"TRUNCATE[\s]*",fileLine,flags=re.I)
                        if fn_sql_counter==0:

                            obj=''
                            if find_fn_sql:
                                output_function=re.sub(r''+function_name+'','f_execute_bq',find_fn_sql[0],flags=re.I)


                                fn_sql_counter=1
                                obj=obj+fileLine
                                find_semi=re.findall(r'\"[\s]*$',fileLine,flags=re.I)
                                if find_semi:
                                    fn_sql_counter=0
                                    # print(obj)
                                    correct_lst.append(obj)
                                    obj=re.sub(r''+function_name+'','f_execute_bq',obj,flags=re.I|re.DOTALL)
                                    find_function=re.findall(r""+output_function+"[\s]*\$\{[\w]+\}[\s]*\.[\s]*[\w]+",obj,flags=re.I|re.DOTALL)
                                    for ele in find_function:
                                        input_function_list.append(ele)
                                    # obj=obj+fileLine
                                    final_obj=final_obj+obj

                            else:
                                final_obj=final_obj+fileLine

                        elif fn_sql_counter==1 :
                            find_semi=re.findall(r'\"[\s]*$',fileLine,flags=re.I)
                            if find_semi:
                                fn_sql_counter=0
                                obj=obj+fileLine
                                # print('ppppppppp',obj)
                                obj=re.sub(r''+function_name+'','f_execute_bq',obj,flags=re.I|re.DOTALL)
                                find_function=re.findall(r""+output_function+"[\s]*\$\{[\w]+\}[\s]*\.[\s]*[\w]+",obj,flags=re.I|re.DOTALL)
                                for ele in find_function:
                                    input_function_list.append(ele)
                                final_obj=final_obj+obj

                            else:
                                obj=obj+fileLine

                with open(inputFolderPath+filename, "r", encoding=e) as f:
                    fileLines = f.readlines()
                    fun_counter=0
                    fn_sql_counter=0
                    final_obj=""
                    obj=""
                    fun_sed=''


                    for fileLine in fileLines:
                        if fun_counter==0:
                            find_fun=re.findall(r'^[\s]*'+function_name+'[\s]*\(\)[\s]*\{',fileLine,flags=re.I|re.DOTALL)
                            if find_fun:
                                fun_name=find_fun[0]
                                fun_counter=1
                                # fun_obj=fileLine
                        elif fun_counter==1:
                            find_end=re.findall(r'^[\s]*\}[\s]*$',fileLine)
                            if find_end:
                                # print(fun_obj)
                                fun_counter=0
                                # fun_obj=fun_obj+line

                            else:
                                # fun_obj=fun_obj+line
                                find_bteq=re.findall(r"bteq[\s]*\<\<[\s]*([\w]+)",fileLine,flags=re.I)
                                if find_bteq and "sed" in fileLine:
                                    fun_sed=fileLine


                        if fn_sql_counter==0:

                            obj=''
                            find_fn_sql=re.findall(r"^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"CALL[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"RENAME[\s]*TABLE[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"DROP[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"MERGE[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"SELECT[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"ALTER[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"INSERT[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"DELETE[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"DELETE[\s]*FROM|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"INS[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"CREATE[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"UPDATE[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"UPD[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"SEL[\s]*|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"DEL[\s]*FROM|^[\s]*[\w]+[\s]*\=[\s]*\`[\s]*"+function_name+"[\s]*\"TRUNCATE[\s]*",fileLine,flags=re.I)
                            if find_fn_sql and fun_sed!='':
                                output_function=re.sub(r'[\w]+[\s]*\=[\s]*\`'+function_name+'','f_execute_bq',find_fn_sql[0],flags=re.I)
                                # print(output_function)

                                fn_sql_counter=1
                                obj=obj+fileLine
                                find_semi=re.findall(r'\`[\s]*$',fileLine,flags=re.I)
                                if find_semi:
                                    fn_sql_counter=0
                                    sed_var=re.findall(r'\<\<[\s]*[\w]+(.*)',fun_sed)
                                    correct_lst.append(obj)
                                    obj=re.sub(r'\`|\n',' ',obj)
                                    find_fn_call=re.findall(r'([\w]+)[\s]*\=[\s]*'+function_name+'',obj,flags=re.I)
                                    obj=re.sub(r'[\w]+[\s]*\=[\s]*'+function_name+'','f_execute_bq',obj,flags=re.I|re.DOTALL)
                                    obj=obj+'\n'+find_fn_call[0]+'=$(echo "${BQ_OUTPUT}" | sed 1d'+sed_var[0]+')\n'
                                    # print(obj)
                                    # input_function_list.append(obj)
                                    find_end=find_fn_call[0]
                                    # print(find_end)
                                    # output_function=re.sub(r"[\s]+","",output_function)
                                    # find_end=re.sub(r"[\s]+","",find_end)
                                    fun_start.append(output_function)
                                    fun_end.append(find_end)
                                    # input_function_list.append(obj)

                                    final_obj=final_obj+obj

                            else:
                                final_obj=final_obj+fileLine

                        elif fn_sql_counter==1 :
                            find_semi=re.findall(r'\`[\s]*$',fileLine,flags=re.I)
                            if find_semi:
                                fn_sql_counter=0
                                obj=obj+fileLine
                                correct_lst.append(obj)
                                sed_var=re.findall(r'\<\<[\s]*[\w]+(.*)',fun_sed)
                                obj=re.sub(r'\`|\n',' ',obj)
                                find_fn_call=re.findall(r'([\w]+)[\s]*\=[\s]*'+function_name+'',obj,flags=re.I)
                                obj=re.sub(r'[\w]+[\s]*\=[\s]*'+function_name+'','f_execute_bq',obj,flags=re.I|re.DOTALL)
                                obj=obj+'\n'+find_fn_call[0]+'=$(echo "${BQ_OUTPUT}" | sed 1d'+sed_var[0]+')\n'
                                # input_function_list.append(obj)
                                find_end=find_fn_call[0]
                                # print(find_end)
                                # output_function=re.sub(r"[\s]+","",output_function)
                                # find_end=re.sub(r"[\s]+","",find_end)
                                fun_start.append(output_function)
                                fun_end.append(find_end)
                                # input_function_list.append(obj)
                                # print(obj)
                                final_obj=final_obj+obj

                            else:
                                obj=obj+fileLine


                with open(inputFolderPath+filename, "r", encoding=e) as f:
                    final_obj = f.read()
                    # print(final_obj)
                    find_fn_sql=re.findall(r'\n[\s]*'+function_name+'[\s]*\"\$\{[\w]+\}\"',final_obj,flags=re.I)
                    # print(find_fn_sql)
                    for i in find_fn_sql:
                        # print("1",i)
                        correct_lst.append(i)

                        if i.startswith("#"):
                            pass
                        else:
                            find_table=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)
                            find_table1=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)
                            # print("dd",find_table)
                            find_table1=re.sub(r'\"','',find_table1[0],flags=re.I)
                            # find_parameter=re.findall(r'([\w]+)[\s]*\=',i,flags=re.I)
                            final_function='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table1+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"'
                            # print(final_function)
                            input_function_list.append(final_function)



                    find_fn_sql=re.findall(r'\#*[\w]+[\s]*\=[\s]*\`'+function_name+'[\s]*\"\$\{.*\}\"\`',final_obj,flags=re.I)
                    if find_fn_sql:
                        for i in find_fn_sql:
                            print(i)
                            correct_lst.append(i)

                            if i.startswith("#"):
                                pass
                            else:
                                find_table=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)
                                find_table1=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)
                                # print("dd",find_table)
                                find_table1=re.sub(r'\"','',find_table1[0],flags=re.I)

                                find_parameter=re.findall(r'([\w]+)[\s]*\=',i,flags=re.I)
                                final_functions='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table1+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n'+find_parameter[0]+'=$(echo "$BQ_OUTPUT" | sed 1d)'
                                print("444444444444",final_functions)
                                input_function_list.append(final_functions)
                            # print(final_obj)

                    else:
                        find_fn_sql1=re.findall(r'\#*[\w]+[\s]*\=[\s]*\`'+function_name+'[\s]*\".*',final_obj,flags=re.I)

                        # sp_opt=`fn_sql_sp "call_stm${ts}"|awk -F '|' '{print $2}'|tr -cd '[a-z] [A-Z] [0-9] _ .'|sed "s/^. *//"|sed "s/ *.$//g"`
                        # cd_tbl_sql_data=fn_sql "${cd_tbl_sql}"|tr '\n' '@'|sed 's/@$//'
                        for i in find_fn_sql1:
                            print("2",i)
                            # print(i)
                            # print(find_fn_sql)
                            correct_lst.append(i)
                            if i.startswith("#"):
                                pass
                            else:
                                find_table=re.findall(r'\"[\w]*\$\{.*\}\"',i,flags=re.I)

                                # print(find_table)
                                find_table1=re.sub(r'\"','',find_table[0],flags=re.I)
                                find_table1=re.sub(r'\}\"','}',find_table1,flags=re.I)
                                find_table2=re.findall(r'\"[\w]*\$\{.*\}\"(.*)',i,flags=re.I)
                                find_table2=re.sub(r'\`','',find_table2[0],flags=re.I)
                                #
                                find_parameter=re.findall(r'([\w]+)[\s]*\=',i,flags=re.I)
                                final_function3='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table1+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n'+find_parameter[0]+'=$(echo "$BQ_OUTPUT" | sed 1d | sed -n \'/^%/p\'|sed -e "s/^%//'+find_table2+')'
                                print("****",final_function3)
                                input_function_list.append(final_function3)




                    # find_fn_sql=re.findall(r'\#*[\w]+[\s]*\=[\s]*\`'+function_name+'[\s]*\"\$\{.*\}\"\|.*\`',final_obj,flags=re.I)
                    # for i in find_fn_sql:
                    #     print("****",i)
                    #     correct_lst.append(i)
                    #     if i.startswith("#"):
                    #         pass
                    #     else:
                    #         find_table=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)
                    #
                    #         # print(find_table)
                    #         find_table1=re.sub(r'\"\$','$',find_table[0],flags=re.I)
                    #         find_table1=re.sub(r'\}\"','}',find_table1,flags=re.I)
                    #         find_table2=re.findall(r'\"\$\{.*\}\"(.*)',i,flags=re.I)
                    #         find_table2=re.sub(r'\`','',find_table2[0],flags=re.I)
                    #         #
                    #         find_parameter=re.findall(r'([\w]+)[\s]*\=',i,flags=re.I)
                    #         final_function1='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table1+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n'+find_parameter[0]+'=$(echo "$BQ_OUTPUT" | sed 1d'+find_table2+')'
                    #         # print(final_function1)
                    #         input_function_list.append(final_function1)
                    find_fn_sql=re.findall(r'\n[\s]*[\w]+[\s]*\=[\s]*\$[\s]*\('+function_name+'[\s]*\".*\"\)',final_obj,flags=re.I)
                    for i in find_fn_sql:
                        correct_lst.append(i)
                        find_table=re.findall(r'\"\$\{.*\}\"',i,flags=re.I)

                        find_table1=re.sub(r'\"\$','$',find_table[0],flags=re.I)
                        find_table1=re.sub(r'\}\"','}',find_table1,flags=re.I)
                        find_table2=re.findall(r'\"\$\{.*\}\"(.*)',i,flags=re.I)
                        find_table2=re.sub(r'\`','',find_table2[0],flags=re.I)
                        #
                        find_parameter=re.findall(r'([\w]+)[\s]*\=',i,flags=re.I)
                        final_function2='f_execute_bq '+find_table[0]+'\nRC=$?\necho $BQ_OUTPUT\nif [[ $RC -ne 0 ]]\nthen\necho "ERROR!!! Error in '+find_table1+' "\necho "Exiting with exit status as 1 "\nexit 1\nfi\necho "SUCCESSFULLY EXECUTED BIGQUERY COMMAND"\n'+find_parameter[0]+'=$(echo "$BQ_OUTPUT" | sed 1d'+find_table2+')'
                        # print(final_function2)
                        input_function_list.append(final_function2)


            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:

                fileLines = f.read()
                split_file=fileLines.split('=$(echo "${BQ_OUTPUT}"')
                sql_fun_lst=[]

                for index,ele in enumerate(fun_start):
                    counter=0
                    for fileobj in split_file:
                        ele=ele.strip()
                        find_query=re.findall(r''+ele+'.*'+fun_end[index]+'',fileobj,flags=re.I|re.DOTALL)
                        if find_query:
                            counter=1
                            input_for_sql_function.append(find_query[0]+'=$(echo "${BQ_OUTPUT}" | sed 1d')
                    if counter==0:
                        # print(ele)
                        sql_fun_lst.append(ele)

                fileLines=re.sub(r"[\s]+","",fileLines)
                for index,ele in enumerate(input_function_list):
                    # print(ele)
                    element=re.sub(r"[\s]+","",str(ele))
                    if element.lower() in fileLines.lower():

                        final_result="Y"
                    else:
                        final_result="N"
                        wrong_lst.append(correct_lst[index])

                for ele in sql_fun_lst:
                    wrong_lst.append(ele)
                for ele in input_for_sql_function:
                    input_function_list.append(ele)

            if final_result=='NA':
                log_result.append(Result_NA())
                result_cheak_function_call.append("NA")
            elif wrong_lst:
                wrong_lst.append("Above function calling is not handle properly in output file.")
                log_result.append(Result_No(correct_lst,wrong_lst))
                result_cheak_function_call.append("N")
            elif correct_lst and len(wrong_lst)==0:
                log_result.append(Result_Yes(correct_lst,input_function_list))
                result_cheak_function_call.append("Y")


    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_cheak_function_call.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_comment_present_in_echo(log_result,filename,result_check_echo_comment):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("#_comment_in_echo"))
    try:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            filelines=f.readlines()
            correct_lst=[]
            wrong_lst=[]
            echo_counter=0
            echo_obj=[]
            lst=[]
            for line in filelines:
                if echo_counter==0:
                    find_echo=re.findall(r'^[\s]*echo[\s]*\"',line,flags=re.I|re.DOTALL)
                    if find_echo:
                        for ele in lst:
                            echo_obj.append(ele)
                        lst=[]
                        find_quotes=re.findall(r'"',line)
                        if len(find_quotes)>1:
                            pass
                        else:
                            lst.append(line)
                            echo_counter=1
                elif echo_counter==1:
                    find_end=re.findall(r'"[\s]*\>[\s]*\$\{|"[\s]*\>\>[\s]*\$\{',line,flags=re.I)
                    find_echo=re.findall(r'^[\s]*echo[\s]*\"',line,flags=re.I|re.DOTALL)
                    if find_end:
                        echo_counter=0
                        lst.append(line)
                    elif find_echo:
                        echo_counter=0
                    else:
                        lst.append(line)
            for ele in lst:
                echo_obj.append(ele)
            for line in echo_obj:
                if line.strip().startswith("# "):
                    wrong_lst.append(line)



        if wrong_lst:
            log_result.append(Output_Result_No(wrong_lst))
            result_check_echo_comment.append("N")
        else:
            log_result.append(Output_Result_Yes(["# comment is not present in echo block."]))
            result_check_echo_comment.append("Y")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_echo_comment.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)



#Saadia
def check_execute_query_before_sql(log_result,filename,result_execute_query_before_sql):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Execute query added"))
    try:
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e = result['encoding']
            with open(OutputFolderPath+filename, "r", encoding=e) as f:
                fileLines = f.readlines()
                file_obj = f.read()
                statment_count = 0
                stat_list = []
                input_list = []
                for fileLine in fileLines:
                    if statment_count == 0:
                        obj = ""
                        find_stat = re.findall(r'echo \"CALL[\s]+|echo \"RENAME[\s]+|echo \"DROP[\s]+|echo \"MERGE[\s]+|echo \"SELECT[\s]+|echo \"ALTER[\s]+'
                                               r'|echo \"INSERT[\s]+|echo \"DELETE[\s]+|echo \"INS[\s]+|echo \"CREATE[\s]+|echo \"UPDATE[\s]+|echo \"UPD[\s]+'
                                               r'|echo \"SEL[\s]+|echo \"DEL[\s]+|echo \"TRUNCATE[\s]+|echo \"REPLACE[\s]+', fileLine.lstrip(), flags=re.I|re.DOTALL)
               
                        if find_stat:
                            statment_count = 1
                            obj = obj + fileLine
                        if ";" in fileLine.upper():
                            statment_count = 0
                            obj = obj + fileLine
                        
                    else:
                        if statment_count == 1 :
                            if ";" in fileLine.upper():
                                statment_count = 0
                                obj = obj + fileLine
                                stat_list.append(obj)
                                
                            else:
                                obj = obj + fileLine
                    
                # print(len(stat_list), stat_list)
                for ele in stat_list:
                    ele = re.sub('^echo "', "", ele)
                    if 'echo "'in ele:
                        input_list.append("Echo inside echo, Check file.")

            with open(OutputFolderPath+filename, "r", encoding=e) as f:
                read_obj = f.readlines()
                
                pass_statement = 0
                comment=0
                for readline in read_obj:
                    if pass_statement == 0:
                        if readline.upper().startswith('EXECUTE_QUERY'):
                            pass_statement =1
                        else:
                            cmt=re.findall(r'^[\s]*\-\-|^[\s]*\#',readline)
                            if readline.strip().startswith("/*") and comment==0 :
                                comment=1
                                if readline.strip().endswith("*/"):
                                    comment=0
                            elif comment==1 and readline.strip().endswith("*/"):
                                comment=0
                            elif cmt:
                                pass
                            elif comment == 0:
                                find_stat=re.findall(r'^CALL[\s]+|^RENAME[\s]+|^DROP[\s]+|^MERGE[\s]+|^SELECT[\s]+|^ALTER[\s]+|^INSERT[\s]+|^DELETE[\s]+|^INS[\s]+|^CREATE[\s]+|^UPDATE[\s]+|^UPD[\s]+|^SEL[\s]+|^DEL[\s]+|^TRUNCATE[\s]+|^REPLACE[\s]+', readline.lstrip(), flags=re.I|re.DOTALL)
                               

        if find_stat or input_list:
            result_execute_query_before_sql.append("N")
            log_result.append(Output_Result_No(stat_list))
        else:
            result_execute_query_before_sql.append("Y")
            log_result.append(Output_Result_Yes(["Echo is not present in echo block."]))
            

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_execute_query_before_sql.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_bteq_logon_database(log_result,filename,result_check_bteq_logon_database):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Bteq Logon Database"))
    input_bteq_logon = []
    output_bteq_logon = []
    input_log = []
    output_log = []
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e = result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLines = f.read()
                find_input = re.findall(r"\#[\s]*BTEQ[\s]*\<\<[\w]+|\#[\s]*\.LOGON.*?\;|\#[\s]*DATABASE[\s]*\$\{.*?\}\;", fileLines, flags=re.I|re.DOTALL)
                for item in find_input:
                    input_log.append(item)
                    input_bteq_logon.append(item)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e = result['encoding']
            with open(OutputFolderPath+filename, "r", encoding=e) as f:
                fileLines = f.read()
                find_output = re.findall(r"\#[\s]*BTEQ[\s]*\<\<[\w]+|\#[\s]*\.LOGON.*?\;|\#[\s]*DATABASE[\s]*\$\{.*?\}\;", fileLines, flags=re.I|re.DOTALL)
                for item in find_output:
                    output_log.append(item)
                    output_bteq_logon.append(item)

        # print(input_bteq_logon)
        # print(output_bteq_logon)

        if len(input_bteq_logon)==0 and len(output_bteq_logon)==0:
            result_check_bteq_logon_database.append("NA")
            log_result.append(Result_NA())
        elif len(input_bteq_logon)>0 and len(output_bteq_logon)>0:
            if input_bteq_logon==output_bteq_logon:
                result_check_bteq_logon_database.append("Y")
                log_result.append(Result_Yes(input_log,output_log))
            else:
                result_check_bteq_logon_database.append("N")
                log_result.append(Result_No(input_log,output_log))
        else:
            result_check_bteq_logon_database.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_bteq_logon_database.append("Error")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_auther_comment_sadiaa(log_result,filename,result_Check_auther_comment_sadiaa):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Author Comment Sadiaa"))
    input_log=[]
    output_log=[]
    input_auther_comment=[]
    output_auther_comment=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                final_obj=[]
                cmt_bteq=[]
                for line in fileLine:
                    find_bteq=re.findall(r'^[\s]*bteq[\s]*\<\<[\s]*\!*([\w]+)',line,flags=re.I)
                    if find_bteq:
                        cmt_bteq.append(find_bteq[0])
                        line="# --"+line
                    find_bteq=re.findall(r'^[\s]*\#[\s]*\-\-[\s]*bteq[\s]*\<\<[\s]*\!*([\w]+)',line,flags=re.I)
                    if find_bteq:
                        cmt_bteq.append(find_bteq[0])
                    if line.strip() in cmt_bteq:
                        line="# --"+line
                    line=re.sub(r"^[\s]*\.[\s]*RUN[\s]+FILE[\s]*\=|^[\s]*\-\-[\s]*\.[\s]*RUN[\s]+FILE[\s]*\="," # -- .RUN FILE=",line,flags=re.I)
                    line=re.sub(r"^[\s]*\/\*[\s]*\.[\s]*RUN[\s]+FILE[\s]*\="," # /* .RUN FILE=",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*IF[\s]+ERRORCODE|^[\s]*\-\-[\s]*\.[\s]*IF[\s]+ERRORCODE"," # -- .IF ERRORCODE ",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*IF[\s]+ERRORLEVEL|^[\s]*\-\-[\s]*\.[\s]*IF[\s]+ERRORLEVEL"," # -- .IF ERRORLEVEL ",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*QUIT|^[\s]*\-\-[\s]*\.[\s]*QUIT"," # -- .QUIT ",line,flags=re.I)
                    line=re.sub(r"^[\s]*QUIT|^[\s]*\-\-[\s]*QUIT"," # -- quit ",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*good[\s]+exit|^[\s]*\-\-[\s]*\.[\s]*good[\s]+exit"," # -- .good exit",line,flags=re.I)
                    line=re.sub(r"^[\s]*good[\s]+exit|^[\s]*\-\-[\s]*good[\s]+exit"," # -- good exit",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*LABEL|^[\s]*\-\-[\s]*\.[\s]*LABEL"," # -- .LABEL",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*LOGOFF|^[\s]*\-\-[\s]*\.[\s]*LOGOFF"," # -- .logoff ",line,flags=re.I)
                    line=re.sub(r"^[\s]*LOGOFF|^[\s]*\-\-[\s]*LOGOFF"," # -- logoff ",line,flags=re.I)
                    line=re.sub(r"^[\s]*\$\{bteq_logon\}|^[\s]*\-\-[\s]*\$\{bteq_logon\}"," # ${bteq_logon}",line,flags=re.I)
                    line=re.sub(r"^[\s]*\$\{logon\}|^[\s]*\-\-[\s]*\$\{logon\}"," # ${logon}",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*EXPORT|^[\s]*\-\-[\s]*\.[\s]*EXPORT"," # -- .EXPORT ",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*OS|^[\s]*\-\-[\s]*\.[\s]*OS"," # -- .OS ",line,flags=re.I)
                    line=re.sub(r"^[\s]*bteq[\s]+\<\<|^[\s]*\-\-[\s]*bteq[\s]+\<\<"," # -- bteq <<",line,flags=re.I)
                    line=re.sub(r"^[\s]*bteq_logon[\s]*\=|^[\s]*\-\-[\s]*bteq_logon[\s]*\="," # -- bteq_logon=",line,flags=re.I)
                    line=re.sub(r"^[\s]*.logon|^[\s]*\-\-[\s]*.logon"," #  -- .logon",line,flags=re.I)
                    line=re.sub(r"^[\s]*\.[\s]*SET|^[\s]*\-\-[\s]*\.[\s]*SET"," # -- .SET ",line,flags=re.I)
                    line=re.sub(r"^[\s]*DATABASE[\s]+|^[\s]*\-\-[\s]*DATABASE[\s]+"," # -- DATABASE ",line,flags=re.I)
                    line=re.sub(r'^[\s]*bteq[\s]*\<\<','# bteq <<',line,flags=re.I|re.DOTALL)
                    line=re.sub(r'^[\s]*COLLECT|^[\s]*\-\-[\s]*COLLECT','# -- COLLECT',line,flags=re.I|re.DOTALL)
                    final_obj.append(line)

                # print(len(final_obj))
                for fileLine in final_obj:
                    find_cmt_in=re.findall(r'\#.*?\n', fileLine)
                    if find_cmt_in:
                        input_log.append(find_cmt_in[0])
                        find_cmt_in=re.sub(r"[\s]*|\n|\-", "", find_cmt_in[0])
                        input_auther_comment.append(find_cmt_in.lower())

        
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()

                for fileLine in fileLines:
                    find_cmt_out=re.findall(r'\#[\s]*.*?\n', fileLine, flags=re.I|re.DOTALL)
                    if find_cmt_out:
                        output_log.append(find_cmt_out[0])
                        find_cmt_out=re.sub(r"[\s]*|\n|\-", "", find_cmt_out[0])
                        output_auther_comment.append(find_cmt_out.lower())

            # print("input_author_comment", len(input_auther_comment), input_auther_comment)
            # print("output_author_comment", len(output_auther_comment),  output_auther_comment)

            if len(input_auther_comment)==0 and len(output_auther_comment) == 0:
                result_Check_auther_comment_sadiaa.append("NA")
                log_result.append(Result_NA())
            elif len(input_auther_comment)>0 and len(output_auther_comment)>0:
                if input_auther_comment==output_auther_comment:
                    result_Check_auther_comment_sadiaa.append("Y")
                    log_result.append(Result_Yes(input_log,output_log))
                else:
                    result_Check_auther_comment_sadiaa.append("N")
                    log_result.append(Result_No(input_log,output_log))
            else:
                result_Check_auther_comment_sadiaa.append("N")
                log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_auther_comment_sadiaa.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)


def Check_Database_Values_Added_Or_NOT(log_result,filename,fload_database_value_result,Schema_And_Value_Json):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Fload Database Values"))
    try:
        input_log=[]
        output_log=[]
        input_compare_data=[]
        output_compare_data=[]
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                error_db=''
                target_db=''
                table_name=''
                error_limit=''
                begin_loading_count=0
                for line in fileLine:
                    if table_name == '':
                        if begin_loading_count == 0:
                            find_error_limit=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*ERRLIMIT[\s]+([\d]+)',line,flags=re.I|re.DOTALL)
                            if find_error_limit:
                                input_log.append(line)
                                error_limit=find_error_limit[0]
                            find_begin_loading=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*BEGIN[\s]+LOADING',line,flags=re.I|re.DOTALL)
                            if find_begin_loading:
                                begin_loading_count = 1
                        elif begin_loading_count == 1:
                            find_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*([\w]+)\}*\.([\w]+)',line,flags=re.I|re.DOTALL)
                            if find_table:
                                input_log.append(line)
                                target_db=find_table[0][0]
                                table_name=find_table[0][1]
                    if error_db == '':
                        find_error_db=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*([\w]+)\}*\.[\w]+_ERR1',line,flags=re.I|re.DOTALL)
                        if find_error_db:
                            input_log.append(line)
                            error_db=find_error_db[0]
            # print(error_db,target_db,table_name)
        if filename in outputFile:
            json_obj=open(Schema_And_Value_Json,'r')
            json_data=json.load(json_obj)
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLine = f.readlines()
                for line in fileLine:
                    if target_db in json_data:
                        target_db_val=json_data[target_db]
                        find_target_db=re.findall(r'^[\s]*export[\s]+TARGET_DB[\s]*\=[\'\"]+'+target_db_val,line,flags=re.I|re.DOTALL)
                        if find_target_db:
                            output_log.append(line)
                    if error_db in json_data:
                        # print(error_db)
                        error_db_val=json_data[error_db]
                        find_error_db=re.findall(r'^[\s]*export[\s]+ERR_DB[\s]*\=[\'\"]+'+error_db_val,line,flags=re.I|re.DOTALL)
                        if find_error_db:
                            output_log.append(line)
                    find_delim_file=re.findall(r'^[\s]*DELIM_FILE_NAME[\s]*=[\s]*\$INPUT_FILE"_DELIM"',line,flags=re.I|re.DOTALL)
                    if find_delim_file:
                        output_log.append(line)
                    find_table_name=re.findall(r'^[\s]*TABLE_NM[\s]*=[\s]*[\"\']'+table_name,line,flags=re.I|re.DOTALL)
                    if find_table_name:
                        output_log.append(line)
                    find_stg_table_name=re.findall(r'^[\s]*STG_TABLE_NM[\s]*=[\s]*\$TABLE_NM"_TEMP"',line,flags=re.I|re.DOTALL)
                    if find_stg_table_name:
                        output_log.append(line)
                    find_other=re.findall(r'^[\s]*DELIM[\s]*\=[\"\']+\||^[\s]*ERR_LIMIT[\s]*=[\s]*'+error_limit+'|^[\s]*SKIP_ROWS[\s]*=[\s]*0',line,flags=re.I|re.DOTALL)
                    if find_other:
                        output_log.append(line)
            if len(input_log)==0 and len(output_log) == 0:
                fload_database_value_result.append("NA")
                log_result.append(Result_NA())
            elif len(output_log) == 8:
                fload_database_value_result.append("Y")
                log_result.append(Result_Yes(input_log,output_log))
            else:
                fload_database_value_result.append("N")
                log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        fload_database_value_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Delimiter_Added_In_sed_Command(log_result,filename,fload_delimiter_in_sed_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Delimeter In sed Command"))
    try:
        input_log=[]
        output_log=[]
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                begin_loading_count=0
                for line in fileLine:
                    # print(line)
                    if begin_loading_count == 0:
                        find_begin_loading=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*BEGIN[\s]+LOADING',line,flags=re.I|re.DOTALL)
                        if find_begin_loading:
                            begin_loading_count = 1
                    elif begin_loading_count == 1:
                        find_define=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*DEFINE[\s]*$',line,flags=re.I|re.DOTALL)
                        if find_define:
                            begin_loading_count = 2
                            column_size_lst=[]
                    elif begin_loading_count == 2:
                        find_end=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*FILE[\s]*\=',line,flags=re.I|re.DOTALL)
                        if find_end:
                            break
                            # begin_loading_count=0
                        else:
                            # print(line)
                            find_column_size=re.findall(r'[\w]+[\s]*\([\s]*[\w]+[\s]*\([\s]*([\d]+)',line,flags=re.I|re.DOTALL)
                            if find_column_size:
                                # print("a",find_column_size[0])
                                input_log.append(line)
                                column_size_lst.append(int(find_column_size[0]))
                # print(input_log,begin_loading_count)
                sed_cmd='sed '
                expected_cmd='sed '
                t_count=0
                col_count=0
                for i in column_size_lst[:-1]:
                    total_count=i+t_count+col_count
                    sed_cmd+="\-e[\s]*[\'\"]+s\/.\/&\|\/"+str(total_count)+'[\'\"]+[\s]+'
                    expected_cmd+='-e \'s/./&,/'+str(total_count)+'\' '
                    t_count+=i
                    col_count+=1
                sed_cmd+='\$INPUT_FILE[\s]*\>[\s]*\$DELIM_FILE_NAME'
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                if column_size_lst:
                    find_sed=re.findall(r''+sed_cmd,fileLines,flags=re.I|re.DOTALL)
                    if find_sed:
                        output_log.append(find_sed[0])

            if len(input_log)==0:
                fload_delimiter_in_sed_result.append("NA")
                log_result.append(Result_NA())
            elif output_log:
                fload_delimiter_in_sed_result.append("Y")
                log_result.append(Result_Yes(input_log,output_log))
            else:
                fload_delimiter_in_sed_result.append("N")
                log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        fload_delimiter_in_sed_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Drop_And_Create_Table(log_result,filename,fload_drop_and_create_table_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("DROP And CREATE Table Statement"))
    try:
        drop_table_lst=[]
        create_table_lst=[]
        drop_stat_lst=[]
        create_stat_lst=[]
        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                file_obj=''
                comment=0
                for line in fileLines:
                    cmt=re.findall(r'^[\s]*--',line)
                    if comment == 0 and "/*" in line:
                        if '*/' in line:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in line:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        file_obj=file_obj+line
                find_drop_table=re.findall(r'DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]+\$[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+\$[\w]+\.[\w]+',file_obj,flags=re.I|re.DOTALL)
                for table_stat in find_drop_table:
                    find_table=re.findall(r'\$[\w]+\.[\w]+',table_stat,flags=re.I|re.DOTALL)
                    if find_table:
                        drop_table_lst.append(find_table[0].upper().replace('$','\$'))
                        drop_stat_lst.append(table_stat)
                for table_name in drop_table_lst:
                    find_create_table=re.findall(r'CREATE[\s]+TABLE[\s]+'+table_name,file_obj,flags=re.I|re.DOTALL)
                    if find_create_table:
                        create_table_lst.append(table_name)
                        create_stat_lst.append(find_create_table[0])
                # print(drop_table_lst)
                # print(create_table_lst)

        if len(drop_table_lst)==0 and len(drop_table_lst) == 0:
            fload_drop_and_create_table_result.append("NA")
            log_result.append(Result_NA())
        elif drop_table_lst == create_table_lst:
            fload_drop_and_create_table_result.append("Y")
            log_result.append(Output_Result_Yes(drop_stat_lst+create_stat_lst))
        else:
            result_Check_auther_comment_sadiaa.append("N")
            log_result.append(Output_Result_No(drop_stat_lst+create_stat_lst))

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        fload_drop_and_create_table_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Error1_Table_Statement(log_result,filename,fload_error1_table_result):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Error1 Create Table Statement"))
    try:
        output_log=[]
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                error_table_name=''
                for line in fileLine:
                    find_error1_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*[\w]+\}*\.([\w]+_ERR1)',line,flags=re.I|re.DOTALL)
                    if find_error1_table:
                        error_table_name=find_error1_table[0]

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                file_obj=''
                comment=0
                for line in fileLines:
                    cmt=re.findall(r'^[\s]*--',line)
                    if comment == 0 and "/*" in line:
                        if '*/' in line:
                            comment=0
                            pass
                        else:
                            comment=1
                            pass
                    elif comment==1:
                        if "*/" in line:
                            #print(fileLine)
                            comment=0
                            pass
                        else:
                            pass
                    elif cmt:
                        pass
                    else:
                        file_obj=file_obj+line
        find_error1_table=re.findall(r'CREATE[\s]+TABLE[\s]+\$ERR_DB\.'+error_table_name+'[\s]*\([\s]*Loading_time[\s]+datetime[\s]*,[\s]*Loading_status[\s]+string[\s]*,[\s]*Table_Name[\s]+string[\s]*,[\s]*Error_desc[\s]+string[\s]*\)',file_obj,flags=re.I|re.DOTALL)
        if find_error1_table:
            output_log.append(find_error1_table[0])
            # print(find_error1_table)


        if error_table_name == '':
            fload_error1_table_result.append("NA")
            log_result.append(Result_NA())
        elif output_log:
            fload_error1_table_result.append("Y")
            log_result.append(Output_Result_Yes(output_log))
        else:
            fload_error1_table_result.append("N")
            log_result.append(Output_Result_No(["CREATE TABLE Statement Not Fount For "+error_table_name+" Table"]))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        fload_error1_table_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Error2_Table_Statement(log_result,filename,fload_error2_table_result,Translate_Table_Folder_Path):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Error2 Create Table Statement"))
    try:
        error_log=[]
        output_log=[]
        table_regex=''
        table_name_obj='\n'
        table_file_lst=glob.glob(Translate_Table_Folder_Path+'/*',recursive=True)
        for name in table_file_lst:
            table_name_obj+=os.path.basename(name)+'\n'
        # print(table_name_obj)
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                error_table_name=''
                target_db=''
                table_name=''
                err2_table=''
                insert_obj=''
                begin_loading_count=0
                tablefilename=''
                for line in fileLine:
                    # print(line,begin_loading_count,"@@",err2_table,"TT",table_name)
                    # if table_name == '':
                    if begin_loading_count == 0:
                        find_begin_loading=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*BEGIN[\s]+LOADING',line,flags=re.I|re.DOTALL)
                        if find_begin_loading:
                            begin_loading_count = 1
                    elif begin_loading_count == 1:
                        if err2_table == '':
                            find_err2_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*[\w]+\}*\.([\w]+_ERR2)',line,flags=re.I|re.DOTALL)
                            if find_err2_table:
                                err2_table=find_err2_table[0]
                                begin_loading_count=2
                        if table_name == '':
                            find_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*([\w]+)\}*\.([\w]+)',line,flags=re.I|re.DOTALL)
                            if find_table:
                                target_db=find_table[0][0]
                                table_name=find_table[0][1]

                            # print(table_name,target_db)
                    elif begin_loading_count == 2:
                        find_define=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*DEFINE[\s]*$',line,flags=re.I|re.DOTALL)
                        if find_define:
                            begin_loading_count = 3
                    elif begin_loading_count == 3:
                        find_end=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*FILE[\s]*\=',line,flags=re.I|re.DOTALL)
                        if find_end:
                            begin_loading_count=4
                        else:
                            # print(line)
                            find_column_name=re.findall(r'([\w]+)[\s]*\([\s]*[\w]+[\s]*\([\s]*[\d]+',line,flags=re.I|re.DOTALL)
                            if find_column_name:
                                pass
                                # print(find_column_name[0])
                    elif begin_loading_count == 4:
                        find_insert=re.findall(r'^[\s]*INSERT[\s]+INTO[\s]+\$\{*'+target_db+'\}*\.'+table_name+'[\W]',line,flags=re.I|re.DOTALL)
                        if find_insert:
                            insert_obj+=line
                            begin_loading_count=5
                    elif begin_loading_count == 5:
                        insert_obj+=line
                        find_end=re.findall(r'\;[\s]*$',line)
                        if find_end:
                            break
                if begin_loading_count == 5:
                    find_table_file_name=re.findall('\n([\w]+\.'+table_name+'\.*[\w]*)\n',table_name_obj,flags=re.I|re.DOTALL)
                    if find_table_file_name:
                        for name in table_file_lst:
                            if name.endswith(find_table_file_name[0]):
                                tablefilename=name
                                break

                    # print(tablefilename)
                    if tablefilename != '':
                        if insert_obj != '':
                            find_col_name=re.findall(r'INSERT[\s]+INTO[\s]+\$\{*'+target_db+'\}*\.'+table_name+'[\s]*\((.*?)\)[\s]*VALUES[\s]*\(',insert_obj,flags=re.I|re.DOTALL)
                            if find_col_name:
                                find_col_name=re.sub(r'[\s]+','',find_col_name[0].upper(),flags=re.I|re.DOTALL).split(',')
                                # print(find_col_name)

                            find_col_value=re.findall(r'\)[\s]*VALUES[\s]*\((.*?)\);',insert_obj,flags=re.I|re.DOTALL)
                            if find_col_value:
                                find_col_value=re.sub(r'[\s]+|\:','',find_col_value[0].upper(),flags=re.I|re.DOTALL).split(',')
                                # print(find_col_value)
                        else:
                            error_log.append("INSERT Statement Not Found For "+table_name)
                        if len(find_col_name) == len(find_col_value) and len(find_col_value) != 0 and len(find_col_name) != 0:
                            with open(tablefilename,'r') as tableobj:
                                table=tableobj.read()
                            table_regex='CREATE[\s]+TABLE[\s]+\$ERR_DB\.'+err2_table+'[\s]*\('
                            for i in range(0,len(find_col_name)):
                                # print(find_col_name[i],find_col_value[i])
                                col_name=find_col_name[i]
                                val_col_nm=find_col_value[i]
                                find_datatype=re.findall(r''+col_name+'[\s]+[\w]+[\s]*\([\d]+\,[\d]+\)|'+col_name+'[\s]+[\w]+[\s]*\([\d]+\)|'+col_name+'[\s]+[\w]+',table)
                                if find_datatype:
                                    datatype=re.findall(r'[\w]+[\s]+(.*)',find_datatype[0])[0]
                                    datatype=datatype.replace('(','\(')
                                    datatype=datatype.replace(')','\)')
                                    table_regex+=val_col_nm+'[\s]+'+datatype+'[\s]*\,[\s]*'
                            table_regex=re.sub(r'\,\[\\s\]\*$','',table_regex)
                            table_regex+=')'
                        else:
                            error_log.append("COLUMN Name And Values Are Not Equal In INSERT Statement for "+table_name)
                    else:
                        error_log.append("DDL Not Found for "+table_name+' TABLE')
                        # print(table_regex)
                                # input_log.append(line)
                                    # column_size_lst.append(int(find_column_size[0]))
                else:

                    print(filename,begin_loading_count)
                    error_log.append("Error While Parsing fload data")
        if not error_log:
            if filename in outputFile:
                with open(OutputFolderPath+filename, 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open(OutputFolderPath+filename, "r",encoding=e) as f:
                    fileLines = f.readlines()
                    file_obj=''
                    comment=0
                    for line in fileLines:
                        cmt=re.findall(r'^[\s]*--',line)
                        if comment == 0 and "/*" in line:
                            if '*/' in line:
                                comment=0
                                pass
                            else:
                                comment=1
                                pass
                        elif comment==1:
                            if "*/" in line:
                                #print(fileLine)
                                comment=0
                                pass
                            else:
                                pass
                        elif cmt:
                            pass
                        else:
                            file_obj=file_obj+line
            # print("EEE",table_regex)
            find_error2_table=re.findall(r''+table_regex,file_obj,flags=re.I|re.DOTALL)
            if find_error2_table:
                output_log.append(find_error2_table[0])
            else:
                error_log.append("CREATE TABLE Statement Not Found For "+err2_table+" Or Cloumn Sequence Or Datatyes Not Correct")


        if len(error_log) == 0 and len(output_log) == 0:
            fload_error2_table_result.append("NA")
            log_result.append(Result_NA())
        elif error_log:
            fload_error2_table_result.append("N")
            log_result.append(Output_Result_No(error_log))
        elif output_log:
            fload_error2_table_result.append("Y")
            log_result.append(Output_Result_Yes(output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        fload_error2_table_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Partition_By_And_Order_By_In_INSERT(log_result,filename,fload_partition_and_order_by_result,Translate_Table_Folder_Path):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partition By And Order By In INSERT Statement"))
    try:
    # if 1== 1:
        error_log=[]
        input_log=[]
        output_log=[]
        cluster_by_columns=[]
        table_regex=''
        table_name_obj='\n'
        table_file_lst=glob.glob(Translate_Table_Folder_Path+'/*',recursive=True)
        for name in table_file_lst:
            table_name_obj+=os.path.basename(name)+'\n'
        # print(table_name_obj)
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r", encoding=e) as f:
                fileLine = f.readlines()
                error_table_name=''
                target_db=''
                table_name=''
                err2_table=''
                insert_obj=''
                column_name_lst=[]
                begin_loading_count=0
                tablefilename=''
                for line in fileLine:
                    # print(line,begin_loading_count,"@@",err2_table,"TT",table_name)
                    # if table_name == '':
                    if begin_loading_count == 0:
                        find_begin_loading=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*BEGIN[\s]+LOADING',line,flags=re.I|re.DOTALL)
                        if find_begin_loading:
                            begin_loading_count = 1
                    elif begin_loading_count == 1:
                        if err2_table == '':
                            find_err2_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*[\w]+\}*\.([\w]+_ERR2)',line,flags=re.I|re.DOTALL)
                            if find_err2_table:
                                err2_table=find_err2_table[0]
                                begin_loading_count=2
                        if table_name == '':
                            find_table=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*\$\{*([\w]+)\}*\.([\w]+)',line,flags=re.I|re.DOTALL)
                            if find_table:
                                target_db=find_table[0][0]
                                table_name=find_table[0][1]

                            # print(table_name,target_db)
                    elif begin_loading_count == 2:
                        find_define=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*DEFINE[\s]*$',line,flags=re.I|re.DOTALL)
                        if find_define:
                            begin_loading_count = 3
                    elif begin_loading_count == 3:
                        find_end=re.findall(r'^[\s]*\-*\-*[\s]*\%*[\s]*FILE[\s]*\=',line,flags=re.I|re.DOTALL)
                        if find_end:
                            begin_loading_count=4
                        else:
                            # print(line)
                            find_column_name=re.findall(r'([\w]+)[\s]*\([\s]*[\w]+[\s]*\([\s]*[\d]+',line,flags=re.I|re.DOTALL)
                            if find_column_name:
                                column_name_lst.append(find_column_name[0])
                                # pass
                                # print(find_column_name[0])
                    elif begin_loading_count == 4:
                        find_insert=re.findall(r'^[\s]*INSERT[\s]+INTO[\s]+\$\{*'+target_db+'\}*\.'+table_name+'[\W]',line,flags=re.I|re.DOTALL)
                        if find_insert:
                            insert_obj+=line
                            begin_loading_count=5
                    elif begin_loading_count == 5:
                        insert_obj+=line
                        find_end=re.findall(r'\;[\s]*$',line)
                        if find_end:
                            break
                if begin_loading_count == 5:
                    find_table_file_name=re.findall('\n([\w]+\.'+table_name+'\.*[\w]*)\n',table_name_obj,flags=re.I|re.DOTALL)
                    if find_table_file_name:
                        for name in table_file_lst:
                            if name.endswith(find_table_file_name[0]):
                                tablefilename=name
                                break

                    if tablefilename != '':
                        if insert_obj != '':
                            find_col_name=re.findall(r'INSERT[\s]+INTO[\s]+\$\{*'+target_db+'\}*\.'+table_name+'[\s]*\((.*?)\)[\s]*VALUES[\s]*\(',insert_obj,flags=re.I|re.DOTALL)
                            if find_col_name:
                                find_col_name=re.sub(r'[\s]+','',find_col_name[0].upper(),flags=re.I|re.DOTALL).split(',')
                                # print(find_col_name)

                            find_col_value=re.findall(r'\)[\s]*VALUES[\s]*\((.*?)\);',insert_obj,flags=re.I|re.DOTALL)
                            if find_col_value:
                                find_col_value=re.sub(r'[\s]+|\:','',find_col_value[0].upper(),flags=re.I|re.DOTALL).split(',')
                        else:
                            error_log.append("INSERT Statement Not Found For "+table_name)
                        if len(find_col_name) == len(find_col_value) and len(find_col_value) != 0 and len(find_col_name) != 0:
                            with open(tablefilename,'r') as tableobj:
                                table=tableobj.read()
                                # print(table)
                                cluster_by_columns=re.findall(r'CLUSTER[\s]+BY[\s]+(.*?);',table,flags=re.I|re.DOTALL)
                                if cluster_by_columns:
                                    input_log.append('CLUSTER BY '+cluster_by_columns[0]+'')
                                    cluster_by_columns=re.sub(r'[\s]+','',cluster_by_columns[0],flags=re.I|re.DOTALL).split(',')
                        else:
                            error_log.append("COLUMN Name And Values Are Not Equal In INSERT Statement for "+table_name)
                    else:
                        error_log.append("DDL Not Found for "+table_name+' TABLE')
                else:
                    error_log.append("Error While Parsing fload data")
        if not error_log:
            if filename in outputFile:
                with open(OutputFolderPath+filename, 'rb') as f:
                    result = chardet.detect(f.read())
                    e=result['encoding']
                with open(OutputFolderPath+filename, "r",encoding=e) as f:
                    fileLines = f.readlines()
                    file_obj=''
                    comment=0
                    for line in fileLines:
                        cmt=re.findall(r'^[\s]*--',line)
                        if comment == 0 and "/*" in line:
                            if '*/' in line:
                                comment=0
                                pass
                            else:
                                comment=1
                                pass
                        elif comment==1:
                            if "*/" in line:
                                #print(fileLine)
                                comment=0
                                pass
                            else:
                                pass
                        elif cmt:
                            pass
                        else:
                            file_obj=file_obj+line
            find_error2_table=re.findall(r'INSERT[\s]+INTO[\s]+\$ERR_DB\.'+err2_table+'.*?;',file_obj,flags=re.I|re.DOTALL)
            if find_error2_table:
                columns_in_partition_by=[]
                if cluster_by_columns:
                    partition_by_col=re.findall(r'PARTITION[\s]+BY(.*?)ORDER[\s]+BY',find_error2_table[0],flags=re.I|re.DOTALL)
                    if partition_by_col:
                        output_log.append("PARTITION BY "+partition_by_col[0]+" Of "+err2_table+" TABLE")
                        partition_by_col=re.sub(r'[\s]+','',partition_by_col[0],flags=re.I|re.DOTALL).split(',')
                        for col in cluster_by_columns:
                            idx=find_col_name.index(col)
                            find_name=find_col_value[idx]
                            if find_name in partition_by_col:
                                columns_in_partition_by.append(find_name)
                            else:
                                error_log.append(find_name+" Column Name Not Found In PARTITION BY Columns of INSERT STATEMENT Of "+err2_table)
                    else:
                        error_log.append("PARTITION BY Not Present In INSERT STATEMENT Of "+err2_table)
                order_by_col=re.findall(r'ORDER[\s]+BY(.*?)\)',find_error2_table[0],flags=re.I|re.DOTALL)
                if order_by_col:
                    output_log.append("ORDER BY "+order_by_col[0]+" Of "+err2_table+" TABLE")
                    order_by_col=re.sub(r'[\s]+ASC[\s]+|[\s]+DESC[\s]+|[\s]+ASC$|[\s]+DESC$','',order_by_col[0],flags=re.I|re.DOTALL)
                    order_by_col=re.sub(r'[\s]+','',order_by_col,flags=re.I|re.DOTALL).split(',')
                    for col in find_col_value:
                        if col not in columns_in_partition_by:
                            if col in order_by_col:
                                pass
                            else:
                                error_log.append(col+" Column Name Not Found In ORDER BY Columns of INSERT STATEMENT Of "+err2_table)
                else:
                    error_log.append("ORDER BY Not Present In INSERT STATEMENT Of "+err2_table)
            else:
                error_log.append("INSERT INTO  Statement Not Found For "+err2_table)

            find_target_table=re.findall(r'INSERT[\s]+INTO[\s]+\$TARGET_DB\.\$TABLE_NM.*?;',file_obj,flags=re.I|re.DOTALL)
            if find_target_table:
                columns_in_partition_by=[]
                if cluster_by_columns:
                    partition_by_col=re.findall(r'PARTITION[\s]+BY(.*?)ORDER[\s]+BY',find_target_table[0],flags=re.I|re.DOTALL)
                    if partition_by_col:
                        output_log.append("PARTITION BY "+partition_by_col[0]+" Of $TABLE_NM TABLE")
                        partition_by_col=re.sub(r'[\s]+','',partition_by_col[0],flags=re.I|re.DOTALL).split(',')
                        for col in cluster_by_columns:
                            idx=find_col_name.index(col)
                            find_name=find_col_value[idx]
                            if find_name in partition_by_col:
                                columns_in_partition_by.append(find_name)
                            else:
                                error_log.append(find_name+" Column Name Not Found In PARTITION BY Columns of INSERT STATEMENT Of TARGET TABLE")
                    else:
                        error_log.append("PARTITION BY Not Present In INSERT STATEMENT Of TARGET TABLE")
                order_by_col=re.findall(r'ORDER[\s]+BY(.*?)\)',find_target_table[0],flags=re.I|re.DOTALL)
                if order_by_col:
                    output_log.append("ORDER BY "+order_by_col[0]+" Of $TABLE_NM TABLE")
                    order_by_col=re.sub(r'[\s]+ASC[\s]+|[\s]+DESC[\s]+|[\s]+ASC$|[\s]+DESC$','',order_by_col[0],flags=re.I|re.DOTALL)
                    order_by_col=re.sub(r'[\s]+','',order_by_col,flags=re.I|re.DOTALL).split(',')
                    for col in find_col_value:
                        if col not in columns_in_partition_by:
                            if col in order_by_col:
                                pass
                            else:
                                error_log.append(col+" Column Name Not Found In ORDER BY Columns of INSERT STATEMENT Of TARGET TABLE")
                else:
                    error_log.append("ORDER BY Not Present In INSERT STATEMENT Of TARGET TABLE")
            else:
                error_log.append("INSERT INTO  Statement Not Found For TARGET TABLE")


        if len(error_log) == 0 and len(output_log) == 0:
            fload_partition_and_order_by_result.append("NA")
            log_result.append(Result_NA())
        elif error_log:
            fload_partition_and_order_by_result.append("N")
            log_result.append(Output_Result_No(error_log))
        elif output_log:
            fload_partition_and_order_by_result.append("Y")
            log_result.append(Result_Yes(input_log,output_log))

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        fload_partition_and_order_by_result.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)


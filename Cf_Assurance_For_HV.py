from gettext import find
from CF_Assurance_Common_Functions import *
from Cf_Assurance_For_TD import *
import os
import re
import sys
import chardet
import pandas as pd


feature_list=[]

#Automation_hive_to_BQ_assurance change --Starts
#Verizon Functions
def check_datatype_for_HV(log_result,filename,Datatype_result_for_HV,check_charles_timestamp_to_datetime):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Datatype Checking"))
    integer_column=[]
    float_column=[]
    char_varchar_datatype=[]
    decimal_column=[]
    date_column=[]
    timestamp_column = []
    boolean_column = []
    binary_column = []
    array_column=[]
    input_datatype=[]
    input_all_datatype=[]
    output_final_decimal_column=[]
    output_all_datatype=[]
    BQ_input_cloumn=[]
    BQ_output_cloumn=[]
    string_datatype=[]
    if filename in inputFile:
        # print("file")
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            for fileLine in fileLines:
                if fileLine.startswith("--"):
                    pass
                else:
                    fileLine = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",fileLine,flags=re.I|re.DOTALL)
                    # fileLine = re.sub(r"array[\s]*\<STRUCT\<(.*?)\>"," ",fileLine,flags=re.I|re.DOTALL)

                    find_integer=re.findall(r"[\w]+[\s]+INT\)*[\s]+|\_[\w]+[\s]+INT\)*[\s]+|\`\_[\w]+\`[\s]+INT\)*[\s]+|\`[\w]+\`[\s]+INT\)*[\s]+|[\w]+[\s]+INT\,|\_[\w]+[\s]+INT\,|\_[\w]+[\s]+INT|\`\_[\w]+\`[\s]+INT\,|\`\_[\w]+\`[\s]+INT|\`[\w]+\`[\s]+INT\,|\`[\w]+\`[\s]+INT\,"
                                            r"|[\w]+[\s]+BIGINT\)*[\s]+|\_[\w]+[\s]+BIGINT\)*[\s]+|\`\_[\w]+\`[\s]+BIGINT\)*[\s]+|\`[\w]+\`[\s]+BIGINT\)*[\s]+|[\w]+[\s]+BIGINT\,|\_[\w]+[\s]+BIGINT\,|\_[\w]+[\s]+BIGINT|\`\_[\w]+\`[\s]+BIGINT\,|\`\_[\w]+\`[\s]+BIGINT|\`[\w]+\`[\s]+BIGINT\,|\`[\w]+\`[\s]+BIGINT\,"
                                            r"|[\w]+[\s]+SMALLINT\)*[\s]+|\_[\w]+[\s]+SMALLINT\)*[\s]+|\`\_[\w]+\`[\s]+SMALLINT\)*[\s]+|\`[\w]+\`[\s]+SMALLINT\)*[\s]+|[\w]+[\s]+SMALLINT\,|\_[\w]+[\s]+SMALLINT\,|\_[\w]+[\s]+SMALLINT|\`\_[\w]+\`[\s]+SMALLINT\,|\`\_[\w]+\`[\s]+SMALLINT|\`[\w]+\`[\s]+SMALLINT\,|\`[\w]+\`[\s]+SMALLINT\,"
                                            r"|[\w]+[\s]+TINYINT\)*[\s]+|\_[\w]+[\s]+TINYINT\)*[\s]+|\`\_[\w]+\`[\s]+TINYINT\)*[\s]+|\`[\w]+\`[\s]+TINYINT\)*[\s]+|[\w]+[\s]+TINYINT\,|\_[\w]+[\s]+TINYINT\,|\_[\w]+[\s]+TINYINT|\`\_[\w]+\`[\s]+TINYINT\,|\`\_[\w]+\`[\s]+TINYINT|\`[\w]+\`[\s]+TINYINT\,|\`[\w]+\`[\s]+TINYINT\,"
                                            r"|[\w]+[\s]*\:INT\)*\>*[\s]*|\_[\w]+[\s]*\:INT\)*[\s]+|\`\_[\w]+\`[\s]*\:INT\)*[\s]+|\`[\w]+\`[\s]*\:INT\)*[\s]+|[\w]+[\s]*\:INT\,|\_[\w]+[\s]*\:INT\,|\_[\w]+[\s]*\:INT|\`\_[\w]+\`[\s]*\:INT\,|\`\_[\w]+\`[\s]*\:INT|\`[\w]+\`[\s]*\:INT\,|\`[\w]+\`[\s]*\:INT\,"
                                            r"|[\w]+[\s]*\:BIGINT\)*\>*[\s]*|\_[\w]+[\s]*\:BIGINT\)*[\s]+|\`\_[\w]+\`[\s]*\:BIGINT\)*[\s]+|\`[\w]+\`[\s]*\:BIGINT\)*[\s]+|[\w]+[\s]*\:BIGINT\,|\_[\w]+[\s]*\:BIGINT\,|\_[\w]+[\s]*\:BIGINT|\`\_[\w]+\`[\s]*\:BIGINT\,|\`\_[\w]+\`[\s]*\:BIGINT|\`[\w]+\`[\s]*\:BIGINT\,|\`[\w]+\`[\s]*\:BIGINT\,"
                                            r"|[\w]+[\s]*\:SMALLINT\)*\>*[\s]*|\_[\w]+[\s]*\:SMALLINT\)*[\s]+|\`\_[\w]+\`[\s]*\:SMALLINT\)*[\s]+|\`[\w]+\`[\s]*\:SMALLINT\)*[\s]+|[\w]+[\s]*\:SMALLINT\,|\_[\w]+[\s]*\:SMALLINT\,|\_[\w]+[\s]*\:SMALLINT|\`\_[\w]+\`[\s]*\:SMALLINT\,|\`\_[\w]+\`[\s]*\:SMALLINT|\`[\w]+\`[\s]*\:SMALLINT\,|\`[\w]+\`[\s]*\:SMALLINT\,"
                                            r"|[\w]+[\s]*\:TINYINT\)*\>*[\s]*|\_[\w]+[\s]*\:TINYINT\)*[\s]+|\`\_[\w]+\`[\s]*\:TINYINT\)*[\s]+|\`[\w]+\`[\s]*\:TINYINT\)*[\s]+|[\w]+[\s]*\:TINYINT\,|\_[\w]+[\s]*\:TINYINT\,|\_[\w]+[\s]*\:TINYINT|\`\_[\w]+\`[\s]*\:TINYINT\,|\`\_[\w]+\`[\s]*\:TINYINT|\`[\w]+\`[\s]*\:TINYINT\,|\`[\w]+\`[\s]*\:TINYINT\,",fileLine,flags=re.I)

                    for ele in find_integer:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        integer_column.append(ele.upper())

                    find_float=re.findall(r"[\w]+[\s]+FLOAT\)*[\s]+|\_[\w]+[\s]+FLOAT\)*[\s]+|\`\_[\w]+\`[\s]+FLOAT\)*[\s]+|\`[\w]+\`[\s]+FLOAT\)*[\s]+|[\w]+[\s]+FLOAT\,|\_[\w]+[\s]+FLOAT\,|\_[\w]+[\s]+FLOAT|\`\_[\w]+\`[\s]+FLOAT\,|\`\_[\w]+\`[\s]+FLOAT|\`[\w]+\`[\s]+FLOAT\,|\`[\w]+\`[\s]+FLOAT\,"
                                          r"|[\w]+[\s]+DOUBLE\)*[\s]+|\_[\w]+[\s]+DOUBLE\)*[\s]+|\`\_[\w]+\`[\s]+DOUBLE\)*[\s]+|\`[\w]+\`[\s]+DOUBLE\)*[\s]+|[\w]+[\s]+DOUBLE\,|\_[\w]+[\s]+DOUBLE\,|\_[\w]+[\s]+DOUBLE|\`\_[\w]+\`[\s]+DOUBLE\,|\`\_[\w]+\`[\s]+DOUBLE|\`[\w]+\`[\s]+DOUBLE\,|\`[\w]+\`[\s]+DOUBLE\,",fileLine,flags=re.I)
                    for ele in find_float:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        ele=re.sub(r"\,","",ele,flags=re.I)
                        float_column.append(ele.upper())

                    find_char=re.findall(r"[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\_[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\`\_[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\`[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\_[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`\_[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)"
                                         r"|[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\_[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\`\_[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\`[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\_[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`\_[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)",fileLine,flags=re.I)
                    for item in find_char:
                        item=re.sub(r"\)\)",")",item,flags=re.I)
                        char_varchar_datatype.append(item.upper())

                    find_decimal=re.findall(r"[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\_[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\`\_[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)"
                                            r"|[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\_[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\`\_[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,"
                                            r"|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)",fileLine,flags=re.I)
                    for ele in find_decimal:
                        item=re.sub(r"\)\)",")",ele,flags=re.I)
                        decimal_column.append(item.upper())

                    find_STRING =re.findall(r"[\w]+[\s]+STRING\)*[\s]+|\_[\w]+[\s]+STRING\)*[\s]+|\`\_[\w]+\`[\s]+STRING\)*[\s]+|\`[\w]+\`[\s]+STRING\)*[\s]+|[\w]+[\s]+STRING\,|\_[\w]+[\s]+STRING\,|\_[\w]+[\s]+STRING|\`\_[\w]+\`[\s]+STRING\,|\`\_[\w]+\`[\s]+STRING|\`[\w]+\`[\s]+STRING\,|\`[\w]+\`[\s]+STRING\,"
                                            r"|[\w]+[\s]*\:STRING\)*[\s]*\>*[\s]*|\_[\w]+[\s]*\:STRING\)*[\s]*|\`[\w]+\`[\s]*\:STRING\)*[\s]*|[\w]+[\s]*\:STRING\,|\_[\w]+[\s]*\:STRING\,|\_[\w]+[\s]*\:STRING|\`\_[\w]+\`[\s]*\:STRING\,|\`\_[\w]+\`[\s]*\:STRING|\`[\w]+\`[\s]*\:STRING\,|\`[\w]+\`[\s]*\:STRING\,"
                                            r"|[\w]+[\s]+void\)*[\s]+|\_[\w]+[\s]+void\)*[\s]+|\`\_[\w]+\`[\s]+void\)*[\s]+|\`[\w]+\`[\s]+void\)*[\s]+|[\w]+[\s]+void\,|\_[\w]+[\s]+void\,|\_[\w]+[\s]+void|\`\_[\w]+\`[\s]+void\,|\`\_[\w]+\`[\s]+void|\`[\w]+\`[\s]+void\,|\`[\w]+\`[\s]+void\,",fileLine,flags=re.I)

                    for ele in find_STRING:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        ele=re.sub(r"\,","",ele,flags=re.I)
                        string_datatype.append(ele.upper())

                    find_Date=re.findall(r"[\w]+[\s]+DATE\)*[\s]+|\_[\w]+[\s]+DATE\)*[\s]+|\`\_[\w]+\`[\s]+DATE\)*[\s]+|\`[\w]+\`[\s]+DATE\)*[\s]+|[\w]+[\s]+DATE\,|\_[\w]+[\s]+DATE\,|\_[\w]+[\s]+DATE|\`\_[\w]+\`[\s]+DATE\,|\`\_[\w]+\`[\s]+DATE|\`[\w]+\`[\s]+DATE\,|\`[\w]+\`[\s]+DATE\,",fileLine,flags=re.I)
                    for ele in find_Date:
                        ele=re.sub(r"\)","",ele,flags=re.I)
                        date_column.append(ele.upper())

                    find_timestamp=re.findall(r"[\w]+[\s]+TIMESTAMP\)*[\s]+|\_[\w]+[\s]+TIMESTAMP\)*[\s]+|\`\_[\w]+\`[\s]+TIMESTAMP\)*[\s]+|\`[\w]+\`[\s]+TIMESTAMP\)*[\s]+|[\w]+[\s]+TIMESTAMP\,|\_[\w]+[\s]+TIMESTAMP\,|\_[\w]+[\s]+TIMESTAMP|\`\_[\w]+\`[\s]+TIMESTAMP\,|\`\_[\w]+\`[\s]+TIMESTAMP|\`[\w]+\`[\s]+TIMESTAMP\,|\`[\w]+\`[\s]+TIMESTAMP\,",fileLine,flags=re.I)
                    for ele in find_timestamp:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        timestamp_column.append(ele.upper())

                    find_boolean=re.findall(r"[\w]+[\s]+BOOLEAN\)*[\s]+|\_[\w]+[\s]+BOOLEAN\)*[\s]+|\`\_[\w]+\`[\s]+BOOLEAN\)*[\s]+|\`[\w]+\`[\s]+BOOLEAN\)*[\s]+|[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN|\`\_[\w]+\`[\s]+BOOLEAN\,|\`\_[\w]+\`[\s]+BOOLEAN|\`[\w]+\`[\s]+BOOLEAN\,|\`[\w]+\`[\s]+BOOLEAN\,",fileLine,flags=re.I)
                    for ele in find_boolean:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        boolean_column.append(ele.upper())

                    find_binary=re.findall(r"[\w]+[\s]+BINARY\)*[\s]+|\_[\w]+[\s]+BINARY\)*[\s]+|\`\_[\w]+\`[\s]+BINARY\)*[\s]+|\`[\w]+\`[\s]+BINARY\)*[\s]+|[\w]+[\s]+BINARY\,|\_[\w]+[\s]+BINARY\,|\_[\w]+[\s]+BINARY|\`\_[\w]+\`[\s]+BINARY\,|\`\_[\w]+\`[\s]+BINARY|\`[\w]+\`[\s]+BINARY\,|\`[\w]+\`[\s]+BINARY\,",fileLine,flags=re.I)
                    for ele in find_binary:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        binary_column.append(ele.upper())

                    find_array=re.findall(r"[\w]+[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|[\w]+[\s]+ARRAY[\s]*\<STRING\>\,|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>\,|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,",fileLine,flags=re.I)
                    for ele in find_array:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        array_column.append(ele.upper())

            for ele in integer_column:
                replce_integer=re.sub(r"[\s]+INT[\s]*|[\s]+BIGINT[\s]*|[\s]+SMALLINT[\s]*|[\s]+TINYINT[\s]*|"
                                      r"\:[\s]*INT[\s]*|\:[\s]*BIGINT[\s]*|\:[\s]*SMALLINT[\s]*|\:[\s]*TINYINT[\s]*","INT64",ele,flags=re.I)
                replce_integer=re.sub(r"[\s]+|\,|\>\>*","",replce_integer,flags=re.I)
                # print(replce_integer)
                replce_integer=re.sub(r"\`","",replce_integer,flags=re.I)


                if replce_integer.lower()=="andint64" or replce_integer.lower()=="asint64" or replce_integer=="":
                    pass
                else:
                    input_all_datatype.append(replce_integer.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in float_column:
                replce_float=re.sub(r"[\s]+FLOAT|[\s]+DOUBLE","FLOAT64",ele,flags=re.I)
                replce_float=re.sub(r"[\s]+|\,","",replce_float,flags=re.I)
                replce_float=re.sub(r"\`","",replce_float,flags=re.I)

                if replce_float.lower()=="andfloat64" or replce_float.lower()=="asfloat64" or replce_float=="":
                    pass
                else:
                    input_all_datatype.append(replce_float.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in char_varchar_datatype:

                rplce_char_to_string=re.sub(r"[\s]+CHAR|[\s]+VARCHAR"," STRING",ele,flags=re.I)
                rplce_char_to_string=re.sub(r"[\s]+","",rplce_char_to_string,flags=re.I)
                rplce_char_to_string=re.sub(r"\,","",rplce_char_to_string,flags=re.I)
                rplce_char_to_string=re.sub(r"\`","",rplce_char_to_string,flags=re.I)

                if rplce_char_to_string.lower() == "asstring" or rplce_char_to_string.lower() == "andstring":
                    pass
                else:
                    BQ_input_cloumn.append(ele.lower())
                    input_all_datatype.append(rplce_char_to_string.lower())

            for ele in string_datatype:
                string_to_STRING=re.sub(r"[\s]+string,","STRING",ele,flags=re.I)
                string_to_STRING=re.sub(r"[\s]+void","STRING",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"\:string\,*","STRING",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"[\s]+|\,|\>\>*","",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"[\s]+","",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"\`","",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r",","",string_to_STRING,flags=re.I)

                if string_to_STRING.lower() == "asstring" or string_to_STRING.lower() == "andstring" or "ofstring" in string_to_STRING.lower():
                    pass
                elif string_to_STRING.lower() == "asvoid" or string_to_STRING.lower() == "andvoid" or "ofvoid" in string_to_STRING.lower():
                    pass
                else:
                    BQ_input_cloumn.append(ele.lower())
                    input_all_datatype.append(string_to_STRING.lower())

            for ele in decimal_column:
                if "as decimal" in ele.lower() or "as dec" in ele.lower() or "as numeric" in ele.lower():
                    pass
                else:

                    find_column_value=re.findall(r"[\d]+\,[\d]+",ele.upper())

                    if find_column_value:
                        find_column_value="".join(find_column_value)
                        find_column_value=find_column_value.split(",")
                        if int(find_column_value[0])>=38 or int(find_column_value[1])>=9:
                            replce_decimal=re.sub("[\s]+DECIMAL"," BIGDECIMAL",ele,flags=re.I)
                            replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)
                            BQ_input_cloumn.append(ele.lower())
                            input_all_datatype.append(replce_decimal.lower())
                        else:
                            replce_decimal=re.sub("[\s]+DECIMAL"," DECIMAL",ele,flags=re.I)
                            replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)
                            BQ_input_cloumn.append(ele.lower())
                            input_all_datatype.append(replce_decimal.lower())
                    else:

                        replce_decimal=re.sub("[\s]+DECIMAL"," DECIMAL",ele,flags=re.I)
                        replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                        replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                        replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)
                        BQ_input_cloumn.append(ele.lower())
                        input_all_datatype.append(replce_decimal.lower())

            for ele in date_column:
                replce_date=re.sub(r"[\s]+DATE","DATE",ele,flags=re.I)
                replce_date=re.sub(r"[\s]+","",replce_date,flags=re.I)
                replce_date=re.sub(r"\`","",replce_date,flags=re.I)
                replce_date=re.sub(r"\,","",replce_date,flags=re.I)

                if replce_date.lower()=="anddate" or replce_date.lower()=="asdate" or replce_date==""  or "elsedate" in replce_date.lower():
                    pass
                else:
                    input_all_datatype.append(replce_date.lower())
                    BQ_input_cloumn.append(ele.lower())

            if(check_charles_timestamp_to_datetime=='N' or check_charles_timestamp_to_datetime==''):
                for ele in timestamp_column:
                    replce_timestamp=re.sub(r"[\s]+TIMESTAMP","TIMESTAMP",ele,flags=re.I)
                    replce_timestamp=re.sub(r"[\s]+","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\`","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\,","",replce_timestamp,flags=re.I)

                    if replce_timestamp.lower()=="andtimestamp" or replce_timestamp.lower()=="astimestamp" or replce_timestamp=="":
                        pass
                    else:
                        input_all_datatype.append(replce_timestamp.lower())
                        BQ_input_cloumn.append(ele.lower())
            else:
                for ele in timestamp_column:
                    replce_timestamp=re.sub(r"[\s]+TIMESTAMP","DATETIME",ele,flags=re.I)
                    replce_timestamp=re.sub(r"[\s]+","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\`","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\,","",replce_timestamp,flags=re.I)

                    if replce_timestamp.lower()=="anddatetime" or replce_timestamp.lower()=="asdatetime" or replce_timestamp=="":
                        pass
                    else:
                        input_all_datatype.append(replce_timestamp.lower())
                        BQ_input_cloumn.append(ele.lower())

            for ele in boolean_column:
                replce_boolean=re.sub(r"[\s]+BOOLEAN","BOOL",ele,flags=re.I)
                replce_boolean=re.sub(r"[\s]+","",replce_boolean,flags=re.I)
                replce_boolean=re.sub(r"\`","",replce_boolean,flags=re.I)
                replce_boolean=re.sub(r"\,","",replce_boolean,flags=re.I)

                if replce_boolean.lower()=="andboolean" or replce_boolean.lower()=="asboolean" or replce_boolean=="":
                    pass
                else:
                    input_all_datatype.append(replce_boolean.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in binary_column:
                replce_binary=re.sub(r"[\s]+BINARY","BYTES",ele,flags=re.I)
                replce_binary=re.sub(r"[\s]+","",replce_binary,flags=re.I)
                replce_binary=re.sub(r"\`","",replce_binary,flags=re.I)
                replce_binary=re.sub(r"\,","",replce_binary,flags=re.I)
                if replce_binary.lower()=="andbinary" or replce_binary.lower()=="asbinary" or replce_binary=="":
                    pass
                else:
                    input_all_datatype.append(replce_binary.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in array_column:
                replce_array=re.sub(r"[\s]+STRING","STRING",ele,flags=re.I)
                replce_array=re.sub(r"[\s]+","",replce_array,flags=re.I)
                replce_array=re.sub(r"\`","",replce_array,flags=re.I)
                replce_array=re.sub(r"\,","",replce_array,flags=re.I)
                # print("replce_binary:  ", replce_binary)
                if replce_array.lower()=="andarray<string>" or replce_array.lower()=="asarray<string>" or replce_array=="":
                    pass
                else:
                    input_all_datatype.append(replce_array.lower())
                    BQ_input_cloumn.append(ele.lower())

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLines = f.readlines()
            for fileLine in fileLines:
                fileLine =re.sub(r"(OPTIONS\(DESCRIPTION\s*\=)*\s*\'(.*?)\'\)","",fileLine,flags=re.DOTALL|re.I)
                # fileLine = re.sub(r"array\<(.*?)\>","",fileLine,flags=re.I|re.DOTALL)
                fileLine = re.sub(r"struct\<(.*?)\>\>|struct\<(.*?)\>","",fileLine,flags=re.I|re.DOTALL)

                find_integer1=re.findall(r"[\w]+[\s]+INT64[\s]*|[\w]+[\s]+INT64[\s]*\)",fileLine,flags=re.I)
                for item in find_integer1:
                    item = re.sub(r"\)",",",item,flags=re.I)
                    item = re.sub(r"\'","",item,flags=re.I)
                    BQ_output_cloumn.append(item)
                    remove_space=re.sub(r"[\s]+|\,","",item,flags=re.I)
                    if remove_space.lower() == "asint64" or remove_space.lower() == "andint64" or remove_space.lower() == "byint64" or "ofint64" in remove_space.lower():
                        pass
                    else:
                        output_all_datatype.append(remove_space.lower())

                find_float=re.findall(r"[\w]+[\s]+FLOAT64[\s]*|[\w]+[\s]+FLOAT64[\s]*\)",fileLine,flags=re.I)
                for item in find_float:
                    item = re.sub(r"\)",",",item,flags=re.I)
                    item = re.sub(r"\'","",item,flags=re.I)
                    BQ_output_cloumn.append(item)
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    output_all_datatype.append(remove_space.lower())

                find_char=re.findall(r"[\w]+[\s]+STRING\([\d]+\)|[\w]+[\s]+string[\s]*|_[\w]+[\s]+string[\s]*|\`_[\w]+\`[\s]+STRING[\s]*|[\w]+[\s]+STRING|[\w]+[\s]+STRING\([\d]+\)\)|[\w]+[\s]+STRING[\s]+|[\w]+[\s]+STRING\)",fileLine,flags=re.I)
                # print("find_char:",find_char)
                for item in find_char:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"string\s*\)","string",remove_space,flags=re.I)
                    remove_space=re.sub(r"\)\)",")",remove_space,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)
                    if remove_space.lower() == "asstring" or remove_space.lower() == "andstring" or remove_space.lower() == "bystring" or "ofstring" in remove_space.lower():
                        pass
                    else:
                        item = re.sub(r"\)\)",")",item,flags=re.I)
                        item = re.sub(r"string\s*\)","string,",item,flags=re.I)
                        item = re.sub(r"\'","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())

                find_decimal=re.findall(r"[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)"
                                        r"|[\w]+[\s]+BIGDECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+BIGDECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)",fileLine,flags=re.I)

                for item in find_decimal:
                    remove_space=re.sub(r"\)\)",")",item,flags=re.I)
                    remove_space=re.sub(r"[\s]","",remove_space,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)

                    if "asdecimal" in remove_space.lower() or "bybigdecimal" in remove_space.lower() or "asbigdecimal" in remove_space.lower():
                        pass
                    else:
                        item = re.sub(r"\)\)",")",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())

                find_date=re.findall(r"[\w]+[\s]+DATE[\s]*\,|[\w]+[\s]+DATE[\s]*\)|[\w]+[\s]+DATE[\s]*",fileLine,flags=re.I)
                for item in find_date:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)
                    remove_space = re.sub(r"\,|\)","",remove_space,flags=re.I)

                    if remove_space.lower() == "asdate" or remove_space.lower()=="anddate" or remove_space.lower()=="bydate" or "elsedate" in remove_space.lower() :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item = re.sub(r"\'","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())

                if(check_charles_timestamp_to_datetime=='N' or check_charles_timestamp_to_datetime==''):
                    find_timestamp1=re.findall(r"[\w]+[\s]+TIMESTAMP[\s]*|[\w]+[\s]+TIMESTAMP[\s]*\)",fileLine,flags=re.I)
                    for item in find_timestamp1:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                        if remove_space.lower() == "astimestamp" or remove_space.lower()=="bytimestamp" or remove_space.lower()=="andtimestamp" :
                            pass
                        else:
                            item = re.sub(r"\)",",",item,flags=re.I)
                            item=re.sub(r"\`","",item,flags=re.I)
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())
                else:
                    find_datetime=re.findall(r"[\w]+[\s]+DATETIME[\s]*|[\w]+[\s]+DATETIME[\s]*\)",fileLine,flags=re.I)
                    for item in find_datetime:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                        if remove_space.lower() == "asdatetime" or remove_space.lower()=="bydatetime" or remove_space.lower()=="anddatetime" :
                            pass
                        else:
                            item = re.sub(r"\)",",",item,flags=re.I)
                            item=re.sub(r"\`","",item,flags=re.I)
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

                find_boolean1=re.findall(r"[\w]+[\s]+BOOL[\s]*|[\w]+[\s]+BOOL[\s]*\)",fileLine,flags=re.I)
                for item in find_boolean1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    if remove_space.lower() == "asbool" or remove_space.lower()=="bybool" or remove_space.lower()=="andbool" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())

                find_binary1=re.findall(r"[\w]+[\s]+BYTES[\s]*|[\w]+[\s]+BYTES[\s]*\)",fileLine,flags=re.I)
                for item in find_binary1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    if remove_space.lower() == "asbytes" or remove_space.lower()=="andbytes" or remove_space.lower()=="bybytes" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())


                find_array1=re.findall(r"[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>\([\d]+\)|[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>[\s]*|_[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>[\s]*|\`_[\w]+\`[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>[\s]*|[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>|\`[\w]+\`[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>\)|[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>[\s]+|[\w]+[\s]+ARRAY[\s]*\<[\s]*STRING[\s]*\>\)",fileLine,flags=re.I)

                for item in find_array1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    if remove_space.lower() == "asarray<string>" or remove_space.lower()=="andarray<string>" or remove_space.lower()=="byarray<string>" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())
                        #print("output_all_datatype :" , output_all_datatype)

            input_all_datatype.sort()
            output_all_datatype.sort()
            BQ_input_cloumn.sort()
            BQ_output_cloumn.sort()
            if BQ_input_cloumn:
                val1 =  BQ_input_cloumn[-1]
                BQ_input_cloumn.pop()
                newval1 = val1.replace(",","")
                BQ_input_cloumn.append(newval1)
            if BQ_output_cloumn:
                val2 = BQ_output_cloumn[-1]
                BQ_output_cloumn.pop()
                newval2 = val2.replace(",","")
                BQ_output_cloumn.append(newval2)

    # for i in input_all_datatype:

    #     if i not in output_all_datatype:
    #         # print("input File:", i+" wrong column Name")
    #         BQ_input_cloumn.append(i+" Error:wrong column Name")
    #     else:
    #         pass
    # for i in output_all_datatype:

    #     if i not in input_all_datatype:
    #         # print("output File:", i+" wrong columnName")
    #         BQ_output_cloumn.append(i+" Error:wrong column Name")
    #         # print("BQ_output_cloumn",BQ_output_cloumn)
    #     else:
    #         pass

    # print("input_all_datatype =",input_all_datatype)
    # print("output_all_datatype=",output_all_datatype)
    # print("BQ_input_cloumn",BQ_input_cloumn)
    # print("BQ_output_cloumn",BQ_output_cloumn)
    # print(len(input_all_datatype))
    # print(len(output_all_datatype))

    if len(input_all_datatype)==0 and len(output_all_datatype) == 0:
        Datatype_result_for_HV.append("NA")
        log_result.append(Result_NA())
    elif input_all_datatype==output_all_datatype:
        # print("Check for datatype has been done successful")
        Datatype_result_for_HV.append("Y")
        log_result.append(Result_Yes(BQ_input_cloumn,BQ_output_cloumn))
    else:
        Datatype_result_for_HV.append("N")
        log_result.append(Result_No(BQ_input_cloumn,BQ_output_cloumn))

def check_comment_to_option_for_HV(log_result,filename,comment_to_option_result_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Comment to option conversion check"))
    output_file_actual_option=[]
    Hive_comment_list_final=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            Hive_comment_list_raw=re.findall(r"(CO.{3}NT)+\s*\'(.*?)\'",query,flags=re.I)
            if Hive_comment_list_raw:
                for k in Hive_comment_list_raw:
                    a, b = k
                    b = "'"+b+"'"
                    c = a, b
                    listq = ' '.join(c)
                    Hive_comment_list_final.append(listq)
            Hive_comment_list_final.sort()
            input_file_comment_to_option= ["OPTIONS(DESCRIPTION =" + "'"  + i[1] + "'"+")"  for i in Hive_comment_list_raw]

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            if 'TABLE' in fileLine.upper():
                find_option=re.findall(r"(OPTIONS\(DESCRIPTION\s*\=)*\s*\'(.*?)\'\)", fileLine, flags=re.I)
                # print(find_option)
                if find_option:
                    for z in find_option:
                        x, y = z
                        y = "'"+y+"'"+")"
                        d = x, y
                        listq = ''.join(d)

                        output_file_actual_option.append(listq)

            Hive_comment_list_final.sort()
            input_file_comment_to_option.sort()
            output_file_actual_option.sort()

    # print("input_file_comment_to_option",input_file_comment_to_option)
    # print("output_file_actual_option",output_file_actual_option)

    if len(input_file_comment_to_option)==0:
        comment_to_option_result_for_HV.append("NA")
        log_result.append(Result_NA())

    elif len(input_file_comment_to_option)==len(output_file_actual_option):
        # print("COMMENT FILES SUCCESSFUL")
        comment_to_option_result_for_HV.append("Y")
        log_result.append(Result_Yes(Hive_comment_list_final,output_file_actual_option))
    else:
        comment_to_option_result_for_HV.append("N")
        # print("COMMENT FILES unSUCCESSFUL")
        log_result.append(Result_No(Hive_comment_list_final,output_file_actual_option))

def check_drop_raw_format_or_location_for_HV(log_result,filename,drop_row_format_or_location_result_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Drop_raw_format_location_from_hv"))
    drop_row_format = []
    find_option = []
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            drop_row_format=re.findall(r"(ROW[\s]+FORMAT[\s]+[\w]+)[\s]*\'*(.*?).\;",query,flags=re.I|re.DOTALL)
            if not drop_row_format:
                drop_row_format=re.findall(r"(LOCATION)+[\s]*\'(.*?).\;",query,flags=re.DOTALL)

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            find_option=re.findall(r"(ROW[\s]+FORMAT[\s]+[\w]+)[\s]*\'(.*?).\;",fileLine,flags=re.I|re.DOTALL)
            if not find_option:
                find_option=re.findall(r"(LOCATION)+[\s]*\'(.*?).\;",fileLine,flags=re.DOTALL)

    if len(drop_row_format)== 0 and not find_option:
        drop_row_format_or_location_result_for_HV.append("NA")
        log_result.append(Result_NA())
    elif drop_row_format and not find_option:
        # print("Comment statement check in hive is through")
        drop_row_format_or_location_result_for_HV.append("Y")
        log_result.append(Result_Yes(list(drop_row_format[0]),find_option))
    else:
        drop_row_format_or_location_result_for_HV.append("N")
        log_result.append(Result_No(drop_row_format[0],find_option[0]))

def check_create_table_external_for_HV(log_result,filename,create_table_external_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Create Table To External Table Checking"))
    input_create_table_column=[]
    bq_equivalent_conversion=[]
    Bq_output_file_create_table=[]

    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            f.close()
            for fileLine in fileLines:
                if fileLine.startswith("--"):
                    pass
                else:
                    find_create_table=re.findall(r"CREATE[\s]+EXTERNAL[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|CREATE[\s]+EXTERNAL[\s]+TABLE|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|CREATE[\s]+TABLE[\s]+",fileLine,flags=re.I)

                    for ele in find_create_table:
                        input_create_table_column.append(ele.upper())

            for ele in input_create_table_column:
                replce_table=re.sub(r"CREATE[\s]+EXTERNAL[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|CREATE[\s]+EXTERNAL[\s]+TABLE|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS","CREATE TABLE IF NOT EXISTS",ele,flags=re.I)
                bq_equivalent_conversion.append(replce_table)

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            Bq_create_table=re.findall(r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS",fileLine,flags=re.I)
            Bq_output_file_create_table = Bq_create_table

    input_create_table_column.sort()
    Bq_output_file_create_table.sort()
    bq_equivalent_conversion.sort()

    # print("Bq_output_file_create_table", Bq_output_file_create_table)
    # print("bq_equivalent_conversion", bq_equivalent_conversion)

    if len(bq_equivalent_conversion)==0 and len(Bq_output_file_create_table) == 0:
        create_table_external_for_HV.append("NA")
        log_result.append(Result_NA())

    elif len(bq_equivalent_conversion)==len(Bq_output_file_create_table):
        # print("check for create statement check has been done")
        create_table_external_for_HV.append("Y")
        log_result.append(Result_Yes(input_create_table_column,Bq_output_file_create_table))
    else:
        create_table_external_for_HV.append("N")
        log_result.append(Result_No(input_create_table_column,Bq_output_file_create_table))

def No_Of_Statement_In_Script_Hive(log_result,filename,Result_No_Of_Statement,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Number of Statements in hive-BQ Script"))
    with_Append_project_id_Final_input_sql_statements=[]
    without_Append_project_id_Final_input_sql_statements=[]
    with_Append_project_id_Final_output_sql_statements=[]
    without_Append_project_id_Final_output_sql_statements=[]
    all_input_file_statements=[]
    all_output_file_statements=[]

    # print("inside check for statement")
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
                        #print(fileLine)
                        comment=0
                        pass
                    else:
                        pass
                elif fileLine.startswith("--") or "--" in fileLine or ".if activitycount" in fileLine.lower():
                    pass
                else:
                    if counter==0:
                        obj=''
                        # print("file line",fileLine )
                        #Search pattern in all input file for no of statements
                        input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE|CREATE[\s]+EXTERNAL[\s]+TABLE|WITH[\s]+CTE[\s]+|CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+TABLE[\s]*IF[\s]*EXISTS[\s]*|MERGE[\s]+",fileLine,flags=re.I)
                        # print("input statement for ssql 1:",input_sql_statements)
                        if input_sql_statements:
                            # print("inside input sql statements")
                            if input_sql_statements[0].upper() in fileLine.upper():
                                counter=1
                                obj=obj+fileLine
                                if "--" in fileLine and ";" in fileLine and "';'" not in fileLine:
                                    x=fileLine.split("--")
                                    if ";" in x[0]:
                                        #print(obj)
                                        counter=0
                                elif ";" in fileLine and "';'" not in fileLine:
                                    # print("qr",obj)
                                    counter=0
                                    input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]\`+|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+\.[\w]|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*[\w]+\.[\w]+"
                                                                    r"|^WITH[\s]+CTE[\s]+[\w]+[\s]+|CREATE[\s]+TEMPORARY[\s]+"
                                                                    r"|TABLE[\s]*[\w]+[\s]*|CREATE[\s]+VIEW[\s]*\`[\w]+\.[\w]+\`+"
                                                                    r"|REPLACE[\s]+VIEW[\s]*\`[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]+\`[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]*INTO[\s]*[\w]+\.[\w]+"
                                                                    r"|UPDATE[\s]*\`[\w]+\.[\w]+\`+|UPDATE[\s]+[\w]+[\s]+"
                                                                    r"|DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;|DROP[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+[\s]*"
                                                                    r"|MERGE[\s]+\`[\w]+\.[\w]+\`+|CREATE[\s]+SET[\s]+TABLE[\s]+\`[\w]+\.[\w]+\`+"
                                                                    ,obj,flags=re.I)
                                    # print("input_sql_statements111:-", input_sql_statements)
                                    # print("check for input statement no 1")
                                    # If project ID is present enter into this block
                                    if fun_project_ID == "Y":
                                        # print("hehe hehehe ")
                                        obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                        if obj.upper().startswith("SELECT "):
                                            find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                            if find_select:
                                                find_select=re.sub(r"[\s]+","",find_select[0])
                                                with_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                            all_input_file_statements.append(obj.upper())
                                        for item in input_sql_statements:
                                            # print("To check here")
                                            # print("inside_inputsql:", item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            all_input_file_statements.append(remove_space.upper())
                                            item=re.sub(r"\n","",item)
                                            item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"drop[\s]+table[\s]+if[\s]+exists","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                                            # print("inside_inputsqldrop:", item)
                                            if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                                            elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                                            if re.findall(r"INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                                item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                            else:
                                                item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                            if re.findall(r"UPDATE[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                                item=re.sub(r"UPDATE","UPDATE `"+Append_project_id+".",item,flags=re.I)
                                            else:
                                                item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                            item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"[\s]+|\;","",item)
                                            item=re.sub(r"\.[\s]+",".",item)
                                            with_Append_project_id_Final_input_sql_statements.append(item.upper())
                                    else:

                                        obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I)
                                        # print("I am here hehehe ")
                                        if obj.upper().startswith("SELECT "):
                                            find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I)
                                            #print(find_select)
                                            if find_select:
                                                find_select=re.sub(r"[\s]+","",find_select[0])
                                                without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                            all_input_file_statements.append(obj.upper())
                                        for item in input_sql_statements:
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            all_input_file_statements.append(remove_space.upper())
                                            item=re.sub(r"\n","",item)
                                            item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                            item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                                            item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS ",item,flags=re.I)
                                            if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                                            elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                                            item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                            item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                                            item=re.sub(r"[\s]+|\;","",item)
                                            item=re.sub(r"\.[\s]+",".",item)
                                            without_Append_project_id_Final_input_sql_statements.append(item.upper())
                    else:
                        if counter==1:
                            if "--" in fileLine and ";" in fileLine and "';'" not in fileLine:
                                x=fileLine.split("--")
                                if ";" in x[0]:
                                    counter=0
                                    obj=obj+fileLine
                                else:
                                    obj=obj+fileLine
                            elif ";" in fileLine and "';'" not in fileLine:
                                counter=0
                                obj=obj+fileLine
                                # print("I am here part 2 ")
                                # print("oobbjj :", obj)
                                input_sql_statements=re.findall(r"INSERT[\s]+INTO[\s]+\`[\w]+\`\.\`[\w]+\`[\s]*|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`[\s]*|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*[\w]+\.[\w]+[\s]*|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+\.[\w]+|DROP[\s]*TABLE[\s]*IF[\s]*EXISTS[\s]*[\w]+\.[\w]+",obj,flags=re.I)
                                # print("input_sql_statements::", input_sql_statements)                                
                                if fun_project_ID == "Y":
                                    obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            # find_select.append(find_select.upper())
                                        all_input_file_statements.append(obj.upper())
                                    for item in input_sql_statements:
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        all_input_file_statements.append(remove_space.upper())
                                        item=re.sub(r"\n","",item)
                                        item=re.sub(r"\`" ,"",item)
                                        # print("item item", item)
                                        item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                                        # print("itemmm",item)
                                        item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                                        if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                                        elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)

                                        if re.findall(r"INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        elif re.findall(r"INSERT[\s]+INTO[\s]*[\w]+\.[\w]+",item,flags=re.I):
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        else:
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                        if re.findall(r"UPDATE[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                            item=re.sub(r"UPDATE","UPDATE `"+Append_project_id+".",item,flags=re.I)
                                        else:
                                            item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                        #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"[\s]+|\;","",item)
                                        item=re.sub(r"\.[\s]+",".",item)
                                        with_Append_project_id_Final_input_sql_statements.append(item.upper())
                                else:
                                    obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                    #print(obj)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                        all_input_file_statements.append(obj.upper())
                                    #without project ID enter into this block
                                    for item in input_sql_statements:
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        all_input_file_statements.append(remove_space.upper())
                                        item=re.sub(r"\n","",item)
                                        # print("cre_item11:-", item)
                                        item=re.sub(r"CREATE[\s]*EXTERNAL[\s]*TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]*EXTERNAL[\s]*TABLE[\s]*","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                                        # print("cre_item22:-", item)
                                        item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                        item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                                        item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS ",item,flags=re.I)
                                        if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                                        elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                                        item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                        #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                                        item=re.sub(r"[\s]+|\;","",item)
                                        item=re.sub(r"\.[\s]+",".",item)
                                        item=re.sub(r"\`","",item)
                                        without_Append_project_id_Final_input_sql_statements.append(item.upper())

                            else:
                                obj=obj+fileLine

    # print("inside check for statement b4 output")


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
                if fileLine.startswith("/*"):
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
                elif fileLine.startswith("--"):
                    pass
                else:
                    if counter==0:
                        obj=''
                        output_sql_statements=re.findall(r"CALL[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]+|^WITH[\s]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]+|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|TRUNCATE[\s]+TABLE[\s]",fileLine,flags=re.I)
                        # print("output_sql_statements",output_sql_statements)
                        if output_sql_statements:
                            if output_sql_statements[0].upper() in fileLine.upper():
                                counter=1
                                obj=obj+fileLine
                                if "--" in fileLine and ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                    x=fileLine.split("--")
                                    if ";" in x[0]:
                                        counter=0
                                elif ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                    counter=0
                                    if fun_project_ID == "Y":
                                        obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                        obj=re.sub("SELECT[\s]+ERROR","SELECT ERROR",obj,flags=re.I|re.DOTALL)
                                        if "SELECT ERROR" in obj.upper():
                                            pass
                                        else:
                                            if obj.upper().startswith("SELECT "):
                                                find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                                if find_select:
                                                    find_select=re.sub(r"\n|[\s]+","",find_select[0])
                                                    with_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                                all_output_file_statements.append(obj.upper())
                                        output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+\`+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                        # print("check here 2")
                                        for item in output_sql_statements:
                                            item=re.sub(r"\n"," ",item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            remove_space=re.sub(r"\`","",remove_space)
                                            all_output_file_statements.append(remove_space.upper())
                                            item=re.sub(r"[\s]+","",item)
                                            with_Append_project_id_Final_output_sql_statements.append(item.upper())
                                    else:
                                        obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                        obj=re.sub("SELECT[\s]+ERROR","SELECT ERROR",obj,flags=re.I|re.DOTALL)
                                        if "SELECT ERROR" in obj.upper():
                                            pass
                                        else:
                                            # print("check here 3")
                                            if obj.upper().startswith("SELECT "):
                                                find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                                if find_select:
                                                    find_select=re.sub(r"[\s]+","",find_select[0])
                                                    without_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                                all_output_file_statements.append(obj.upper())
                                        output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                        # print("check here 4")
                                        for item in output_sql_statements:
                                            item=re.sub(r"\n","",item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            remove_space=re.sub(r"\`","",remove_space)
                                            all_output_file_statements.append(remove_space.upper())
                                            item=re.sub(r"[\s]+","",item)
                                            without_Append_project_id_Final_output_sql_statements.append(item.upper())

                    else:
                        if counter==1:
                            if "--" in fileLine and ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                x=fileLine.split("--")
                                if ";" in x[0]:
                                    counter=0
                                    obj=obj+fileLine
                                else:
                                    obj=obj+fileLine
                            elif ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                counter=0
                                obj=obj+fileLine
                                # print("o/p_obj",obj)
                                if fun_project_ID == "Y":
                                    # print("fun_project_ID",fun_project_ID)
                                    obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            with_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                        all_output_file_statements.append(obj.upper())
                                    output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    r"|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                    # print("op_:", output_sql_statements)
                                    # print("check here 6")
                                    for item in output_sql_statements:
                                        item=re.sub(r"\n"," ",item)
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        remove_space=re.sub(r"\`","",remove_space)
                                        all_output_file_statements.append(remove_space.upper())
                                        item=re.sub(r"[\s]+","",item)
                                        with_Append_project_id_Final_output_sql_statements.append(item.upper())
                                else:
                                    # print("fun_project_ID",fun_project_ID)

                                    obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            without_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                        all_output_file_statements.append(obj.upper())
                                    output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+"
                                                                     r"|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+"
                                                                     r"|CREATE[\s]*TABLE[\s]*IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]*TABLE[\s]*IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\.[\w]+\`"
                                                                     r"|INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`"
                                                                     r"|DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                    #|UPDATE[\s]*[\w]+\.[\w]+|UPDATE[\s]+[\w]+
                                    # print("check here 7")
                                    # print("cred-o/p:-", output_sql_statements)
                                    for item in output_sql_statements:
                                        # print("after check here 7 check item", item)
                                        item=re.sub(r"\n","",item)
                                        item=re.sub(r"\`","",item)
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        # remove_space=re.sub(r"\`","",item)
                                        remove_space=re.sub(r"\`","",remove_space)
                                        # print("remove `:", remove_space)
                                        all_output_file_statements.append(remove_space.upper())
                                        item=re.sub(r"[\s]+","",item)
                                        without_Append_project_id_Final_output_sql_statements.append(item.upper())
                            else:
                                obj=obj+fileLine
    with_Append_project_id_Final_input_sql_statements.sort()
    with_Append_project_id_Final_output_sql_statements.sort()
    without_Append_project_id_Final_input_sql_statements.sort()
    without_Append_project_id_Final_output_sql_statements.sort()
    # print("with_Append_project_id_Final_input_sql_statements",with_Append_project_id_Final_input_sql_statements)
    # print("with_Append_project_id_Final_output_sql_statements",with_Append_project_id_Final_output_sql_statements)

    # print("without_Append_project_id_Final_input_sql_statements",without_Append_project_id_Final_input_sql_statements)
    # print("without_Append_project_id_Final_output_sql_statements",without_Append_project_id_Final_output_sql_statements)

    # print("all_input_file_statements",all_input_file_statements)
    # print("all_output_file_statements",all_output_file_statements)
    # print("No_with_Append_project_id_Final_input_sql_statements",len(with_Append_project_id_Final_input_sql_statements))
    # print("No_with_Append_project_id_Final_output_sql_statements",len(with_Append_project_id_Final_output_sql_statements))
    # print("No_without_Append_project_id_Final_input_sql_statements",len(without_Append_project_id_Final_input_sql_statements))
    # print("No_without_Append_project_id_Final_output_sql_statements",len(without_Append_project_id_Final_output_sql_statements))


    if len(with_Append_project_id_Final_input_sql_statements)>0:

        if with_Append_project_id_Final_input_sql_statements==with_Append_project_id_Final_output_sql_statements:
            # print("No_Of_Statement SUCCESSFUL")
            Result_No_Of_Statement.append("Y")
            log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
        else:
            Result_No_Of_Statement.append("N")
            log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
    elif len(without_Append_project_id_Final_input_sql_statements)>0:
        if without_Append_project_id_Final_input_sql_statements==without_Append_project_id_Final_output_sql_statements:
            # print("without results SUCCESSFUL")
            Result_No_Of_Statement.append("Y")
            log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
        else:
            Result_No_Of_Statement.append("N")
            log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
    else:
        Result_No_Of_Statement.append("N")
        log_result.append(Result_No(all_input_file_statements,all_output_file_statements))

def check_cross_join_hive(log_result,filename,JoinResult):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Verifying Join Conditions"))
    inputCrossJoin=[]
    outputCrossJoin=[]
    input_cross_join_list=[]
    output_cross_join_list=[]
    input_cross_join_log=[]
    output_cross_join_log=[]
    final_result="NA"
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                input_cross=re.findall(r'CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT',fileLines,flags=re.I|re.DOTALL)

                if input_cross:
                    for ele in input_cross:
                        remove_space=re.sub(r"[\s]+","",ele)
                        inputCrossJoin.append(remove_space)
                        input_cross_join_log.append(ele)

                input_from_to_where=re.findall(r'FROM(.*)WHERE',fileLines,flags=re.I|re.DOTALL)
                if input_from_to_where:
                    for ele in input_from_to_where:
                        input_cross_join_log.append(ele)

                    remove_space=re.split("\,|\n|\t|[\s]+",input_from_to_where[0])
                    for item in remove_space:
                        if item=='':
                            pass
                        else:
                            input_cross_join_list.append(item.upper())

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                output_cross=re.findall(r"CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|CROSS[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|LEFT[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|RIGHT[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+"
                                        r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+",fileLines,flags=re.I|re.DOTALL)

                if output_cross:
                    for ele in output_cross:
                        remove_space=re.sub(r"[\s]+","",ele)
                        outputCrossJoin.append(remove_space)
                        output_cross_join_log.append(ele)
                output_comma_cross=re.findall(r"INNER[\s]+JOIN[\s]+[\w]+\.[\w]+|INNER[\s]+JOIN[\s]+[\w]+[\s]+|INNER[\s]+JOIN[\s]+\([\s]*SELECT[\s]+",fileLines,flags=re.I|re.DOTALL)
                str1=""
                if output_comma_cross:
                    for ele_innerJoin in output_comma_cross:
                        str1=str1+" "+ele_innerJoin
                    output_cross_join_log.append(str1)
                    output_comma_cross=' '.join(map(str, output_comma_cross)).upper().split('INNER JOIN')

                if output_comma_cross:
                    for ele_innerJoin in output_comma_cross:
                        remove_space=re.sub("[\s]+","",ele_innerJoin,flags=re.I)
                        if remove_space=='':
                            pass
                        else:
                            output_cross_join_list.append(remove_space.upper())

        if len(output_cross_join_list)==0:
            final_result="true"
        else:
            for item in output_cross_join_list:
                if item in input_cross_join_list:
                    final_result="true"
                else:
                    final_result="false"
                    break
        inputCrossJoin.sort()
        outputCrossJoin.sort()

        # print(inputCrossJoin)
        # print(outputCrossJoin)

        if len(inputCrossJoin)==0 and len(outputCrossJoin)==0 and final_result=="NA":
            JoinResult.append("NA")
            log_result.append(Result_NA())
        elif len(inputCrossJoin)>0 and len(outputCrossJoin)>0 or final_result=="true":
            if len(inputCrossJoin) == len(outputCrossJoin) and final_result=="true":
                JoinResult.append("Y")
                log_result.append(Result_Yes(input_cross_join_log,output_cross_join_log))
            else:
                JoinResult.append("N")
                log_result.append(Result_No(input_cross_join_log,output_cross_join_log))
        else:
            JoinResult.append("N")
            log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        JoinResult.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def PROJECT_ID_Hive(log_result,project_ID,filename):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Project ID"))
    try:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as f:
            file_obj=f.read()
            project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*\(|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
            if project_id:
                project_ID.append("N")
                log_result.append(Result_No(project_id,project_id))
            else:
                project_id=re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\(|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
                if project_id:
                    project_ID.append("Y")
                    log_result.append(Result_Yes(project_id,project_id))
                else:
                    project_ID.append("NA")
                    log_result.append(Result_NA())
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        project_ID.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_colan_to_space(log_result,filename,Result_check_colan_to_space):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check for Struct colan to space"))
    input_struct_raw=[]
    output_struct_raw=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()

            find_struct=re.findall(r"struct\<(.*?)\>\>",query,flags=re.I|re.DOTALL)
            for i in find_struct:
                find_struct1=i.split(",")
                for replce_space in find_struct1:
                    replce_space=re.sub(r":", " ",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"int|bigint|smallint|tinyint", "INT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"double|float", "FLOAT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"string", "STRING",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"string\(|varchar\(|char\(", "STRING(",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"date", "DATE",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"binary", "BYTES",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"timestamp", "TIMESTAMP",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"boolean", "BOOL",replce_space,flags=re.I|re.DOTALL)
                    input_struct_raw.append(replce_space)

    if filename in outputFile:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()

            query=re.sub(r"struct\<[\s]*key[\s]*[\w]+\,[\s]*value[\s]*[\w]+\>"," ",query,flags=re.I|re.DOTALL)
            find_struct_op=re.findall(r"struct\<(.*?)\>\>",query,flags=re.I|re.DOTALL)
            for i in find_struct_op:
                output_struct_raw1=i.split(",")
                for replce_space in output_struct_raw1:
                    output_struct_raw.append(replce_space)

                    # print("input_struct_raw=", input_struct_raw)
    # print("output_struct_raw=", output_struct_raw)        

    if len(input_struct_raw)==0 and len(output_struct_raw)==0:
        Result_check_colan_to_space.append("NA")
        log_result.append(Result_NA())
    elif input_struct_raw == output_struct_raw:
        # print("check colan to space has been done successfully")
        Result_check_colan_to_space.append("Y")
        log_result.append(Result_Yes(find_struct,find_struct_op))
    else:
        Result_check_colan_to_space.append("N")
        log_result.append(Result_No(input_struct_raw,output_struct_raw))

def Check_map_to_array(log_result,filename,Result_check_map_to_array):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check for Map to Array"))
    input_map=[]
    input_map_raw=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()

            find_map=re.findall(r"map\<(.*?)\>",query,flags=re.I|re.DOTALL)
            find_inputlog=re.findall(r"map\<[\w]+\,[\w]+\>",query,flags=re.I|re.DOTALL)

            for i in find_map:
                find_map1=i.split(",")
                for replce_space in find_map1:
                    replce_space=re.sub(r"int|bigint|smallint|tinyint", "INT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"double|float", "FLOAT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"string|varchar|char", "STRING",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"date", "DATE",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"binary", "BYTES",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"timestamp", "TIMESTAMP",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"boolean", "BOOL",replce_space,flags=re.I|re.DOTALL)
                    input_map_raw.append(replce_space)

            a=[i for i in zip(*[iter(input_map_raw)]*2)]
            for i in a:
                input_map.append("array<struct<" + "key "+ i[0] + ",value "+ i[1] + ">>")

    if filename in outputFile:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            output_array=re.findall(r"(ARRAY<STRUCT[\s]*\<[\s]*key[\s]*[\w]+\,[\s]*value[\s]*[\w]+[\s]*\>\>)",query,flags=re.I|re.DOTALL)

    # print("input_map=", input_map)
    # print("output_array=", output_array)

    if len(input_map)==0 and len(output_array)==0:
        Result_check_map_to_array.append("NA")
        log_result.append(Result_NA())
    elif len(input_map) == len(output_array):
        # print("Check Map to Array has been done successfully")
        Result_check_map_to_array.append("Y")
        log_result.append(Result_Yes(find_inputlog,output_array))
    else:
        Result_check_map_to_array.append("N")
        log_result.append(Result_No(find_inputlog,output_array))

def fun_Check_par_col_datatype_change(log_result,filename,result_check_par_col_datatype_change,table_column_list):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_par_col_datatype_change"))
    input_columns=[]
    output_columns=[]
    input_log=[]
    output_log=[]
    try:
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        for ele in table_column_list:

            if ele.split(':')[0].lower() in file_obj.lower():
                # print("ele", ele)

                find_columns = re.findall(r'\`*[\w]+\`*[\s]+[\w]+', file_obj, flags=re.I|re.DOTALL)
                # print(find_columns)

                if find_columns:
                    for col in find_columns:
                        col = re.sub(r"[\s]+", " ", col)
                        col1 = re.sub(r"\`", "", col)

                        if col1.split(' ')[0].lower() == ele.split(':')[1].lower():
                            # print("col1", col1)
                            output_columns.append(col1)
                            output_log.append(col1)

        
        output_columns.sort()


        # print("output_columns=", len(output_columns),output_columns)

        if len(output_columns)==0:
            result_check_par_col_datatype_change.append("NA")
            log_result.append(Result_NA())
        elif output_columns:
            # print("YYYY")
            result_check_par_col_datatype_change.append("Y")
            log_result.append(Result_Yes(output_log,output_log))
        else:
            result_check_par_col_datatype_change.append("N")
            log_result.append(Result_No(output_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_par_col_datatype_change.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_Check_add_option_change(log_result,filename,result_Check_add_option_change,table_option_list):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_par_col_datatype_change"))
    input_option=[]
    output_option=[]
    input_log=[]
    output_log=[]
    try:
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename) 
        # print(table_option_list)
        for ele in table_option_list:

            if ele.split(':')[0].lower() in file_obj.lower():
                # print("ele", ele)

                find_option = re.findall(r'options[\s]*\(.*?\;', file_obj)
                # print(find_option)

                if find_option:
                    for op in find_option:
                        output_option.append(op)
                        output_log.append(op)


        # print("output_option=", len(output_option),output_option)

        if len(output_option)==0:
            result_Check_add_option_change.append("NA")
            log_result.append(Result_NA())
        elif output_option:
            # print("YYYY")
            result_Check_add_option_change.append("Y")
            log_result.append(Result_Yes(output_log,output_log))
        else:
            result_Check_add_option_change.append("N")
            log_result.append(Result_No(output_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_add_option_change.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_PartitionBy_to_ClusterBy(log_result,filename,result_Check_PartitionBy_to_ClusterBy):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partition to cluster by check"))
    input_partition_list = []
    output_cluster_by_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                query = f.read()
                f.close()
                query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",query,flags=re.I|re.DOTALL)
                query = re.sub(r"array\<(.*?)\>"," ",query,flags=re.I|re.DOTALL)
                find_par_col=re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)",query,flags=re.I|re.DOTALL)
                try:
                    for ele in find_par_col:
                        ele=re.sub(r"\`*[\w]+\`*[\s]+DATE|\\n\s|STRING|\,|(CO.{3}NT)+\s*\'(.*?)\'|\`","",ele,flags=re.I)
                        input_partition_list=ele.split()
                except:
                    pass

                try:
                    find_clusted_col=re.findall(r"CLUSTERED[\s]+BY[\s]*\((.*?)\)",query,flags=re.I|re.DOTALL)
                    find_clusted_col=find_clusted_col[0].split()
                    for ele in find_clusted_col:
                        ele=re.sub(r"\n|\s+|\,","",ele)
                        input_partition_list.append(ele)
                except:
                    pass

        Log_input=input_partition_list
        input_partition_list=input_partition_list[:4]

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as finl:
                query = finl.read()
                finl.close()
                query =re.sub(r"(OPTIONS\(DESCRIPTION\s*\=)*\s*\'(.*?)\'\)","",query,flags=re.DOTALL|re.I)
                find_clust =re.findall(r"CLUSTER[\s]+BY[\s]+(.*?)\;",query,flags=re.DOTALL|re.I)
                try:
                    find_clust[0]=re.sub(r"\s*|\n","",find_clust[0],flags=re.I)
                    output_cluster_by_list=find_clust[0].split(",")
                    Log_output=output_cluster_by_list
                except:
                    pass

        # print("input_partition_list:", input_partition_list)
        # print("output_cluster_by_list:", output_cluster_by_list)

        if len(input_partition_list)==0:
            log_result.append(Result_NA())
            result_Check_PartitionBy_to_ClusterBy.append("NA")
        elif input_partition_list==output_cluster_by_list:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Check_PartitionBy_to_ClusterBy.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Check_PartitionBy_to_ClusterBy.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_PartitionBy_to_ClusterBy.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_clusterBy_create_cols(log_result,filename,result_Check_clusterBy_create_cols):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("clusterBy Create Columns check"))
    input_create_list = []
    output_create_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                query = f.read()
                f.close()
                query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",query,flags=re.I|re.DOTALL)
                query = re.sub(r"array\<(.*?)\>"," ",query,flags=re.I|re.DOTALL)

                if ' TABLE' in query:
                    find_par_col=re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)",query,flags=re.I|re.DOTALL)
                    if find_par_col:
                        try:
                            create_list=find_par_col[0].split(",")
                            for ele in create_list:
                                ele=re.sub(r"\n|\s*|\`|\,","",ele)
                                input_create_list.append(ele.lower())
                                Log_input.append(ele)
                        except:
                            pass

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as finl:
                query1 = finl.read()
                finl.close()
                query1=re.sub(r'OPTIONS\(DESCRIPTION[\s]*=[\s]*\'.*?\'\)|\d+\)', '', query1, flags=re.I)

                if ' TABLE' in query:
                    find_create_col=re.findall(r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]*\((.*?)\)",query1,flags=re.DOTALL|re.I)
                    create_list=find_create_col[0].split(",")
                    if create_list:
                        try:
                            for ele in create_list:
                                ele=re.sub(r"\n|\s*|\`|\,","",ele)
                                output_create_list.append(ele.lower())
                                Log_output.append(ele)
                        except:
                            pass

        # print("input_partition_list:", input_create_list)
        # print("output_create_list:", output_create_list)

        if len(input_create_list)==0:  #len(find_par_col)==0: #and len(output_create_list)==0:
            log_result.append(Result_NA())
            result_Check_clusterBy_create_cols.append("NA")
        elif set(input_create_list).issubset(output_create_list)==True:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Check_clusterBy_create_cols.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Check_clusterBy_create_cols.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_clusterBy_create_cols.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Partitioned_BY_to_Partition_By(log_result,filename,result_Check_Partitioned_BY_to_Partition_By):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partition to cluster by check"))
    input_partitioned_list = []
    output_partition_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                query = f.read()
                f.close()
                query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",query,flags=re.I|re.DOTALL)
                query = re.sub(r"array\<(.*?)\>"," ",query,flags=re.I|re.DOTALL)
                find_par_col=re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)",query,flags=re.I|re.DOTALL)
                if find_par_col:
                    try:
                        for ele in find_par_col:
                            ele=re.sub(r"\`*[\w]+\`*[\s]+STRING|\`*[\w]+\`*[\s]+INT|\`*[\w]+\`*[\s]+TIMESTAMP|DATE|\\n\s|\,|(CO.{3}NT)+\s*\'(.*?)\'|\`","",ele,flags=re.I)
                            input_partitioned_list=ele.split()
                    except:
                        pass

        Log_input=input_partitioned_list

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as finl:
                query = finl.read()
                finl.close()
                query =re.sub(r"CLUSTER[\s]+BY[\s]+(.*?)\;",';',query,flags=re.DOTALL|re.I)
                query =re.sub(r"RANGE_BUCKET[\s]*\(",'',query,flags=re.DOTALL|re.I)
                query =re.sub(r"DATE[\s]*\(",'',query,flags=re.DOTALL|re.I)
                find_clust =re.findall(r"PARTITION[\s]+BY[\s]+(.*?)\;",query,flags=re.DOTALL|re.I)

                try:
                    list1=find_clust[0].split(",")
                    for i in list1:
                        i=re.sub(r"\(|\)|\n","",i)
                        output_partition_list.append(i)
                    Log_output=output_partition_list
                except:
                    pass

        # print("input_partition_list:", input_partitioned_list)
        # print("output_cluster_by_list:", output_partition_list)

        if len(input_partitioned_list)==0:
            log_result.append(Result_NA())
            result_Check_Partitioned_BY_to_Partition_By.append("NA")
        elif set(input_partitioned_list).issubset(output_partition_list)==True:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Check_Partitioned_BY_to_Partition_By.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Check_Partitioned_BY_to_Partition_By.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_Partitioned_BY_to_Partition_By.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def check_for_current_functions_CS(log_result,filename,result_check_for_current_functions):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Current Functions"))
    input_current_func=[]
    output_current_func=[]
    log_input=[]
    log_output=[]
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
                        find_time=re.findall(r"CURRENT_TIME[\s]*\(\)|CURRENT_TIME[\s]*\([0-9]+\)|CURRENT_TIME[\s]+|CURRENT_TIME[\s]*\)|CURRENT_TIME[\s]*\,",fileLine,flags=re.I)
                        if find_time==[]:
                            pass
                        else:
                            for ele in find_time:
                                log_input.append(ele)
                                ele=re.sub(r"\,|\(|[\d]*|\)|\n","",ele,flags=re.I)
                                ele=re.sub(r"TIME",'TIME("America/New_York")',ele,flags=re.I)
                                ele=re.sub(r"[\s]*","",ele,flags=re.I)
                                input_current_func.append(ele)

                        find_datetime=re.findall(r"CURRENT_DATETIME[\s]*\(\)|CURRENT_DATETIME[\s]*\([0-9]+\)|CURRENT_DATETIME[\s]+|CURRENT_DATETIME[\s]*\)|CURRENT_DATETIME[\s]*\,",fileLine,flags=re.I)
                        if find_datetime==[]:
                            pass
                        else:
                            for ele in find_datetime:
                                log_input.append(ele)
                                ele=re.sub(r"\,|\(|[\d]*|\)|\n","",ele,flags=re.I)
                                ele=re.sub(r"DATETIME",'DATETIME("America/New_York")',ele,flags=re.I)
                                ele=re.sub(r"[\s]*","",ele,flags=re.I)
                                input_current_func.append(ele)

                        find_date=re.findall(r"CURRENT_DATE[\s]*\(\)|CURRENT_DATE[\s]*\([0-9]+\)|CURRENT_DATE[\s]+|CURRENT_DATE[\s]*\)|CURRENT_DATE[\s]*\,",fileLine,flags=re.I)
                        if find_date==[]:
                            pass
                        else:
                            for ele in find_date:
                                log_input.append(ele)
                                ele=re.sub(r"\,|\(|[\d]*|\)|\n","",ele,flags=re.I)
                                ele=re.sub(r"DATE",'DATE("America/New_York")',ele,flags=re.I)
                                ele=re.sub(r"[\s]*","",ele,flags=re.I)
                                input_current_func.append(ele)

                        find_extra=re.findall(r'CURRENT_TIME\(\"AMERICA\/NEW_YORK\"\)|CURRENT_TIME\(\'AMERICA\/NEW_YORK\'\)|'
                                              r'CURRENT_DATETIME\(\"AMERICA\/NEW_YORK\"\)|CURRENT_DATETIME\(\'AMERICA\/NEW_YORK\'\)|'
                                              r'CURRENT_DATE\(\"AMERICA\/NEW_YORK\"\)|CURRENT_DATE\(\'AMERICA\/NEW_YORK\'\)',fileLine,flags=re.I)

                        for ele in find_extra:
                            log_input.append(ele)
                            input_current_func.append(ele)


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_current=re.findall(r"CURRENT_DATETIME\(\"AMERICA\/NEW_YORK\"\)|CURRENT_DATETIME\(\'AMERICA\/NEW_YORK\'\)|"
                                            r"CURRENT_DATE\(\"AMERICA\/NEW_YORK\"\)|CURRENT_DATE\(\'AMERICA\/NEW_YORK\'\)|"
                                            r"CURRENT_TIME\(\"AMERICA\/NEW_YORK\"\)|CURRENT_TIME\(\'AMERICA\/NEW_YORK\'\)",fileLine,flags=re.I)

                    for ele in find_current:
                        log_output.append(ele)
                        output_current_func.append(ele)

        # print("input_current_func=",filename,input_current_func)
        # print("output_current_func=",filename,output_current_func)

        if len(input_current_func)==0 and len(output_current_func)==0:
            log_result.append(Result_NA())
            result_check_for_current_functions.append("NA")
        elif len(input_current_func)==len(output_current_func):
            # print("Current Functions has been done succesfully")
            log_result.append(Result_Yes(log_input,log_output))
            result_check_for_current_functions.append("Y")
        else:
            log_result.append(Result_No(log_input,log_output))
            result_check_for_current_functions.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_for_current_functions.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_schema_table_case(log_result,filename,result_schema_table_case):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Schema Table Case Checking"))
    input_schema_table=[]
    output_schema_table=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_schema_table_ip=re.findall(r"IF[\s]+NOT[\s]+EXISTS[\s]+\`[\w]+\.[\w]+\`|REPLACE[\s]+VIEW[\s]+\`[\w]+\.[\w]+\`|CREATE[\s]+VIEW[\s]+\`[\w]+\.[\w]+\`",fileLine,flags=re.I)

                    for ele in find_schema_table_ip:
                        ele=re.sub(r"IF[\s]+NOT[\s]+EXISTS[\s]*|REPLACE[\s]+VIEW[\s]+|CREATE[\s]+VIEW[\s]+|\`","",ele,flags=re.I)
                        input_schema_table.append(ele)
                        Log_input.append(ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_schema_table_op=re.findall(r"IF[\s]+NOT[\s]+EXISTS[\s]+[\w]+\.[\w]+|REPLACE[\s]+VIEW[\s]+[\w]+\.[\w]+",fileLine,flags=re.I)

                    for ele in find_schema_table_op:
                        ele=re.sub(r"IF[\s]+NOT[\s]+EXISTS[\s]*|REPLACE[\s]+VIEW[\s]+","",ele,flags=re.I)
                        output_schema_table.append(ele)
                        Log_output.append(ele)

        # print("input_schema_table-",input_schema_table)
        # print("output_schema_table-",output_schema_table)

        if len(input_schema_table)==0 and len(output_schema_table)==0:
            log_result.append(Result_NA())
            result_schema_table_case.append("NA")
        elif input_schema_table==output_schema_table:
            log_result.append(Result_Yes(Log_input,Log_output))
            result_schema_table_case.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_schema_table_case.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_schema_table_case.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_column_and_its_alias_check(log_result,filename,result_column_and_its_alias_check):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table Alias Column Checking"))
    input_column_and_its_alias_check=[]
    output_column_and_its_alias_check=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_table_alias_ip=re.findall(r"FROM[\s]+\`[\w]+\`\.\`[\w]+\`[\s]+\`[\w]+\`|JOIN[\s]+\`[\w]+\`\.\`[\w]+\`[\s]+\`[\w]+\`",fileLines,flags=re.I|re.DOTALL)

                if find_table_alias_ip:
                    for ele in find_table_alias_ip:
                        ele=re.sub(r"FROM[\s]+|JOIN[\s]+|\`","",ele,flags=re.I)
                        ele=re.sub(r"[\w]+\.[\w]+[\s]*","",ele,flags=re.I)
                        ele=re.sub(ele,"AS "+ele,ele,flags=re.I)
                        input_column_and_its_alias_check.append(ele)
                        Log_input.append(ele)

                find_table_alias_ip1=re.findall(r"AS[\s]*SELECT[\s]*(.*?)FROM[\s]+",fileLines,flags=re.I|re.DOTALL)
                if find_table_alias_ip1:
                    for ele in find_table_alias_ip1[0].split(","):
                        ele=ele.lstrip()
                        ele=ele.rstrip()
                        ele=re.sub(r"distinct[\s]+|\`","",ele,flags=re.I)
                        ele=re.sub(r"\s+\-\s+|\s+\>\s+|\s+","",ele,flags=re.I)
                        if re.findall(r"\w+\.\d+",ele,flags=re.I):
                            ele=re.sub(r"\.","._",ele,flags=re.I)
                        input_column_and_its_alias_check.append(ele)
                        Log_input.append(ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.read()
                find_table_alias_op=re.findall(r"FROM[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+|JOIN[\s]+[\w]+\.[\w]+[\s]+AS[\s]+[\w]+",fileLines,flags=re.I|re.DOTALL)

                if find_table_alias_op:
                    for ele in find_table_alias_op:
                        ele=re.sub(r"FROM[\s]+|JOIN[\s]+","",ele,flags=re.I)
                        ele=re.sub(r"[\w]+\.[\w]+[\s]*","",ele,flags=re.I)
                        output_column_and_its_alias_check.append(ele)
                        Log_output.append(ele)

                find_table_alias_op1=re.findall(r"AS[\s]*SELECT[\s]*(.*?)FROM[\s]+",fileLines,flags=re.I|re.DOTALL)
                if find_table_alias_op1:
                    for ele in find_table_alias_op1[0].split(","):
                        ele=re.sub(r"DISTINCT|\s*","",ele,flags=re.I)
                        output_column_and_its_alias_check.append(ele)
                        Log_output.append(ele)

        # print("input_all_datatype =",input_column_and_its_alias_check)
        # print("output_all_datatype=",output_column_and_its_alias_check)

        if len(input_column_and_its_alias_check)==0 and len(output_column_and_its_alias_check)==0:
            log_result.append(Result_NA())
            result_column_and_its_alias_check.append("NA")
        elif input_column_and_its_alias_check==output_column_and_its_alias_check:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_column_and_its_alias_check.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_column_and_its_alias_check.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_column_and_its_alias_check.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_From_Unixtime(log_result,filename,result_From_Unixtime):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table Select From Timestamp"))
    input_from_unixtime=[]
    output_from_unixtime=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_from_unixtime_ip=re.findall(r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyyMMddHHmmss\'\)\,\'yyyy-MM-dd[\s]*HH:mm:ss\'\)|" #1          
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyyMMddHHmmss\'\)|" #2    
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyyMMdd\'\)|" #3    
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'MM/dd/yyyy[\s]*HH:mm:ss\'\)|" #6
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyy-MM-dd[\s]*HH:mm:ss\'\)|" #7
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyy\-MM\-dd\'\)|" #4
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'yyyy\-MM\-dd\'\)\,\'MM/dd/yyyy\'\)|" #8
                                                 r"from_unixtime[\s]*\(UNIX_TIMESTAMP[\s]*\(.*?\,[\s]*\'MM/dd/yyyy\'\)",fileLines,flags=re.I|re.DOTALL) #5


                if find_from_unixtime_ip:
                    for ele in find_from_unixtime_ip:
                        input_from_unixtime.append(ele.lower())
                        Log_input.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.read()

                find_from_unixtime_op=re.findall(r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"YYYYMMDDHH24MISS\"[\s]*\)|" #1,2
                                                 r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"YYYYMMDD\"[\s]*\)|" #3
                                                 r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"YYYY\-MM\-DD\"[\s]*\)|" #4
                                                 r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"MM\/DD\/YYYY\"[\s]*\)|" #5
                                                 r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"MM\/DD\/YYYY[\s]*HH24\:MI\:SS\"[\s]*\)|" #6
                                                 r"CAST\(.*?AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"YYYY\-MM\-DD[\s]*HH24\:MI\:SS\"[\s]*\)|" #7
                                                 r"FORMAT_TIMESTAMP[\s]*\(\"\%m\/\%d\/\%Y\"\,[\s]*CAST\([\w]+\.[\w]+[\s]+AS[\s]+TIMESTAMP[\s]+FORMAT[\s]+\"YYYY\-MM\-DD\"[\s]*\)\)*",fileLines,flags=re.I|re.DOTALL) #8

                if find_from_unixtime_op:
                    for ele in find_from_unixtime_op:
                        output_from_unixtime.append(ele.lower())
                        Log_output.append(ele.lower())

        # print("input_from_unixtime =",input_from_unixtime)
        # print("output_from_unixtime =",output_from_unixtime)

        if len(input_from_unixtime)==0 and len(output_from_unixtime)==0:
            log_result.append(Result_NA())
            result_From_Unixtime.append("NA")
        elif len(input_from_unixtime)==len(output_from_unixtime):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_From_Unixtime.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_From_Unixtime.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_From_Unixtime.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_NVL_Columns(log_result,filename,result_NVL_Columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("NVL Columns"))
    input_nvl_columns=[]
    output_nvl_columns=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_nvl_columns_ip=re.findall(r"NVL\(.*?\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_nvl_columns_ip:
                    input_nvl_columns.append(ele.lower())
                    Log_input.append(ele)

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.read()

                find_nvl_columns_op=re.findall(r"IFNULL\(.*?\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_nvl_columns_op:
                    output_nvl_columns.append(ele.lower())
                    Log_output.append(ele)

        # print("input_all_datatype =",len(input_table_alias_column))
        # print("output_all_datatype=",len(output_table_alias_column))

        if len(input_nvl_columns)==0 and len(output_nvl_columns)==0:
            log_result.append(Result_NA())
            result_NVL_Columns.append("NA")
        elif len(input_nvl_columns)==len(output_nvl_columns):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_NVL_Columns.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_NVL_Columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_NVL_Columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_DATE_SUB_Columns(log_result,filename,result_DATE_SUB_Columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table Select From Timestamp"))
    input_date_sub_column=[]
    output_date_sub_column=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_date_sub_ip=re.findall(r"date_sub[\s]*\(\w+\,[\s]*\d+\)",fileLine,flags=re.I|re.DOTALL)

                    for ele in find_date_sub_ip:
                        Log_input.append(ele)
                        ele=re.sub(r"\,",",INTERVAL",ele,flags=re.I)
                        ele=re.sub(r"\)","DAY)",ele,flags=re.I)
                        ele=re.sub(r"\s*","",ele,flags=re.I)
                        input_date_sub_column.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_date_sub_op=re.findall(r"date_sub[\s]*\(\w+\,INTERVAL \d+ DAY\)",fileLine,flags=re.I|re.DOTALL)

                    for ele in find_date_sub_op:
                        Log_output.append(ele)
                        ele=re.sub(r"\s*","",ele,flags=re.I)
                        output_date_sub_column.append(ele.lower())

        # print("input_all_datatype =",input_date_sub_column)
        # print("output_all_datatype=",output_date_sub_column)

        if len(input_date_sub_column)==0 and len(output_date_sub_column)==0:
            log_result.append(Result_NA())
            result_DATE_SUB_Columns.append("NA")
        elif input_date_sub_column==output_date_sub_column:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_DATE_SUB_Columns.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_DATE_SUB_Columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_DATE_SUB_Columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_To_Date_Columns(log_result,filename,result_To_Date_Columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table Select From Timestamp"))
    input_to_date_column=[]
    output_to_date_column=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_to_date_ip=re.findall(r"to_date\(\w+\)",fileLine,flags=re.I|re.DOTALL)

                    for ele in find_to_date_ip:
                        Log_input.append(ele)
                        ele=re.sub(r"to_date","date",ele,flags=re.I)
                        input_to_date_column.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                for fileLine in fileLines:
                    find_to_date_op=re.findall(r"date\(\w+\)",fileLine,flags=re.I|re.DOTALL)

                    for ele in find_to_date_op:
                        Log_output.append(ele)
                        output_to_date_column.append(ele.lower())

        # print("input_all_datatype =",input_to_date_column)
        # print("output_all_datatype=",output_to_date_column)

        if len(input_to_date_column)==0 and len(output_to_date_column)==0:
            log_result.append(Result_NA())
            result_To_Date_Columns.append("NA")
        elif input_to_date_column==output_to_date_column:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_To_Date_Columns.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_To_Date_Columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_To_Date_Columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Date_Add_Columns(log_result,filename,result_Date_Add_Columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Date Add Columns"))
    input_date_add_column=[]
    output_date_add_column=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_to_date_ip=re.findall(r"DATE_ADD\(.*?\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_to_date_ip:
                    Log_input.append(ele)
                    input_date_add_column.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.read()

                find_date_add_op=re.findall(r"DATE_ADD\(.*?DAY[\s]*\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_date_add_op:
                    Log_output.append(ele)
                    output_date_add_column.append(ele.lower())

        # print("input_all_datatype =",len(input_date_add_column))
        # print("output_all_datatype=",len(output_date_add_column))

        if len(input_date_add_column)==0 and len(output_date_add_column)==0:
            log_result.append(Result_NA())
            result_Date_Add_Columns.append("NA")
        elif len(input_date_add_column)==len(output_date_add_column):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Date_Add_Columns.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Date_Add_Columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Date_Add_Columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def Check_Date_Diff_Columns(log_result,filename,result_Date_Diff_Columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Date Diff Columns"))
    input_date_diff_column=[]
    output_date_diff_column=[]
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()

                find_date_diff_ip=re.findall(r"DATEDIFF\(.*?\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_date_diff_ip:
                    ele=re.sub(r"\)",",DAY)",ele,flags=re.I)
                    ele=re.sub(r"[\s]+"," ",ele,flags=re.I)
                    Log_input.append(ele)
                    input_date_diff_column.append(ele.lower())

        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.read()

                find_date_diff_op=re.findall(r"DATE_DIFF\(.*?DAY[\s]*\)",fileLines,flags=re.I|re.DOTALL)
                for ele in find_date_diff_op:
                    ele=re.sub(r"[\s]+"," ",ele,flags=re.I)
                    Log_output.append(ele)
                    output_date_diff_column.append(ele.lower())


        # print("input_all_datatype =",len(input_date_diff_column))
        # print("output_all_datatype=",len(output_date_diff_column))

        if len(input_date_diff_column)==0 and len(output_date_diff_column)==0:
            log_result.append(Result_NA())
            result_Date_Diff_Columns.append("NA")
        elif len(input_date_diff_column)==len(output_date_diff_column):
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Date_Diff_Columns.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Date_Diff_Columns.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Date_Diff_Columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)


#Flipkart project
def fkt_check_datatype_for_HV(log_result,filename,Datatype_result_for_HV,fkt_check_timestamp_to_datetime):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Datatype Checking"))
    integer_column=[]
    float_column=[]
    char_varchar_datatype=[]
    decimal_column=[]
    date_column=[]
    timestamp_column = []
    boolean_column = []
    binary_column = []
    array_column=[]
    input_datatype=[]
    input_all_datatype=[]
    output_final_decimal_column=[]
    output_all_datatype=[]
    BQ_input_cloumn=[]
    BQ_output_cloumn=[]
    string_datatype=[]
    input_all_datatype1=[]
    output_all_datatype1=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            # fileLines = f.read()
            # find_struct= re.findall(r'[\s]+struct[\s]*\<',fileLines,flags=re.I|re.DOTALL)
            # # for item in find_struct:
            # # find_exists=re.findall(r'exists\(',fileline,flags=re.I)
            # if find_struct:
            #     split_str=re.split('[\s]+struct[\s]*\<',fileLines,flags=re.I)
            #     for ele in split_str[1:]:                    
            #         ele='exists('+ele
            #         exists_string=''
            #         s_c=0
            #         e_c=0
            #         for i in ele:
            #             if i=='(':
            #                 s_c+=1
            #             elif i==')':
            #                 e_c+=1
            #             exists_string+=i
            #             if s_c != 0 and s_c==e_c:
            #                 break
                

            for fileLine in fileLines:
                # print(fileLine)
                find_struct=re.findall(r'[\s]+struct[\s]*\<',fileLine,flags=re.I|re.DOTALL)
                if fileLine.startswith("--"):
                    pass
                elif find_struct:
                    BQ_input_cloumn.append(fileLine)
                    # print(fileLine)

                    fileLine = re.sub(r"CREATE.*TABLE.*\`*[\w]+\.[\w]+\`*[\s]*\(|\)[\s]*STORED[\s]*AS.*?\;", "", fileLine, flags=re.I|re.DOTALL) #added 23 feb
                    
                    
                    fileLine=re.sub("\:timestamp",":DATETIME",fileLine,flags=re.I)
                    # fileLine=re.sub("\<[\s]*timestamp","< DATETIME",fileLine,flags=re.I)
                    fileLine=fileLine.replace(":"," ")
                    fileLine=fileLine.replace("`","")
                    fileLine=re.sub("\,"," , ",fileLine,flags=re.I)
                    fileLine=re.sub("\>"," > ",fileLine,flags=re.I)
                    fileLine=re.sub("\<"," < ",fileLine,flags=re.I)
                    fileLine=re.sub(" BIGINT | SMALLINT | TINYINT | INT ","INT64",fileLine,flags=re.I)
                    fileLine=re.sub(" FLOAT | DOUBLE "," FLOAT64 ",fileLine,flags=re.I)
                    fileLine=re.sub(" VARCHAR | CHAR "," STRING ",fileLine,flags=re.I)
                    fileLine=re.sub(" STRING | void "," STRING ",fileLine,flags=re.I)
                    fileLine=re.sub(" BOOLEAN "," BOOL",fileLine,flags=re.I)
                    fileLine=re.sub(" BINARY ","BYTES",fileLine,flags=re.I)
                    fileLine=re.sub(" DATE "," DATE ",fileLine,flags=re.I)  
                    fileLine=re.sub("\< timestamp \>","< DATETIME >",fileLine,flags=re.I)   #added 23 feb
                    # print(fileLine)
                    fileLine=re.sub("\,[\s]*$","",fileLine,flags=re.I)
                    find_cmt=re.findall(r"COMMENT[\s]*\'(.*)\'",fileLine,flags=re.I|re.DOTALL)
                    if find_cmt:
                        fileLine=re.sub("COMMENT[\s]*\'.*","",fileLine,flags=re.I)
                    input_all_datatype.append(re.sub("[\s]+","",fileLine,flags=re.I).lower())

                    # print("s_input_all_datatype", input_all_datatype)

                else:
                    fileLine = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",fileLine,flags=re.I|re.DOTALL)
                    find_integer=re.findall(r"[\w]+[\s]+INT\)*[\s]+|\_[\w]+[\s]+INT\)*[\s]+|\`\_[\w]+\`[\s]+INT\)*[\s]+|\`[\w]+\`[\s]+INT\)*[\s]+|[\w]+[\s]+INT\,|\_[\w]+[\s]+INT\,|\_[\w]+[\s]+INT|\`\_[\w]+\`[\s]+INT\,|\`\_[\w]+\`[\s]+INT|\`[\w]+\`[\s]+INT\,|\`[\w]+\`[\s]+INT\,"
                                            r"|[\w]+[\s]+BIGINT\)*[\s]+|\_[\w]+[\s]+BIGINT\)*[\s]+|\`\_[\w]+\`[\s]+BIGINT\)*[\s]+|\`[\w]+\`[\s]+BIGINT\)*[\s]+|[\w]+[\s]+BIGINT\,|\_[\w]+[\s]+BIGINT\,|\_[\w]+[\s]+BIGINT|\`\_[\w]+\`[\s]+BIGINT\,|\`\_[\w]+\`[\s]+BIGINT|\`[\w]+\`[\s]+BIGINT\,|\`[\w]+\`[\s]+BIGINT\,"
                                            r"|[\w]+[\s]+SMALLINT\)*[\s]+|\_[\w]+[\s]+SMALLINT\)*[\s]+|\`\_[\w]+\`[\s]+SMALLINT\)*[\s]+|\`[\w]+\`[\s]+SMALLINT\)*[\s]+|[\w]+[\s]+SMALLINT\,|\_[\w]+[\s]+SMALLINT\,|\_[\w]+[\s]+SMALLINT|\`\_[\w]+\`[\s]+SMALLINT\,|\`\_[\w]+\`[\s]+SMALLINT|\`[\w]+\`[\s]+SMALLINT\,|\`[\w]+\`[\s]+SMALLINT\,"
                                            r"|[\w]+[\s]+TINYINT\)*[\s]+|\_[\w]+[\s]+TINYINT\)*[\s]+|\`\_[\w]+\`[\s]+TINYINT\)*[\s]+|\`[\w]+\`[\s]+TINYINT\)*[\s]+|[\w]+[\s]+TINYINT\,|\_[\w]+[\s]+TINYINT\,|\_[\w]+[\s]+TINYINT|\`\_[\w]+\`[\s]+TINYINT\,|\`\_[\w]+\`[\s]+TINYINT|\`[\w]+\`[\s]+TINYINT\,|\`[\w]+\`[\s]+TINYINT\,",fileLine,flags=re.I)
                    #print("find_integer",find_integer)
                    for ele in find_integer:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        integer_column.append(ele.upper())

                        #print("integer_column",integer_column)

                    find_float=re.findall(r"[\w]+[\s]+FLOAT\)*[\s]+|\_[\w]+[\s]+FLOAT\)*[\s]+|\`\_[\w]+\`[\s]+FLOAT\)*[\s]+|\`[\w]+\`[\s]+FLOAT\)*[\s]+|[\w]+[\s]+FLOAT\,|\_[\w]+[\s]+FLOAT\,|\_[\w]+[\s]+FLOAT|\`\_[\w]+\`[\s]+FLOAT\,|\`\_[\w]+\`[\s]+FLOAT|\`[\w]+\`[\s]+FLOAT\,|\`[\w]+\`[\s]+FLOAT\,"
                                          r"|[\w]+[\s]+DOUBLE\)*[\s]+|\_[\w]+[\s]+DOUBLE\)*[\s]+|\`\_[\w]+\`[\s]+DOUBLE\)*[\s]+|\`[\w]+\`[\s]+DOUBLE\)*[\s]+|[\w]+[\s]+DOUBLE\,|\_[\w]+[\s]+DOUBLE\,|\_[\w]+[\s]+DOUBLE|\`\_[\w]+\`[\s]+DOUBLE\,|\`\_[\w]+\`[\s]+DOUBLE|\`[\w]+\`[\s]+DOUBLE\,|\`[\w]+\`[\s]+DOUBLE\,",fileLine,flags=re.I)
                    for ele in find_float:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        ele=re.sub(r"\,","",ele,flags=re.I)
                        float_column.append(ele.upper())
                        # print("float_column :", float_column) 

                    find_char=re.findall(r"[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\_[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\`\_[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|\`[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*|[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\_[\w]+[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`\_[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`[\w]+\`[\s]+VARCHAR[\s]*\(\s*[\d]+\s*\)\s*\)"
                                         r"|[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\_[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\`\_[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|\`[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*|[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\_[\w]+[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`\_[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)|\`[\w]+\`[\s]+CHAR[\s]*\(\s*[\d]+\s*\)\s*\)",fileLine,flags=re.I)
                    for item in find_char:
                        item=re.sub(r"\)\)",")",item,flags=re.I)
                        char_varchar_datatype.append(item.upper())
                        #print("char_varchar_datatype :", char_varchar_datatype) 

                    find_decimal=re.findall(r"[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\_[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\`\_[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\)"
                                            r"|[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\_[\w]+[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\`\_[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)\,"
                                            r"|\`[\w]+\`[\s]+DECIMAL[\s]*\(\s*[\d]+\s*\,[\d]+\)",fileLine,flags=re.I)
                   
                    for ele in find_decimal:
                        item=re.sub(r"\)\)",")",ele,flags=re.I)
                        decimal_column.append(item.upper())
                        # print("decimal_column:", decimal_column)



                    find_STRING =re.findall(r"[\w]+[\s]+STRING\)*[\s]+|\_[\w]+[\s]+STRING\)*[\s]+|\`\_[\w]+\`[\s]+STRING\)*[\s]+|\`[\w]+\`[\s]+STRING\)*[\s]+|[\w]+[\s]+STRING\,|\_[\w]+[\s]+STRING\,|\_[\w]+[\s]+STRING|\`\_[\w]+\`[\s]+STRING\,|\`\_[\w]+\`[\s]+STRING|\`[\w]+\`[\s]+STRING\,|\`[\w]+\`[\s]+STRING\,"
                                            r"|[\w]+[\s]+void\)*[\s]+|\_[\w]+[\s]+void\)*[\s]+|\`\_[\w]+\`[\s]+void\)*[\s]+|\`[\w]+\`[\s]+void\)*[\s]+|[\w]+[\s]+void\,|\_[\w]+[\s]+void\,|\_[\w]+[\s]+void|\`\_[\w]+\`[\s]+void\,|\`\_[\w]+\`[\s]+void|\`[\w]+\`[\s]+void\,|\`[\w]+\`[\s]+void\,",fileLine,flags=re.I)
                    # print("find_STRING:", find_STRING)
                    for ele in find_STRING:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        ele=re.sub(r"\,","",ele,flags=re.I)
                        string_datatype.append(ele.upper())
                        # print("string_datatype:", string_datatype)

                    find_Date=re.findall(r"[\w]+[\s]+DATE\)*[\s]+|\_[\w]+[\s]+DATE\)*[\s]+|\`\_[\w]+\`[\s]+DATE\)*[\s]+|\`[\w]+\`[\s]+DATE\)*[\s]+|[\w]+[\s]+DATE\,|\_[\w]+[\s]+DATE\,|\_[\w]+[\s]+DATE|\`\_[\w]+\`[\s]+DATE\,|\`\_[\w]+\`[\s]+DATE|\`[\w]+\`[\s]+DATE\,|\`[\w]+\`[\s]+DATE\,",fileLine,flags=re.I)
                    for ele in find_Date:
                        ele=re.sub(r"\)","",ele,flags=re.I)
                        date_column.append(ele.upper())
                        # print("date_column", date_column)

                    find_timestamp=re.findall(r"[\w]+[\s]+TIMESTAMP\)*[\s]+|\_[\w]+[\s]+TIMESTAMP\)*[\s]+|\`\_[\w]+\`[\s]+TIMESTAMP\)*[\s]+|\`[\w]+\`[\s]+TIMESTAMP\)*[\s]+|[\w]+[\s]+TIMESTAMP\,|\_[\w]+[\s]+TIMESTAMP\,|\_[\w]+[\s]+TIMESTAMP|\`\_[\w]+\`[\s]+TIMESTAMP\,|\`\_[\w]+\`[\s]+TIMESTAMP|\`[\w]+\`[\s]+TIMESTAMP\,|\`[\w]+\`[\s]+TIMESTAMP\,",fileLine,flags=re.I)
                    for ele in find_timestamp:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        timestamp_column.append(ele.upper())
                        # print("timestamp_column", timestamp_column)

                    find_boolean=re.findall(r"[\w]+[\s]+BOOLEAN\)*[\s]+|\_[\w]+[\s]+BOOLEAN\)*[\s]+|\`\_[\w]+\`[\s]+BOOLEAN\)*[\s]+|\`[\w]+\`[\s]+BOOLEAN\)*[\s]+|[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN|\`\_[\w]+\`[\s]+BOOLEAN\,|\`\_[\w]+\`[\s]+BOOLEAN|\`[\w]+\`[\s]+BOOLEAN\,|\`[\w]+\`[\s]+BOOLEAN\,",fileLine,flags=re.I)
                    for ele in find_boolean:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        boolean_column.append(ele.upper())
                        # print("boolean_column", boolean_column)

                    find_binary=re.findall(r"[\w]+[\s]+BINARY\)*[\s]+|\_[\w]+[\s]+BINARY\)*[\s]+|\`\_[\w]+\`[\s]+BINARY\)*[\s]+|\`[\w]+\`[\s]+BINARY\)*[\s]+|[\w]+[\s]+BINARY\,|\_[\w]+[\s]+BINARY\,|\_[\w]+[\s]+BINARY|\`\_[\w]+\`[\s]+BINARY\,|\`\_[\w]+\`[\s]+BINARY|\`[\w]+\`[\s]+BINARY\,|\`[\w]+\`[\s]+BINARY\,",fileLine,flags=re.I)
                    for ele in find_binary:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        binary_column.append(ele.upper())
                        # print("binary_column", binary_column)

                    find_array=re.findall(r"[\w]+[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\)*[\s]+|[\w]+[\s]+ARRAY[\s]*\<STRING\>\,|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>\,|\_[\w]+[\s]+ARRAY[\s]*\<STRING\>|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,|\`\_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\,",fileLine,flags=re.I)
                    for ele in find_array:
                        ele=re.sub(r"\)",",",ele,flags=re.I)
                        array_column.append(ele.upper())
                        # print("find_array", find_array)

            for ele in integer_column:
                replce_integer=re.sub(r"[\s]+INT[\s]*|[\s]+BIGINT[\s]*|[\s]+SMALLINT[\s]*|[\s]+TINYINT[\s]*","INT64",ele,flags=re.I)
                replce_integer=re.sub(r"[\s]+|\,","",replce_integer,flags=re.I)
                replce_integer=re.sub(r"\`","",replce_integer,flags=re.I)

                #print("replce_integer",replce_integer)
                if replce_integer.lower()=="andint64" or replce_integer.lower()=="asint64" or replce_integer=="":
                    pass
                else:
                    input_all_datatype.append(replce_integer.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in float_column:
                replce_float=re.sub(r"[\s]+FLOAT|[\s]+DOUBLE","FLOAT64",ele,flags=re.I)
                replce_float=re.sub(r"[\s]+|\,","",replce_float,flags=re.I)
                replce_float=re.sub(r"\`","",replce_float,flags=re.I)

                # print("replce_float",replce_float)
                if replce_float.lower()=="andfloat64" or replce_float.lower()=="asfloat64" or replce_float=="":
                    pass
                else:
                    input_all_datatype.append(replce_float.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in char_varchar_datatype:
                rplce_char_to_string=re.sub(r"[\s]+CHAR|[\s]+VARCHAR"," STRING",ele,flags=re.I)
                rplce_char_to_string=re.sub(r"[\s]+","",rplce_char_to_string,flags=re.I)
                rplce_char_to_string=re.sub(r"\,","",rplce_char_to_string,flags=re.I)
                rplce_char_to_string=re.sub(r"\`","",rplce_char_to_string,flags=re.I)

                #print("rplce_char_to_string",rplce_char_to_string)
                if rplce_char_to_string.lower() == "asstring" or rplce_char_to_string.lower() == "andstring":
                    pass
                else:
                    BQ_input_cloumn.append(ele.lower())
                    input_all_datatype.append(rplce_char_to_string.lower())

            for ele in string_datatype:
                string_to_STRING=re.sub(r"[\s]+string,","STRING",ele,flags=re.I)
                string_to_STRING=re.sub(r"[\s]+void","STRING",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"[\s]+","",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r"\`","",string_to_STRING,flags=re.I)
                string_to_STRING=re.sub(r",","",string_to_STRING,flags=re.I)

                if string_to_STRING.lower() == "asstring" or string_to_STRING.lower() == "andstring" or "ofstring" in string_to_STRING.lower():
                    pass
                elif string_to_STRING.lower() == "asvoid" or string_to_STRING.lower() == "andvoid" or "ofvoid" in string_to_STRING.lower():
                    pass
                else:
                    BQ_input_cloumn.append(ele.lower())
                    input_all_datatype.append(string_to_STRING.lower())

            for ele in decimal_column:
                if "as decimal" in ele.lower() or "as dec" in ele.lower() or "as numeric" in ele.lower():
                    pass
                else:
                    find_column_value=re.findall(r"[\d]+\,[\d]+",ele.upper())
                    if find_column_value:
                        find_column_value="".join(find_column_value)
                        find_column_value=find_column_value.split(",")
                        if int(find_column_value[0])>=38 or int(find_column_value[1])>=9:
                            replce_decimal=re.sub("[\s]+DECIMAL"," BIGNUMERIC",ele,flags=re.I)
                            replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)

                            BQ_input_cloumn.append(ele.lower())
                            input_all_datatype.append(replce_decimal.lower())
                        else:
                            replce_decimal=re.sub("[\s]+DECIMAL"," NUMERIC",ele,flags=re.I)
                            replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                            replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)
                            BQ_input_cloumn.append(ele.lower())
                            input_all_datatype.append(replce_decimal.lower())
                    else:
                        replce_decimal=re.sub("[\s]+DECIMAL"," NUMERIC",ele,flags=re.I)
                        replce_decimal=re.sub(r"[\s]+","",replce_decimal,flags=re.I)
                        replce_decimal=re.sub(r"\`","",replce_decimal,flags=re.I)
                        replce_decimal=re.sub(r"\)\,",")",replce_decimal,flags=re.I)
                        BQ_input_cloumn.append(ele.lower())
                        input_all_datatype.append(replce_decimal.lower())

            for ele in date_column:
                replce_date=re.sub(r"[\s]+DATE","DATE",ele,flags=re.I)
                replce_date=re.sub(r"[\s]+","",replce_date,flags=re.I)
                replce_date=re.sub(r"\`","",replce_date,flags=re.I)
                replce_date=re.sub(r"\,","",replce_date,flags=re.I)

                if replce_date.lower()=="anddate" or replce_date.lower()=="asdate" or replce_date==""  or "elsedate" in replce_date.lower():
                    pass
                else:
                    input_all_datatype.append(replce_date.lower())
                    BQ_input_cloumn.append(ele.lower())
                    # print(input_all_datatype)

            if(fkt_check_timestamp_to_datetime=='N' or fkt_check_timestamp_to_datetime==''):
                for ele in timestamp_column:
                    replce_timestamp=re.sub(r"[\s]+TIMESTAMP","TIMESTAMP",ele,flags=re.I)
                    replce_timestamp=re.sub(r"[\s]+","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\`","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\,","",replce_timestamp,flags=re.I)

                    # print("replce_timestamp:  ", replce_timestamp)
                    if replce_timestamp.lower()=="andtimestamp" or replce_timestamp.lower()=="astimestamp" or replce_timestamp=="":
                        pass
                    else:
                        input_all_datatype.append(replce_timestamp.lower())
                        BQ_input_cloumn.append(ele.lower())
            else:
                for ele in timestamp_column:
                    replce_timestamp=re.sub(r"[\s]+TIMESTAMP","DATETIME",ele,flags=re.I)
                    replce_timestamp=re.sub(r"[\s]+","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\`","",replce_timestamp,flags=re.I)
                    replce_timestamp=re.sub(r"\,","",replce_timestamp,flags=re.I)

                    # print("replce_timestamp:  ", replce_timestamp)
                    if replce_timestamp.lower()=="anddatetime" or replce_timestamp.lower()=="asdatetime" or replce_timestamp=="":
                        pass
                    else:
                        input_all_datatype.append(replce_timestamp.lower())
                        BQ_input_cloumn.append(ele.lower())


            for ele in boolean_column:
                replce_boolean=re.sub(r"[\s]+BOOLEAN","BOOL",ele,flags=re.I)
                replce_boolean=re.sub(r"[\s]+","",replce_boolean,flags=re.I)
                replce_boolean=re.sub(r"\`","",replce_boolean,flags=re.I)
                replce_boolean=re.sub(r"\,","",replce_boolean,flags=re.I)

                # print("replce_boolean:  ", replce_boolean)
                if replce_boolean.lower()=="andboolean" or replce_boolean.lower()=="asboolean" or replce_boolean=="":
                    pass
                else:
                    input_all_datatype.append(replce_boolean.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in binary_column:
                replce_binary=re.sub(r"[\s]+BINARY","BYTES",ele,flags=re.I)
                replce_binary=re.sub(r"[\s]+","",replce_binary,flags=re.I)
                replce_binary=re.sub(r"\`","",replce_binary,flags=re.I)
                replce_binary=re.sub(r"\,","",replce_binary,flags=re.I)
                # print("replce_binary:  ", replce_binary)
                if replce_binary.lower()=="andbinary" or replce_binary.lower()=="asbinary" or replce_binary=="":
                    pass
                else:
                    input_all_datatype.append(replce_binary.lower())
                    BQ_input_cloumn.append(ele.lower())

            for ele in array_column:
                replce_array=re.sub(r"[\s]+STRING","STRING",ele,flags=re.I)
                replce_array=re.sub(r"[\s]+","",replce_array,flags=re.I)
                replce_array=re.sub(r"\`","",replce_array,flags=re.I)
                replce_array=re.sub(r"\,","",replce_array,flags=re.I)
                # print("replce_binary:  ", replce_binary)
                if replce_array.lower()=="andarray<string>" or replce_array.lower()=="asarray<string>" or replce_array=="":
                    pass
                else:
                    input_all_datatype.append(replce_array.lower())
                    BQ_input_cloumn.append(ele.lower())

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileobj = f.read()
            fileobj=re.sub(r'[\s]*\)', ' )', fileobj, flags=re.DOTALL)

            # print(fileobj)
            find_date_function = re.findall(r"PARTITION[\s]+BY[\s]+DATE\(|PARTITION[\s]+BY[\s]+DATETIME_TRUNC\(", fileobj, flags=re.I|re.DOTALL)
            # print(find_date_function)

            find_struct=re.findall(r'[\w]+[\s]+STRUCT[\s]*\<.*\>',fileobj,flags=re.I|re.DOTALL)
            # print(find_struct)
            if find_struct:
                BQ_output_cloumn.append(find_struct[0])
                struct_obj=re.sub("[\s]+|\`","",find_struct[0],flags=re.I|re.DOTALL)
                struct_obj=re.sub("OPTIONS\(description.*","",struct_obj,flags=re.I|re.DOTALL)
                output_all_datatype.append(re.sub("[\s]+","",struct_obj,flags=re.I).lower())
                # print("s_output_all_datatype", output_all_datatype)
                fileobj=fileobj.replace(find_struct[0],"")
            fileLines=fileobj.split('\n')

            for fileLine in fileLines:
                fileLine =re.sub(r"(OPTIONS\(DESCRIPTION\s*\=)*\s*\'(.*?)\'\)","",fileLine,flags=re.DOTALL|re.I)
                fileLine = re.sub(r"array[\s]*\<STRUCT\<(.*?)\>","",fileLine,flags=re.I|re.DOTALL)  #array\<(.*?)\>
                fileLine = re.sub(r"struct\<(.*?)\>\>|struct\<(.*?)\>","",fileLine,flags=re.I|re.DOTALL)
                fileLine = re.sub(r"STRUCT[\s]*<(.*?)\>","",fileLine,flags=re.I|re.DOTALL)
                # print("array_lineo_p:", fileLine)

                find_integer1=re.findall(r"[\w]+[\s]+INT64[\s]*|[\w]+[\s]+INT64[\s]*\)",fileLine,flags=re.I)
                # print("find_integer1",find_integer1)
                for item in find_integer1:
                    item = re.sub(r"\)",",",item,flags=re.I)
                    item = re.sub(r"\'","",item,flags=re.I)
                    BQ_output_cloumn.append(item)
                    remove_space=re.sub(r"[\s]+|\,","",item,flags=re.I)
                    output_all_datatype.append(remove_space.lower())
                    # print(len(output_all_datatype), output_all_datatype)

                find_float=re.findall(r"[\w]+[\s]+FLOAT64[\s]*|[\w]+[\s]+FLOAT64[\s]*\)",fileLine,flags=re.I)
                for item in find_float:
                    item = re.sub(r"\)",",",item,flags=re.I)
                    item = re.sub(r"\'","",item,flags=re.I)
                    BQ_output_cloumn.append(item)
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    output_all_datatype.append(remove_space.lower())
                    # print(len(output_all_datatype))

                find_char=re.findall(r"[\w]+[\s]+STRING[\s]*\([\s]*[\d]+[\s]*\)|[\w]+[\s]+string[\s]*|_[\w]+[\s]+string[\s]*|\`_[\w]+\`[\s]+STRING[\s]*|[\w]+[\s]+STRING|[\w]+[\s]+STRING\([\d]+\)\)|[\w]+[\s]+STRING[\s]+|[\w]+[\s]+STRING\)", fileLine, flags=re.I)
                # print("find_char", find_char)
                # print("fileLine", fileLine)
                for item in find_char:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"string\s*\)","string",remove_space,flags=re.I)
                    remove_space=re.sub(r"\)\)",")",remove_space,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)
                    # print("remove_space",remove_space)
                    if remove_space.lower() == "asstring" or remove_space.lower() == "andstring" or remove_space.lower() == "bystring" or "ofstring" in remove_space.lower():
                        pass
                    else:
                        item = re.sub(r"\)\)",")",item,flags=re.I)
                        item = re.sub(r"string\s*\)","string,",item,flags=re.I)
                        item = re.sub(r"\'","",item,flags=re.I)

                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())

                find_decimal=re.findall(r"[\w]+[\s]+NUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+NUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)"
                                        r"|[\w]+[\s]+BIGNUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+BIGNUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)",fileLine,flags=re.I)
               
                for item in find_decimal:
                    remove_space=re.sub(r"\)\)",")",item,flags=re.I)
                    remove_space=re.sub(r"[\s]","",remove_space,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)

                    # print("remove_space",remove_space)
                    if "asnumeric" in remove_space.lower() or "bybignumeric" in remove_space.lower() or "asbignumeric" in remove_space.lower():
                        pass
                    else:
                        item = re.sub(r"\)\)",")",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())


                if(fkt_check_timestamp_to_datetime=='N' or fkt_check_timestamp_to_datetime==''):
                    find_timestamp1=re.findall(r"[\w]+[\s]+TIMESTAMP[\s]*|[\w]+[\s]+TIMESTAMP[\s]*\)",fileLine,flags=re.I)
                    #find_timestamp1=re.sub(r"\)",",",find_timestamp1,flags=re.I)
                    for item in find_timestamp1:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                        # print("timestampo/p :" , remove_space)
                        if remove_space.lower() == "astimestamp" or remove_space.lower()=="bytimestamp" or remove_space.lower()=="andtimestamp" :
                            pass
                        else:
                            item = re.sub(r"\)",",",item,flags=re.I)
                            item=re.sub(r"\`","",item,flags=re.I)
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())
                            #print("output_all_datatype :" , output_all_datatype)
                else:
                    find_datetime=re.findall(r"[\w]+[\s]+DATETIME[\s]*|[\w]+[\s]+DATETIME[\s]*\)",fileLine,flags=re.I)
                    # print("find_datetime", find_datetime)
                    #find_timestamp1=re.sub(r"\)",",",find_timestamp1,flags=re.I)
                    for item in find_datetime:
                        remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                        remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                        # print("dateaertyll :" , remove_space)
                        if remove_space.lower() == "asdatetime" or remove_space.lower()=="bydatetime" or remove_space.lower()=="anddatetime" :
                            pass
                        else:
                            item = re.sub(r"\)",",",item,flags=re.I)
                            item=re.sub(r"\`","",item,flags=re.I)
                            BQ_output_cloumn.append(item)
                            output_all_datatype.append(remove_space.lower())

                find_boolean1=re.findall(r"[\w]+[\s]+BOOL[\s]*|[\w]+[\s]+BOOL[\s]*\)",fileLine,flags=re.I)
                #find_boolean1=re.sub(r"\)",",",find_boolean1,flags=re.I)
                for item in find_boolean1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    # print("dateaertyll :" , remove_space)
                    if remove_space.lower() == "asbool" or remove_space.lower()=="bybool" or remove_space.lower()=="andbool" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())
                        #print("output_all_datatype :" , output_all_datatype)

                find_binary1=re.findall(r"[\w]+[\s]+BYTES[\s]*|[\w]+[\s]+BYTES[\s]*\)",fileLine,flags=re.I)
                #find_binary1=re.sub(r"\)",",",find_binary1,flags=re.I)
                for item in find_binary1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    # print("dateaertyll :" , remove_space)
                    if remove_space.lower() == "asbytes" or remove_space.lower()=="andbytes" or remove_space.lower()=="bybytes" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())
                        #print("output_all_datatype :" , output_all_datatype)

                find_array1=re.findall(r"[\w]+[\s]+ARRAY[\s]*\<STRING\>\([\d]+\)|[\w]+[\s]+ARRAY[\s]*\<STRING\>[\s]*|_[\w]+[\s]+ARRAY[\s]*\<STRING\>[\s]*|\`_[\w]+\`[\s]+ARRAY[\s]*\<STRING\>[\s]*|[\w]+[\s]+ARRAY[\s]*\<STRING\>|\`[\w]+\`[\s]+ARRAY[\s]*\<STRING\>\)|[\w]+[\s]+ARRAY[\s]*\<STRING\>[\s]+|[\w]+[\s]+ARRAY[\s]*\<STRING\>\)",fileLine,flags=re.I)
                #find_binary1=re.sub(r"\)",",",find_binary1,flags=re.I)
                for item in find_array1:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space=re.sub(r"\`","",remove_space,flags=re.I)
                    # print("dateaertyll :" , remove_space)
                    if remove_space.lower() == "asarray<string>" or remove_space.lower()=="andarray<string>" or remove_space.lower()=="byarray<string>" :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item=re.sub(r"\`","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())
                        #print("output_all_datatype :" , output_all_datatype)

                find_date=re.findall(r"[\w]+[\s]+DATE[\s]*\,|[\w]+[\s]+DATE[\s]*\)",fileLine,flags=re.I)   
                for item in find_date:
                    remove_space=re.sub(r"[\s]+","",item,flags=re.I)
                    remove_space = re.sub(r"\'","",remove_space,flags=re.I)
                    remove_space = re.sub(r"\,|\)","",remove_space,flags=re.I)

                    if remove_space.lower() == "asdate" or remove_space.lower()=="anddate" or remove_space.lower()=="bydate" or "elsedate" in remove_space.lower() :
                        pass
                    else:
                        item = re.sub(r"\)",",",item,flags=re.I)
                        item = re.sub(r"\'","",item,flags=re.I)
                        BQ_output_cloumn.append(item)
                        output_all_datatype.append(remove_space.lower())


        

            input_all_datatype.sort()
            output_all_datatype.sort()
            BQ_input_cloumn.sort()
            BQ_output_cloumn.sort()
            if BQ_input_cloumn:
                val1 =  BQ_input_cloumn[-1]
                BQ_input_cloumn.pop()
                newval1 = val1.replace(",","")
                BQ_input_cloumn.append(newval1)
            if BQ_output_cloumn:
                val2 = BQ_output_cloumn[-1]
                BQ_output_cloumn.pop()
                newval2 = val2.replace(",","")
                BQ_output_cloumn.append(newval2)

    for ele in input_all_datatype:
        # print("ele", ele)
        # ele = ele.replace('d_', '')
        ele = re.sub(r"^d_", "", ele)
        # print("ele", ele)
        input_all_datatype1.append(ele)
    for ele in output_all_datatype:
        # ele = ele.replace('d_', '')
        ele = re.sub(r"^d_", "", ele)
        output_all_datatype1.append(ele)

    input_all_datatype1.sort()
    output_all_datatype1.sort()

    # print("input_list =",len(set(input_all_datatype1)),input_all_datatype1)
    # print("output_list=",len(set(output_all_datatype1)),output_all_datatype1)
    # print("BQ_input_cloumn",BQ_input_cloumn)
    # print("BQ_output_cloumn",BQ_output_cloumn)
    # print(len(input_all_datatype))
    # print(len(output_all_datatype))

    if len(input_all_datatype1)==0 and len(output_all_datatype1) == 0:
        Datatype_result_for_HV.append("NA")
        log_result.append(Result_NA())
    elif list(set(input_all_datatype1))==list(set(output_all_datatype1)):
        # print("YYYY")
        Datatype_result_for_HV.append("Y")
        log_result.append(Result_Yes(BQ_input_cloumn,BQ_output_cloumn))
    
    elif len(input_all_datatype1) > len(output_all_datatype1) or find_date_function:
        Datatype_result_for_HV.append("CM")
        log_result.append(Result_CM("check Output file DATE Funaction."))
    else:
        Datatype_result_for_HV.append("N")
        log_result.append(Result_No(BQ_input_cloumn,BQ_output_cloumn))

def fkt_check_struct_datatype(log_result,filename,result_struct_datatype):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Struct Datatype"))
    input_struct_list = []
    output_struct_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            file_obj = common_obj.Read_file(inputFolderPath+filename)

            find_struct = re.findall(r"\`*[\w]+\`*[\s]+STRUCT[\s]*\<", file_obj, flags=re.I|re.DOTALL)
            # print(find_struct)
            if find_struct:
                find_struct_obj = re.findall(r"STRUCT[\s]*\<.*?\>", file_obj, flags=re.I|re.DOTALL)
                # print(find_struct_obj)
                if find_struct_obj:
                    for ele in find_struct_obj:
                        # print(ele)
                        ele = ele.replace(":", " ")
                        ele = ele.replace("`", "")
                        ele = re.sub("\,", " , ", ele, flags=re.I)
                        ele = re.sub("\>", " > ", ele, flags=re.I)
                        ele = re.sub("\<", " < ", ele, flags=re.I)
                        ele = re.sub(" BIGINT | SMALLINT | TINYINT | INT ","INT64", ele, flags=re.I)
                        ele = re.sub(" FLOAT | DOUBLE "," FLOAT64 ", ele, flags=re.I)
                        ele = re.sub(" VARCHAR | CHAR "," STRING ", ele, flags=re.I)
                        ele = re.sub(" STRING | void "," STRING ", ele, flags=re.I)
                        ele = re.sub(" BOOLEAN "," BOOL", ele, flags=re.I)
                        ele = re.sub(" BINARY ","BYTES", ele, flags=re.I)
                        ele = re.sub(" DATE "," DATE ", ele, flags=re.I)  
                        ele = re.sub("\< timestamp \>","< DATETIME >", ele, flags=re.I)   #added 23 feb
                        # print(ele)
                        ele = re.sub("\,[\s]*$", "", ele, flags=re.I)
                        find_cmt = re.findall(r"COMMENT[\s]*\'(.*)\'", ele, flags=re.I|re.DOTALL)
                        if find_cmt:
                            ele = re.sub("COMMENT[\s]*\'.*", "", ele, flags=re.I)
                        input_struct_list.append(re.sub("[\s]+","",ele,flags=re.I).lower())
                        Log_input.append(re.sub("[\s]+","",ele,flags=re.I).lower())



        # print(input_struct_list)

        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            find_struct = re.findall(r"\`*[\w]+\`*[\s]+STRUCT[\s]*\<", file_obj, flags=re.I|re.DOTALL)
            # print(find_struct)
            if find_struct:
                find_struct_obj = re.findall(r"STRUCT[\s]*\<.*?\>", file_obj, flags=re.I|re.DOTALL)
                # print(find_struct_obj)
                if find_struct_obj:
                    for ele in find_struct_obj:
                        # print(ele)
                        ele = re.sub("[\s]+", "", ele, flags=re.I)
                        ele = ele.replace("`", "")
                        # print(ele)
                        output_struct_list.append(ele.lower())
                        Log_output.append(ele.lower())

        # print("input_struct_list =", input_struct_list)
        # print("output_struct_list=", output_struct_list)

        if len(input_struct_list)==0 and len(output_struct_list)==0:
            log_result.append(Result_NA())
            result_struct_datatype.append("NA")
        elif list(set(input_struct_list)) == list(set(output_struct_list)):
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_struct_datatype.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_struct_datatype.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_struct_datatype.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_check_comment_to_option_for_HV(log_result,filename,comment_to_option_result_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Comment to option conversion check"))
    output_file_actual_option=[]
    Hive_comment_list_final=[]
    hive_output_cloumn=[]
    string_datatype=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            Hive_comment_list_raw=re.findall(r"(CO.{3}NT)+\s*\'(.*?)\'",query,flags=re.I)
            for k in Hive_comment_list_raw:
                a, b = k
                b = "'"+b+"'"
                c = a, b
                listq = ' '.join(c)
                Hive_comment_list_final.append(listq)
            Hive_comment_list_final.sort()
            # print(Hive_comment_list_final)
            input_file_comment_to_option= ["OPTIONS(DESCRIPTION =" + "'"  + i[1] + "'"+")"  for i in Hive_comment_list_raw]
            # print("input_file_comment_to_option:", input_file_comment_to_option)

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            find_option=re.findall(r"(OPTIONS\(DESCRIPTION\s*\=)*\s*\'(.*?)\'\)",fileLine,flags=re.I)
            # print("find_option",find_option)
            for z in find_option:
                x, y = z
                y = "'"+y+"'"+")"
                d = x, y
                listq = ''.join(d)
                # print("listq" , listq)

                output_file_actual_option.append(listq) #removed lower()

            Hive_comment_list_final.sort()
            input_file_comment_to_option.sort()
            output_file_actual_option.sort()

    # print("After check")
    # print("input_file_comment_to_option",input_file_comment_to_option)
    # print("output_file_actual_option",output_file_actual_option)

    # print(len(input_file_comment_to_option))
    # print(len(output_file_actual_option))

    if len(input_file_comment_to_option)==0 and len(output_file_actual_option) == 0:
        comment_to_option_result_for_HV.append("NA")
        log_result.append(Result_NA())

    elif len(input_file_comment_to_option)==len(output_file_actual_option):
        # print("COMMENT FILES SUCCESSFUL")
        comment_to_option_result_for_HV.append("Y")
        log_result.append(Result_Yes(Hive_comment_list_final,output_file_actual_option))
    else:
        comment_to_option_result_for_HV.append("N")
        # print("COMMENT FILES unSUCCESSFUL")
        log_result.append(Result_No(Hive_comment_list_final,output_file_actual_option))

def fkt_check_drop_raw_format_or_location_for_HV(log_result,filename,drop_row_format_or_location_result_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Drop_raw_format_location_from_hv"))
    drop_row_format = []
    find_option = []
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            drop_row_format=re.findall(r"(STORED[\s]+AS[\s]+ORC[\s]+[\w]+)[\s]*\'*(.*?).\;",query,flags=re.I|re.DOTALL)
           
            if not drop_row_format:
                drop_row_format=re.findall(r"(LOCATION)+[\s]*\'(.*?).\;",query,flags=re.DOTALL)

    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            find_option=re.findall(r"(ROW[\s]+FORMAT[\s]+[\w]+)[\s]*\'(.*?).\;",fileLine,flags=re.I|re.DOTALL)
        
            if not find_option:
                find_option=re.findall(r"(LOCATION)+[\s]*\'(.*?).\;",fileLine,flags=re.DOTALL)


    if len(drop_row_format)== 0 and not find_option:
        drop_row_format_or_location_result_for_HV.append("NA")
        log_result.append(Result_NA())
    elif drop_row_format and not find_option:
        drop_row_format_or_location_result_for_HV.append("Y")
        log_result.append(Result_Yes(list(drop_row_format[0]),find_option))
    else:
        drop_row_format_or_location_result_for_HV.append("N")
        log_result.append(Result_No(drop_row_format[0],find_option[0]))

def fkt_check_create_table_external_for_HV(log_result,filename,create_table_external_for_HV):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Create Table To External Table Checking"))
    input_create_table_column=[]
    bq_equivalent_conversion=[]
    Bq_output_file_create_table=[]

    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            f.close()
            for fileLine in fileLines:
                if fileLine.startswith("--"):
                    pass
                else:
                    find_create_table=re.findall(r"CREATE[\s]+TABLE[\s]+|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|CREATE[\s]+EXTERNAL[\s]+TABLE|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS",fileLine,flags=re.I)
                    # print("find_create_table",find_create_table)
                    for ele in find_create_table:
                        # ele=re.sub(r"[\s]+","",ele,flags=re.I)
                        input_create_table_column.append(ele.upper())
                        #print("input_create_table_column",input_create_table_column)

            for ele in input_create_table_column:
                replce_table=re.sub(r"CREATE[\s]+EXTERNAL[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS|CREATE[\s]+EXTERNAL[\s]+TABLE|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS","CREATE TABLE IF NOT EXISTS",ele,flags=re.I)
                # replce_table=re.sub(r"[\s]+","",ele,flags=re.I)
                #print("replce_table",replce_table)
                bq_equivalent_conversion.append(replce_table)



    if filename in outputFile:
        with open(OutputFolderPath+filename, "r") as f:
            fileLine = f.read()
            f.close()
            Bq_create_table=re.findall(r"CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS",fileLine,flags=re.I)
            # Bq_create_table=re.sub(r"[\s]+","",fileLine,flags=re.I)
            Bq_output_file_create_table = Bq_create_table

    input_create_table_column.sort()
    Bq_output_file_create_table.sort()
    bq_equivalent_conversion.sort()

    # print("input_create_table_column", input_create_table_column)
    # print("Bq_output_file_create_table", Bq_output_file_create_table)
    # print("bq_equivalent_conversion", bq_equivalent_conversion)


    if len(bq_equivalent_conversion)==0 and len(Bq_output_file_create_table) == 0:
        create_table_external_for_HV.append("NA")
        log_result.append(Result_NA())

    elif len(bq_equivalent_conversion)==len(Bq_output_file_create_table):
        # print("check for create statement check has been done")
        create_table_external_for_HV.append("Y")
        log_result.append(Result_Yes(input_create_table_column,Bq_output_file_create_table))
    else:
        create_table_external_for_HV.append("N")
        log_result.append(Result_No(input_create_table_column,Bq_output_file_create_table))

def fkt_No_Of_Statement_In_Script(log_result,filename,Result_No_Of_Statement,fun_project_ID):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Number of Statements in hive-BQ Script"))
    with_Append_project_id_Final_input_sql_statements=[]
    without_Append_project_id_Final_input_sql_statements=[]
    with_Append_project_id_Final_output_sql_statements=[]
    without_Append_project_id_Final_output_sql_statements=[]
    all_input_file_statements=[]
    all_output_file_statements=[]

    # print("inside check for statement")

    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as f:
            fileLines = f.readlines()
            counter=0
            comment=0
            obj=''
            fileLines.append(";")
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
                        #print(fileLine)
                        comment=0
                        pass
                    else:
                        pass
                elif fileLine.startswith("--") or "--" in fileLine or ".if activitycount" in fileLine.lower():
                    pass
                else:
                    if counter==0:

                        obj=''
                        # print("file line",fileLine )
                        #Search pattern in all input file for no of statements
                        input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE|CREATE[\s]+EXTERNAL[\s]+TABLE|WITH[\s]+CTE[\s]+|CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+|SELECT[\s]+|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+TABLE[\s]*IF[\s]*EXISTS[\s]*|MERGE[\s]+",fileLine,flags=re.I)
                        # print("input statement for ssql 1:",input_sql_statements)
                        if input_sql_statements:
                            # print("inside input sql statements",input_sql_statements)
                            if input_sql_statements[0].upper() in fileLine.upper():
                                counter=1
                                obj=obj+fileLine
                                if "--" in fileLine and ";" in fileLine and "';'" not in fileLine:
                                    x=fileLine.split("--")
                                    if ";" in x[0]:
                                        #print(obj)
                                        counter=0
                                elif ";" in fileLine and "';'" not in fileLine:
                                    # print("qr",obj)
                                    counter=0
                                    input_sql_statements=re.findall(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]\`+|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\`\.\`[\w]+\`|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+\.[\w]|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+[\w]+\.[\w]+"
                                                                    r"|^WITH[\s]+CTE[\s]+[\w]+[\s]+|CREATE[\s]+TEMPORARY[\s]+"
                                                                    r"|CREATE[\s]+VIEW[\s]*\`[\w]+\.[\w]+\`+"  #|TABLE[\s]*[\w]+[\s]* removed for RWR
                                                                    r"|REPLACE[\s]+VIEW[\s]*\`[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]+\`[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]*INTO[\s]*[\w]+\.[\w]+"
                                                                    r"|UPDATE[\s]*\`[\w]+\.[\w]+\`+|UPDATE[\s]+[\w]+[\s]+"
                                                                    r"|DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;|DROP[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+[\s]*"
                                                                    r"|MERGE[\s]+\`[\w]+\.[\w]+\`+|CREATE[\s]+SET[\s]+TABLE[\s]+\`[\w]+\.[\w]+\`+"
                                                                    ,obj,flags=re.I)
                                    # print("input_sql_statements111:-", input_sql_statements)
                                    # print("check for input statement no 1")
                                    # If project ID is present enter into this block
                                    if fun_project_ID == "Y":
                                        # print("hehe hehehe ")
                                        obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                        if obj.upper().startswith("SELECT "):
                                            find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                            if find_select:
                                                find_select=re.sub(r"[\s]+","",find_select[0])
                                                with_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                            all_input_file_statements.append(obj.upper())
                                        for item in input_sql_statements:
                                            # print("To check here")
                                            # print("inside_inputsql:", item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            all_input_file_statements.append(remove_space.upper())
                                            item=re.sub(r"\n","",item)

                                            item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"drop[\s]+table[\s]+if[\s]+exists","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                                            # print("inside_inputsqldrop:", item)
                                            if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                                            elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                                            if re.findall(r"INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                                item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                            else:
                                                item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                            if re.findall(r"UPDATE[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                                item=re.sub(r"UPDATE","UPDATE `"+Append_project_id+".",item,flags=re.I)
                                            else:
                                                item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                            item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                                            item=re.sub(r"[\s]+|\;","",item)
                                            item=re.sub(r"\.[\s]+",".",item)
                                            with_Append_project_id_Final_input_sql_statements.append(item.upper())
                                    else:

                                        obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I)
                                        # print("I am here hehehe ")

                                        if obj.upper().startswith("SELECT "):
                                            find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I)
                                            #print(find_select)
                                            if find_select:
                                                find_select=re.sub(r"[\s]+","",find_select[0])
                                                without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                            all_input_file_statements.append(obj.upper())
                                        for item in input_sql_statements:
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            all_input_file_statements.append(remove_space.upper())
                                            item=re.sub(r"\n","",item)
                                            item=re.sub(r"CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                            item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                                            item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS ",item,flags=re.I)
                                            if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                                            elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                                item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                                            item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                            item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                            item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                            #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                                            item=re.sub(r"[\s]+|\;","",item)
                                            item=re.sub(r"\.[\s]+",".",item)
                                            item=re.sub(r"\`","",item)
                                            without_Append_project_id_Final_input_sql_statements.append(item.upper())
                    else:
                        if counter==1:
                            if "--" in fileLine and ";" in fileLine and "';'" not in fileLine:
                                x=fileLine.split("--")
                                if ";" in x[0]:
                                    counter=0
                                    obj=obj+fileLine
                                else:
                                    obj=obj+fileLine
                            elif ";" in fileLine and "';'" not in fileLine:
                                counter=0
                                obj=obj+fileLine
                                # print("I am here part 2 ")
                                # print("oobbjj :", obj)
                                input_sql_statements=re.findall(r"INSERT[\s]+INTO[\s]+\`[\w]+\`\.\`[\w]+\`[\s]*|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`[\s]*|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*[\w]+\.[\w]+[\s]*|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+[\s]*NOT[\s]*EXISTS[\s]*\`[\w]+\.[\w]+\`|CREATE[\s]+EXTERNAL[\s]+TABLE[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]*[\w]+\.[\w]+|DROP[\s]*TABLE[\s]*IF[\s]*EXISTS[\s]*[\w]+\.[\w]+",obj,flags=re.I)
                                # print("input_sql_statements::", input_sql_statements)                                
                                if fun_project_ID == "Y":
                                    obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            # ###find_select.append(find_select.upper())
                                        all_input_file_statements.append(obj.upper())
                                    for item in input_sql_statements:
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        all_input_file_statements.append(remove_space.upper())
                                        item=re.sub(r"\n","",item)
                                        item=re.sub(r"\`" ,"",item)
                                        # print("item item", item)
                                        item=re.sub(r"CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]+TABLE","CREATE TABLE IF NOT EXISTS `"+Append_project_id+".",item,flags=re.I)
                                        # print("itemmm",item)
                                        item=re.sub(r"MERGE[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS`"+Append_project_id+".",item,flags=re.I)
                                        if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)
                                        elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM `"+Append_project_id+".",item,flags=re.I)

                                        if re.findall(r"INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        elif re.findall(r"INSERT[\s]+INTO[\s]*[\w]+\.[\w]+",item,flags=re.I):
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO `"+Append_project_id+".",item,flags=re.I)
                                        else:
                                            item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)


                                        if re.findall(r"UPDATE[\s]*\`[\w]+\.[\w]+\`+",item,flags=re.I):
                                            item=re.sub(r"UPDATE","UPDATE `"+Append_project_id+".",item,flags=re.I)
                                        else:
                                            item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                        #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE `"+Append_project_id+".",item,flags=re.I)
                                        item=re.sub(r"[\s]+|\;","",item)
                                        item=re.sub(r"\.[\s]+",".",item)
                                        with_Append_project_id_Final_input_sql_statements.append(item.upper())
                                else:
                                    obj=re.sub("[\s]+SELECT","SELECT ",obj,flags=re.I|re.DOTALL)
                                    #print(obj)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            without_Append_project_id_Final_input_sql_statements.append(find_select.upper())
                                        all_input_file_statements.append(obj.upper())
                                    #without project ID enter into this block
                                    for item in input_sql_statements:
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        all_input_file_statements.append(remove_space.upper())
                                        item=re.sub(r"\n","",item)
                                        # print("cre_item11:-", item)
                                        item=re.sub(r"CREATE[\s]*EXTERNAL[\s]*TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|CREATE[\s]*EXTERNAL[\s]*TABLE[\s]*","CREATE TABLE IF NOT EXISTS ",item,flags=re.I)
                                        # print("cre_item22:-", item)
                                        item=re.sub(r"INSERT[\s]+INTO[\s]+","INSERT INTO ",item,flags=re.I)
                                        item=re.sub(r"MERGE[\s]+","INSERT INTO ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+FUNCTION[\s]+","CREATE OR REPLACE FUNCTION ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+VIEW[\s]+|REPLACE[\s]+VIEW[\s]+","CREATE OR REPLACE VIEW ",item,flags=re.I)
                                        item=re.sub(r"drop[\s]+table","DROP TABLE IF EXISTS ",item,flags=re.I)
                                        if "DELETE FROM" in obj.upper() and "WHERE " in obj.upper() and re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","DELETE FROM ",item,flags=re.I)
                                        elif re.findall(r"DELETE[\s]+FROM[\s]+\`[\w]+\.[\w]+\`+|DELETE[\s]+FROM[\s]+[\w]+[\s]+|DELETE[\s]+FROM[\s]+[\w]+[\s]*\;|DELETE[\s]+[\w]+\.[\w]+|DELETE[\s]+[\w]+[\s]*\;",item,flags=re.I|re.DOTALL):
                                            item=re.sub(r"DELETE[\s]+FROM[\s]+|DELETE[\s]+","TRUNCATE TABLE ",item,flags=re.I)
                                        item=re.sub(r"UPDATE","UPDATE ",item,flags=re.I)
                                        item=re.sub(r"WITH[\s]+CTE[\s]+","WITH ",item,flags=re.I)
                                        item=re.sub(r"CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+","CREATE TEMPORARY TABLE IF NOT EXISTS ",item,flags=re.I)
                                        #item=re.sub(r"CREATE[\s]+PROCEDURE[\s]+|REPLACE[\s]+MACRO[\s]+|CREATE[\s]+MACRO[\s]+|REPLACE[\s]+PROCEDURE[\s]+","CREATE OR REPLACE PROCEDURE ",item,flags=re.I)
                                        item=re.sub(r"[\s]+|\;","",item)
                                        item=re.sub(r"\.[\s]+",".",item)
                                        item=re.sub(r"\`","",item)
                                        without_Append_project_id_Final_input_sql_statements.append(item.upper())

                            else:
                                obj=obj+fileLine

    # print("inside check for statement b4 output")


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
                if fileLine.startswith("/*"):
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
                elif fileLine.startswith("--"):
                    pass
                else:
                    if counter==0:
                        obj=''
                        output_sql_statements=re.findall(r"CALL[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]+|^WITH[\s]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]+|CREATE[\s]+TABLE[\s]*IF[\s]*NOT[\s]*EXISTS[\s]*|INSERT[\s]+INTO[\s]+|UPDATE[\s]+|DELETE[\s]+|DROP[\s]+|TRUNCATE[\s]+TABLE[\s]",fileLine,flags=re.I)
                        # print("output_sql_statements",output_sql_statements)
                        if output_sql_statements:
                            if output_sql_statements[0].upper() in fileLine.upper():
                                counter=1
                                obj=obj+fileLine
                                if "--" in fileLine and ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                    x=fileLine.split("--")
                                    if ";" in x[0]:
                                        counter=0
                                elif ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                    counter=0
                                    if fun_project_ID == "Y":
                                        obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                        obj=re.sub("SELECT[\s]+ERROR","SELECT ERROR",obj,flags=re.I|re.DOTALL)
                                        if "SELECT ERROR" in obj.upper():
                                            pass
                                        else:
                                            if obj.upper().startswith("SELECT "):
                                                find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                                if find_select:
                                                    find_select=re.sub(r"\n|[\s]+","",find_select[0])
                                                    with_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                                all_output_file_statements.append(obj.upper())
                                        output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+\`+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+\`+|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                        # print("check here 2")
                                        for item in output_sql_statements:
                                            item=re.sub(r"\n"," ",item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            remove_space=re.sub(r"\`","",remove_space)
                                            all_output_file_statements.append(remove_space.upper())
                                            item=re.sub(r"[\s]+","",item)
                                            with_Append_project_id_Final_output_sql_statements.append(item.upper())
                                    else:
                                        obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                        obj=re.sub("SELECT[\s]+ERROR","SELECT ERROR",obj,flags=re.I|re.DOTALL)
                                        if "SELECT ERROR" in obj.upper():
                                            pass
                                        else:
                                            # print("check here 3")
                                            if obj.upper().startswith("SELECT "):
                                                find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                                if find_select:
                                                    find_select=re.sub(r"[\s]+","",find_select[0])
                                                    without_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                                all_output_file_statements.append(obj.upper())
                                        output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                        # print("check here 4")

                                        for item in output_sql_statements:
                                            item=re.sub(r"\n","",item)
                                            remove_space=re.sub(r"[\s]+"," ",item)
                                            remove_space=re.sub(r"\`","",remove_space)
                                            all_output_file_statements.append(remove_space.upper())
                                            item=re.sub(r"[\s]+","",item)
                                            without_Append_project_id_Final_output_sql_statements.append(item.upper())

                    else:
                        if counter==1:
                            if "--" in fileLine and ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                x=fileLine.split("--")
                                if ";" in x[0]:
                                    counter=0
                                    obj=obj+fileLine
                                else:
                                    obj=obj+fileLine
                            elif ";" in fileLine and "';'" not in fileLine and "RETURN;" not in fileLine and "RAISE;" not in fileLine and "END;" not in fileLine:
                                counter=0
                                obj=obj+fileLine
                                # print("o/p_obj",obj)
                                if fun_project_ID == "Y":
                                    # print("fun_project_ID",fun_project_ID)
                                    obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            with_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                        all_output_file_statements.append(obj.upper())
                                    output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    r"|INSERT[\s]+INTO[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|UPDATE[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|UPDATE[\s]+[\w]+|DELETE[\s]*FROM[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*\`"+Append_project_id+"\.[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                    # print("op_:", output_sql_statements)
                                    # print("check here 6")

                                    for item in output_sql_statements:
                                        item=re.sub(r"\n"," ",item)
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        remove_space=re.sub(r"\`","",remove_space)
                                        all_output_file_statements.append(remove_space.upper())
                                        item=re.sub(r"[\s]+","",item)
                                        with_Append_project_id_Final_output_sql_statements.append(item.upper())

                                else:
                                    # print("fun_project_ID",fun_project_ID)

                                    obj=re.sub("[\s]+SELECT","\nSELECT ",obj,flags=re.I|re.DOTALL)
                                    if obj.upper().startswith("SELECT "):
                                        find_select=re.findall(r"^SELECT[\s]+",obj,flags=re.I|re.DOTALL)
                                        if find_select:
                                            find_select=re.sub(r"[\s]+","",find_select[0])
                                            without_Append_project_id_Final_output_sql_statements.append(find_select.upper())
                                        all_output_file_statements.append(obj.upper())
                                    output_sql_statements=re.findall(r"TRUNCATE[\s]+TABLE[\s]*\`[\w]+\.[\w]+\`|TRUNCATE[\s]+TABLE[\s]*\`[\w]+\`|TRUNCATE[\s]+TABLE[\s]*[\w]+\.[\w]+|TRUNCATE[\s]+TABLE[\s]*[\w]+"
                                                                     r"|CALL[\s]+[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+FUNCTION[\s]*[\w]+\.[\w]+|^WITH[\s]+[\w]+|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+"
                                                                     r"|CREATE[\s]*TABLE[\s]*IF[\s]+NOT[\s]+EXISTS[\s]*[\w]+\.[\w]+|CREATE[\s]+OR[\s]+REPLACE[\s]+VIEW[\s]*[\w]+\.[\w]+|CREATE[\s]*TABLE[\s]*IF[\s]+NOT[\s]+EXISTS[\s]*\`[\w]+\.[\w]+\`"
                                                                     r"|INSERT[\s]+INTO[\s]*[\w]+\.[\w]+[\s]+|INSERT[\s]+INTO[\s]+[\w]+[\s]+|INSERT[\s]+INTO[\s]*\`[\w]+\.[\w]+\`"
                                                                     r"|DELETE[\s]*FROM[\s]*[\w]+\.[\w]+|DELETE[\s]+FROM[\s]+[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+\.[\w]+|DROP[\s]+TABLE[\s]+IF[\s]+EXISTS[\s]*[\w]+",obj,flags=re.I)
                                    #|UPDATE[\s]*[\w]+\.[\w]+|UPDATE[\s]+[\w]+
                                    # print("check here 7")
                                    # print("cred-o/p:-", output_sql_statements)
                                    for item in output_sql_statements:
                                        # print("after check here 7 check item", item)
                                        item=re.sub(r"\n","",item)
                                        item=re.sub(r"\`","",item)
                                        remove_space=re.sub(r"[\s]+"," ",item)
                                        # remove_space=re.sub(r"\`","",item)
                                        remove_space=re.sub(r"\`","",remove_space)
                                        # print("remove `:", remove_space)
                                        all_output_file_statements.append(remove_space.upper())
                                        item=re.sub(r"[\s]+","",item)
                                        without_Append_project_id_Final_output_sql_statements.append(item.upper())
                            else:
                                obj=obj+fileLine
    with_Append_project_id_Final_input_sql_statements.sort()
    with_Append_project_id_Final_output_sql_statements.sort()
    without_Append_project_id_Final_input_sql_statements.sort()
    without_Append_project_id_Final_output_sql_statements.sort()
    # print("with_Append_project_id_Final_input_sql_statements",with_Append_project_id_Final_input_sql_statements)
    # print("with_Append_project_id_Final_output_sql_statements",with_Append_project_id_Final_output_sql_statements)

    # print("without_Append_project_id_Final_input_sql_statements",without_Append_project_id_Final_input_sql_statements)
    # print("without_Append_project_id_Final_output_sql_statements",without_Append_project_id_Final_output_sql_statements)

    # print("all_input_file_statements",all_input_file_statements)
    # print("all_output_file_statements",all_output_file_statements)
    # print("No_with_Append_project_id_Final_input_sql_statements",len(with_Append_project_id_Final_input_sql_statements))
    # print("No_with_Append_project_id_Final_output_sql_statements",len(with_Append_project_id_Final_output_sql_statements))
    # print("No_without_Append_project_id_Final_input_sql_statements",len(without_Append_project_id_Final_input_sql_statements))
    # print("No_without_Append_project_id_Final_output_sql_statements",len(without_Append_project_id_Final_output_sql_statements))


    if len(with_Append_project_id_Final_input_sql_statements)>0:

        if with_Append_project_id_Final_input_sql_statements==with_Append_project_id_Final_output_sql_statements:
            # print("No_Of_Statement SUCCESSFUL")
            Result_No_Of_Statement.append("Y")
            log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
        else:
            Result_No_Of_Statement.append("N")
            log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
    elif len(without_Append_project_id_Final_input_sql_statements)>0:
        if without_Append_project_id_Final_input_sql_statements==without_Append_project_id_Final_output_sql_statements:
            # print("YYYY")
            Result_No_Of_Statement.append("Y")
            log_result.append(Result_Yes(all_input_file_statements,all_output_file_statements))
        else:
            Result_No_Of_Statement.append("N")
            log_result.append(Result_No(all_input_file_statements,all_output_file_statements))
    else:
        Result_No_Of_Statement.append("N")
        log_result.append(Result_No(all_input_file_statements,all_output_file_statements))

def fkt_check_cross_join(log_result,filename,JoinResult):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Verifying Join Conditions"))
    inputCrossJoin=[]
    outputCrossJoin=[]
    input_cross_join_list=[]
    output_cross_join_list=[]
    input_cross_join_log=[]
    output_cross_join_log=[]
    final_result="NA"
    # try:
    if 1==1:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as f:
                # print("hi")
                fileLines = f.read()
                #print("fileLines cross", fileLines)
                input_cross=re.findall(r'CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT|LEFT[\s]+JOIN[\s]+\`[\s]*[\w]+[\s]*\`\.[\w]+|RIGHT[\s]+JOIN[\s]+\`[\s]*[\w]+[\s]*\`\.[\w]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\`[\s]*[\w]+[\s]*\`\.[\w]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\`[\s]*[\w]+[\s]*\`\.[\w]+',fileLines,flags=re.I|re.DOTALL)
                # print("input cross", input_cross)
                if input_cross:
                    for ele in input_cross:
                        # print("inside")
                        remove_space=re.sub(r"[\s]+","",ele)
                        inputCrossJoin.append(remove_space)
                        input_cross_join_log.append(ele)

                input_from_to_where=re.findall(r'FROM(.*)WHERE',fileLines,flags=re.I|re.DOTALL)
                if input_from_to_where:
                    for ele in input_from_to_where:
                        input_cross_join_log.append(ele)

                    remove_space=re.split("\,|\n|\t|[\s]+",input_from_to_where[0])
                    for item in remove_space:
                        if item=='':
                            pass
                        else:
                            input_cross_join_list.append(item.upper())

                    # print("inputCrossJoin",inputCrossJoin)
                #print("input_cross_join_log",input_cross_join_log)

                #print("input_cross_join_list",input_cross_join_list)


        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as f:
                fileLines = f.read()
                #fileLines=re.sub(r"db_aedwd1.","SYS_CALENDAR.",fileLines,flags=re.I|re.DOTALL)

                # print("b4 Outputoutput_cross")

                output_cross=re.findall(r"CROSS[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|CROSS[\s]+JOIN[\s]+[\w]+[\s]+|CROSS[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|LEFT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|RIGHT[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|LEFT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+\.[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+[\w]+[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\([\s]*SELECT"
                                        r"|CROSS[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|LEFT[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|RIGHT[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+"
                                        r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+|RIGHT[\s]+OUTER[\s]+JOIN[\s]+\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]+",fileLines,flags=re.I|re.DOTALL)
                # print("Output cross", output_cross)
                if output_cross:
                    for ele in output_cross:
                        remove_space=re.sub(r"[\s]+","",ele)
                        outputCrossJoin.append(remove_space)
                        output_cross_join_log.append(ele)
                output_comma_cross=re.findall(r"INNER[\s]+JOIN[\s]+[\w]+\.[\w]+|INNER[\s]+JOIN[\s]+[\w]+[\s]+|INNER[\s]+JOIN[\s]+\([\s]*SELECT[\s]+",fileLines,flags=re.I|re.DOTALL)
                str1=""
                if output_comma_cross:
                    for ele_innerJoin in output_comma_cross:
                        str1=str1+" "+ele_innerJoin
                    output_cross_join_log.append(str1)
                    output_comma_cross=' '.join(map(str, output_comma_cross)).upper().split('INNER JOIN')

                if output_comma_cross:
                    for ele_innerJoin in output_comma_cross:
                        remove_space=re.sub("[\s]+","",ele_innerJoin,flags=re.I)
                        if remove_space=='':
                            pass
                        else:
                            output_cross_join_list.append(remove_space.upper())

        if len(output_cross_join_list)==0:
            final_result="true"
        else:
            for item in output_cross_join_list:
                if item in input_cross_join_list:
                    final_result="true"
                else:
                    final_result="false"
                    break
        inputCrossJoin.sort()
        outputCrossJoin.sort()
        # print(filename)
        # print("inputCrossJoin",inputCrossJoin)
        # print("outputCrossJoin",outputCrossJoin)
        # print(len(inputCrossJoin))
        # print(len(outputCrossJoin))
        for ele in list(outputCrossJoin):
            if ele.lower() == "crossjoinunnest":
                outputCrossJoin.remove(ele)
        for ele in list(outputCrossJoin):
            if ele.lower() == "leftjoinunnest":
                outputCrossJoin.remove(ele)


        # print("inputCrossJoin",inputCrossJoin)
        # print("outputCrossJoin",outputCrossJoin)
        # print(len(inputCrossJoin))
        # print(len(outputCrossJoin))


        if len(inputCrossJoin)==0 and len(outputCrossJoin)==0 and final_result=="NA":
            JoinResult.append("NA")
            log_result.append(Result_NA())
        elif len(inputCrossJoin)>0 and len(outputCrossJoin)>0 or final_result=="true":
            if len(inputCrossJoin) == len(outputCrossJoin) and final_result=="true":
                JoinResult.append("Y")
                log_result.append(Result_Yes(input_cross_join_log,output_cross_join_log))
            else:
                JoinResult.append("N")
                log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

        else:
            JoinResult.append("N")
            log_result.append(Result_No(input_cross_join_log,output_cross_join_log))

def fkt_PROJECT_ID(log_result,project_ID,filename):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Project ID"))
    try:
        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            file_obj = re.sub(r"EXTRACT[\s]*\(.*?\)", "", file_obj, flags=re.I|re.DOTALL)
            project_id=re.findall(r'JOIN[\s]+\`*[\w]+\.[\w]+\`*\;|FROM[\s]+\`*[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*\(|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
            # print(project_id)
            if project_id:
                project_ID.append("N")
                log_result.append(Result_No(project_id,project_id))
            else:
                project_id=re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;|PROCEDURE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\(|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+',file_obj,flags=re.I)
                if project_id:
                    project_ID.append("Y")
                    log_result.append(Result_Yes(project_id,project_id))
                else:
                    project_ID.append("NA")
                    log_result.append(Result_NA())

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        project_ID.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)



def fkt_Check_colan_to_space(log_result,filename,Result_check_colan_to_space):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check for Struct colan to space"))
    input_struct_raw=[]
    output_struct_raw=[]
    try:
        if filename in inputFile:
            query = common_obj.Read_file(inputFolderPath+filename)

            find_map = re.findall(r"MAP[\s]*\<[\s]*[\w]+[\s]*\,[\s]*[\w]+[\s]*|ARRAY[\s]*\<[\s]*STRUCT[\s]*\<[\s]*KEY[\s]*\:", query, flags=re.I|re.DOTALL)
            # print(find_map)


            find_struct=re.findall(r"struct\<(.*?)\>\>", query, flags=re.I|re.DOTALL)
            
            if find_struct:
                for i in find_struct:
                    # print("1111", i)
                    
                    i = re.sub(r"COMMENT[\s]*\'*.*?\'", "", i,flags=re.I|re.DOTALL)  #added
                    i = re.sub(r"MAP[\s]*\<[\s]*[\w]+[\s]*\,[\s]*[\w]+[\s]*|ARRAY[\s]*\<[\s]*STRUCT[\s]*\<[\s]*KEY[\s]*\:[\s]*[\w]+[\s]*\,[\s]*VALUE[\s]*\:[\s]*[\w]+[\s]*\>", "", i, flags=re.I|re.DOTALL)  #added
                    find_struct1=i.split(",")

                    for replce_space in find_struct1:
                        # print("1111", replce_space)
                        replce_space = re.sub(r":", " ", replce_space,flags=re.I|re.DOTALL)
                        
                        replce_space = re.sub(r" int| bigint| smallint| tinyint", "INT64", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r"\<int|\<bigint|\<smallint|\<tinyint", "<INT64", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" double| float", "FLOAT64", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r"\<double", "<FLOAT64", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" string", "STRING", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" string\(| varchar\(| char\(", "STRING(", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" date", "DATE", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" binary", "BYTES", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" timestamp", "DATETIME", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r" boolean", "BOOL", replce_space,flags=re.I|re.DOTALL)
                        replce_space = re.sub(r"\`|[\s]*", "", replce_space,flags=re.I|re.DOTALL)
                        # print("1111", replce_space)
                        replce_space = re.sub(r"\<timestamp\>", "<DATETIME>", replce_space,flags=re.I|re.DOTALL)
                        input_struct_raw.append(replce_space.lower())

        if filename in outputFile:
            query = common_obj.Read_file(OutputFolderPath+filename)
            
            query=re.sub(r"ARRAY\<[\s]*STRUCT[\s]*\<[\s]*key[\s]*[\w]+[\s]*\,[\s]*value[\s]*[\w]+[\s]*\>"
                         r"|STRUCT[\s]*\<[\s]*key[\s]*[\w]+[\s]*\,[\s]*value[\s]*[\w]+[\s]*\>"
                         r"|OPTIONS[\s]*\([\s]*DESCRIPTION[\s]*\=[\s]*\'.*?\'[\s]*\)", " ", query, flags=re.I|re.DOTALL)
            # print(query)
            find_struct_op=re.findall(r"STRUCT[\s]*\<(.*?)\>\>", query, flags=re.I|re.DOTALL)
            # print("find_struct_op=", find_struct_op)
            if find_struct_op:
                for i in find_struct_op:
                    i = re.sub(r"\n|[\s]*|\`", "", i)
                    i = re.sub(r"[\s]*\<[\s]*", "<", i)
                    i = re.sub(r"[\s]*\>[\s]*", ">", i)
                    output_struct_raw1 = i.lower().split(",")

                    for replce_space in output_struct_raw1:
                        # print("111", replce_space)
                        output_struct_raw.append(replce_space)
                    
        # print("input_struct_raw =", input_struct_raw)
        # print("output_struct_raw=", output_struct_raw)        

        if len(input_struct_raw)==0 and len(output_struct_raw)==0:
            Result_check_colan_to_space.append("NA")
            log_result.append(Result_NA())
        elif find_map:
            Result_check_colan_to_space.append("CM")
            log_result.append(Result_CM("Map in  input file."))        
        elif input_struct_raw == output_struct_raw:
            # print("YYYY")
            Result_check_colan_to_space.append("Y")
            log_result.append(Result_Yes(find_struct,find_struct_op))
        else:
            Result_check_colan_to_space.append("N")
            log_result.append(Result_No(input_struct_raw,output_struct_raw))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        Result_check_colan_to_space.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_Check_map_to_array(log_result,filename,Result_check_map_to_array):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check for Map to Array"))
    # print(":inside input")
    input_map=[]
    input_map_raw=[]
    if filename in inputFile:
        with open(inputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(inputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            # print("query=", query)
            find_map=re.findall(r"map\<(.*?)\>",query,flags=re.I|re.DOTALL)
            # print("find_map=", find_map)
            find_inputlog=re.findall(r"map\<[\w]+\,[\w]+\>",query,flags=re.I|re.DOTALL)

            for i in find_map:
                find_map1=i.split(",")
                # print("find_map1 =",find_map1)
                for replce_space in find_map1:
                    replce_space=re.sub(r"int|bigint|smallint|tinyint", "INT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"double|float", "FLOAT64",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"string|varchar|char", "STRING",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"date", "DATE",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"binary", "BYTES",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"timestamp", "TIMESTAMP",replce_space,flags=re.I|re.DOTALL)
                    replce_space=re.sub(r"boolean", "BOOL",replce_space,flags=re.I|re.DOTALL)
                    input_map_raw.append(replce_space)
                    # print("input_map_raw =", input_map_raw)
            a=[i for i in zip(*[iter(input_map_raw)]*2)]
            # print("a", a)  
            for i in a:
                input_map.append("array<struct<" + "key "+ i[0] + ",value "+ i[1] + ">>")
            # print("input_map=", len(input_map))

    if filename in outputFile:
        with open(OutputFolderPath+filename, 'rb') as f:
            result = chardet.detect(f.read())
            e=result['encoding']
        with open(OutputFolderPath+filename, "r",encoding=e) as fr:
            query = fr.read()
            fr.close()
            # print("query=", query)
            output_array=re.findall(r"(ARRAY<STRUCT[\s]*\<[\s]*key[\s]*[\w]+\,[\s]*value[\s]*[\w]+[\s]*\>\>)",query,flags=re.I|re.DOTALL)
            # print("output_array=", len(output_array))

    # print("input_map=", input_map)
    # print("output_array=", output_array)

    if len(input_map)==0 and len(output_array)==0:
        Result_check_map_to_array.append("NA")
        log_result.append(Result_NA())

    elif len(input_map) == len(output_array):
        # print("Check Map to Array has been done successfully")
        Result_check_map_to_array.append("Y")
        log_result.append(Result_Yes(find_inputlog,output_array))
    else:
        Result_check_map_to_array.append("N")
        log_result.append(Result_No(find_inputlog,output_array))

def fun_fkt_check_d_added_for_number_in_column(log_result,filename,result_d_added_for_number_in_column):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("d_ in number column"))
    input_columns = []
    output_columns = []
    input_log = []
    output_log = []
    if filename in inputFile:
        file_obj = common_obj.Read_file(inputFolderPath+filename)
       
        if 'CREATE EXTERNAL TABLE' in file_obj.upper() or 'CREATE TABLE' in file_obj.upper():
            if 'STRUCT' in file_obj.upper():
                find_num_column = re.findall(r"\<[\s]*[\w]+|\,[\s]*[\w]+|SELECT[\s]*[\w]+", file_obj, flags=re.I|re.DOTALL)  #,[\s]*[\w]+|select[\s]*[\w]+
                # print(find_num_column)
                for ele in find_num_column:
                    # print("ele", ele)
                    ele = re.sub(r"\<|\,", "", ele)
                    # print("ele", ele)
                    find_num = re.findall(r"^[\d]+.*", ele)

                    if find_num:
                        # print(find_num)
                        input_log.append(find_num[0])
                        input_columns.append(find_num[0])
            
            else:
                find_num_column=re.findall(r"\`[\w]+\`[\s]+[\w]+", file_obj, flags=re.I|re.DOTALL)  
                # print("find_num_column=", len(find_num_column), find_num_column)
                if find_num_column:
                    for ele in find_num_column:
                        ele=re.sub(r"\`|[\s]+[\w]+", "", ele)
                        find_num=re.findall(r"^[\d]+.*", ele)
                        
                        if find_num:
                            # print(find_num)
                            input_log.append(find_num[0])
                            input_columns.append(find_num[0])
                    
   
    if filename in outputFile:
        file_obj = common_obj.Read_file(OutputFolderPath+filename)
        
        if 'CREATE TABLE IF NOT EXISTS' in file_obj.upper() or 'CREATE OR REPLACE EXTERNAL TABLE' in file_obj.upper():
            if 'STRUCT' in file_obj:
                find_num_column = re.findall(r"\<[\s]*[\w]+|\,[\s]*[\w]+|SELECT[\s]*[\w]+", file_obj, flags=re.I|re.DOTALL)  #,[\s]*[\w]+|select[\s]*[\w]+
                # print(find_num_column)
                for ele in find_num_column:
                    # print("ele", ele)
                    ele = re.sub(r"\<|\,|\n|[\s]+", "", ele)
                    # print("ele", ele)
                    find_num = re.findall(r"^d_[\d]+.*", ele)

                    if find_num:
                        # print(find_num)
                        output_log.append(find_num[0])
                        output_columns.append(find_num[0])

            else:
                find_d_columns = re.findall(r"\nd_[\d]+.*?[\s]+[\w]+", file_obj, flags=re.I|re.DOTALL)
                if find_d_columns:
                    for ele in find_d_columns:
                        # print(ele)
                        ele = re.sub(r"\n", "", ele)
                        ele = re.sub(r"[\s]+[\w]+", "", ele)
                        output_log.append(ele)
                        ele = re.sub(r"d_", "", ele)
                        output_columns.append(ele)

    # print("input_columns=", len(input_columns), input_columns)
    # print("output_columns=", len(output_columns), output_columns)

    if len(input_columns)==0:
        result_d_added_for_number_in_column.append("NA")
        log_result.append(Result_NA())
    elif len(input_columns) == len(output_columns):
        # print("YYYY")
        result_d_added_for_number_in_column.append("Y")
        log_result.append(Result_Yes(input_log,output_log))
    else:
        result_d_added_for_number_in_column.append("N")
        log_result.append(Result_No(input_log,output_log))

def fun_fkt_check_keyword_columns(log_result,filename,result_keyword_columns):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check_keyword_columns"))
    input_columns = []
    output_columns = []
    input_log = []
    output_log = []
    try:
    # if 1==1:
        if filename in inputFile:
            file_obj = common_obj.Read_file(inputFolderPath+filename)
            keyword_list = ['partition','all', 'and', 'any', 'array', 'as', 'asc', 'assert_rows_modified', 'at', 'between', 'by', 'case', 'cast', 'collate', 'contains', 'create', 'cross', 'cube', 'current', 'default', 'define', 'desc', 'distinct', 'else', 'end', 'enum', 'escape', 'except', 'exclude', 'exists', 'extract', 'false', 'fetch', 'following', 'for', 'from', 'full', 'group', 'grouping', 'groups', 'hash', 'having', 'if', 'ignore', 'in', 'inner', 'intersect', 'interval', 'into', 'is', 'join', 'lateral', 'left', 'like', 'limit', 'lookup', 'merge', 'natural', 'new', 'no', 'not', 'null', 'nulls', 'of', 'on', 'or', 'order', 'outer', 'over', 'partition', 'preceding', 'proto', 'qualify', 'range', 'recursive', 'respect', 'right', 'rollup', 'rows', 'select', 'set', 'some', 'struct', 'tablesample', 'then', 'to', 'treat', 'true', 'unbounded', 'union', 'unnest', 'using', 'when', 'where', 'window', 'with', 'within']
            
            if 'CREATE EXTERNAL TABLE' in file_obj.upper() or 'CREATE TABLE' in file_obj.upper():
                file_obj = re.sub(r"STORED[\s]*AS.*?\;", "", file_obj, flags=re.I|re.DOTALL)
                # print(file_obj)

                if 'STRUCT' in file_obj.upper():
                    find_num_column = re.findall(r"\<[\s]*[\w]+|\,[\s]*[\w]+|SELECT[\s]*[\w]+", file_obj, flags=re.I|re.DOTALL)  #,[\s]*[\w]+|select[\s]*[\w]+
                    # print(find_num_column)
                    if find_num_column:
                        for ele in find_num_column:
                            ele = re.sub(r"\<struct|\,|\<", "", ele)
                            if ele.lower() in keyword_list:
                                input_log.append(ele)
                                input_columns.append(ele)
                
                else:
                    find_num_column=re.findall(r"\`[\w]+\`[\s]+[\w]+", file_obj, flags=re.I|re.DOTALL)  
                    # print("find_num_column=", len(find_num_column), find_num_column)
                    if find_num_column:
                        for ele in find_num_column:
                            ele=re.sub(r"\`|[\s]+[\w]+", "", ele)
                            # print("ele", ele)
                            
                            if ele.lower() in keyword_list:
                                input_log.append(ele)
                                input_columns.append(ele)
                        
     

        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            if 'CREATE TABLE IF NOT EXISTS' in file_obj.upper() or 'CREATE OR REPLACE EXTERNAL TABLE' in file_obj.upper():
                find_num_column = re.findall(r"\`[\w]+\`", file_obj, flags=re.I|re.DOTALL)  #,[\s]*[\w]+|select[\s]*[\w]+
                # print(find_num_column)
                for ele in find_num_column:
                    # print("ele", ele)
                    ele = re.sub(r"[\s]+[\w]+", "", ele)
                    ele = re.sub(r"^\_", "", ele)
                    output_log.append(ele)
                    output_columns.append(ele)

                
        # print("input_columns=", len(input_columns), input_columns)
        # print("output_columns=", len(output_columns), output_columns)

        if len(input_columns)==0 and len(output_columns)==0:
            result_keyword_columns.append("NA")
            log_result.append(Result_NA())
        elif len(input_columns) == len(output_columns):
            # print("YYYY")
            result_keyword_columns.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_keyword_columns.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_keyword_columns.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_native_external_table(log_result,filename,result_native_external_table,native_list):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check_Native_External_table"))
    input_list = []
    output_list = []
    input_log = []
    output_log = []
    try:
        if filename in inputFile:
            file_obj = common_obj.Read_file(inputFolderPath+filename)

            find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+`*[\w]+\.([\w]+)`*", file_obj, flags=re.I|re.DOTALL)
            if find_table:
                find_table1 = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+`*([\w]+\.[\w]+)`*", file_obj, flags=re.I|re.DOTALL)

                if find_table[0].lower() in native_list:
                    ele = 'CREATE TABLE IF NOT EXISTS `cf-raven-flipkart.'+find_table1[0]+'`'
                    input_log.append(ele)
                    input_list.append(ele)
                else:
                    ele = 'CREATE OR REPLACE EXTERNAL TABLE `cf-raven-flipkart.'+find_table1[0]+'`'
                    input_log.append(ele)
                    input_list.append(ele)


        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.[\w]+`*", file_obj, flags=re.I|re.DOTALL)
            # print(find_table)

            if find_table:
                output_log.append(find_table[0])
                output_list.append(find_table[0])
                
        # print("input_list=", len(input_list), input_list)
        # print("output_list=", len(output_list), output_list)

        if len(input_list)==0 and len(output_list)==0:
            result_native_external_table.append("NA")
            log_result.append(Result_NA())
        elif 'CREATE TABLE IF NOT EXISTS' in input_list[0] and 'CREATE TABLE IF NOT EXISTS' in output_list[0]:
            result_native_external_table.append("Y(N-N)")
            log_result.append(Result_Yes(input_log,output_log))
        elif 'CREATE OR REPLACE EXTERNAL TABLE' in input_list[0] and 'CREATE OR REPLACE EXTERNAL TABLE' in output_list[0]:
            result_native_external_table.append("Y(E-E)")
            log_result.append(Result_Yes(input_log,output_log))
        elif 'CREATE TABLE IF NOT EXISTS' in input_list[0] and 'CREATE OR REPLACE EXTERNAL TABLE' in output_list[0]:
            result_native_external_table.append("N(N-E)")
            log_result.append(Result_No(input_log,output_log))
        elif 'CREATE OR REPLACE EXTERNAL TABLE' in input_list[0] and 'CREATE TABLE IF NOT EXISTS' in output_list[0]:
            result_native_external_table.append("N(E-N)")
            log_result.append(Result_No(input_log,output_log))
        else:
            result_native_external_table.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_native_external_table.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def get_external_table_path_from_sheet(External_Table_GS_Path):
    try:
        df=pd.read_excel(External_Table_GS_Path)
        external_table_path_dict={}
        for i in range(0,len(df)):
            ele=df.iloc[i]
            table_name=str(ele[1]).strip().lower()
            table_path=str(ele[2]).strip()
            external_table_path_dict[table_name]=table_path
        return external_table_path_dict
        # print(df)
    except Exception as e:
        print(e)
        # print("Error in processing: ", filename)
        print("Unexpected error:", sys.exc_info()[0])

def fkt_check_external_table_path_for_HV(log_result,filename,external_table_path_result_for_hv,external_table_path_dict):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check External Table Path "))
    input_table_location_dict={}
    input_log_obj=[]
    output_log_obj=[]
    final_result='Y'
    try:
        if filename in inputFile:
            with open(inputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(inputFolderPath+filename, "r",encoding=e) as fr:
                query = fr.read()
                fr.close()
                find_external_table=re.findall(r'(CREATE.*?EXTERNAL.*?TABLE.*?\`*[\w]+\`*\.\`*([\w]+)\`*.*?;)',query,flags=re.I|re.DOTALL)
                for table in find_external_table:
                    table_name=table[1].lower()
                    table_obj=table[0]
                    find_location=re.findall(r'[\W]LOCATION[\s]+[\'\"](.*?)[\'\"]',table_obj,flags=re.I|re.DOTALL)
                    if find_location:
                        input_table_location_dict[table_name]=find_location[0]
                        # print(find_location)

        if filename in outputFile:
            with open(OutputFolderPath+filename, 'rb') as f:
                result = chardet.detect(f.read())
                e=result['encoding']
            with open(OutputFolderPath+filename, "r",encoding=e) as fr:
                query = fr.read()
                fr.close()
                find_external_table=re.findall(r'(CREATE.*?EXTERNAL.*?TABLE.*?\`*[a-zA-Z0-9-_]+\`*\.\`*[\w]+\`*\.\`*([\w]+)\`*.*?;)',query,flags=re.I|re.DOTALL)
                # print(find_external_table)
                for table in find_external_table:
                    table_name=table[1].lower()
                    table_obj=table[0]
                    find_location=re.findall(r'uris[\s]*\=[\s]*\[[\'\"](.*?)\*.orc[\'\"]',table_obj,flags=re.I|re.DOTALL)
                    # print(find_location)
                    if find_location:
                        if table_name in external_table_path_dict:
                            if external_table_path_dict[table_name] == find_location[0]:
                                input_log_obj.append("Path In Input File : "+input_table_location_dict[table_name])
                                output_log_obj.append("Path In Output File : "+find_location[0])
                            else:
                                input_log_obj.append("Path In Input File : "+input_table_location_dict[table_name])
                                output_log_obj.append("Path In Output File : "+find_location[0])
                                final_result='N'
                        else:
                            if input_table_location_dict[table_name] == find_location[0]:
                                input_log_obj.append("Path In Input File : "+input_table_location_dict[table_name])
                                output_log_obj.append("Path In Output File : "+find_location[0])
                            else:
                                input_log_obj.append("Path In Input File : "+input_table_location_dict[table_name])
                                output_log_obj.append("Path In Output File : "+find_location[0])
                                final_result='N'
                    elif table_name in input_table_location_dict and not find_location:
                        input_log_obj.append("Path In Input File : "+input_table_location_dict[table_name])
                        output_log_obj.append("Path In Output File : External Table Path Not Present In Output File")
                        final_result='N'

        if len(input_log_obj)==0 and len(output_log_obj) == 0:
            external_table_path_result_for_hv.append("NA")
            log_result.append(Result_NA())

        elif final_result == 'Y':
            # print("COMMENT FILES SUCCESSFUL")
            external_table_path_result_for_hv.append("Y")
            log_result.append(Result_Yes(input_log_obj,output_log_obj))
        else:
            external_table_path_result_for_hv.append("N")
            # print("COMMENT FILES unSUCCESSFUL")
            log_result.append(Result_No(input_log_obj,output_log_obj))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        external_table_path_result_for_hv.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_partition_col_datatype(log_result,filename,result_partition_col_datatype,df):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_partition_col_datatype"))
    input_list = []
    output_list = []
    input_log = []
    output_log = []
    try:
        if filename in inputFile:
            for i in range(0,len(df)):
                ele = df.iloc[i]
                table_name = str(ele[0]).strip().lower()

                file_obj = common_obj.Read_file(inputFolderPath+filename)

                find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+`*[\w]+\.([\w]+)`*", file_obj, flags=re.I|re.DOTALL)
                # print(find_table)

                if find_table:
                    if find_table[0].lower() in table_name:
                        column_name = str(ele[1]).strip().lower()
                        src_datatype = str(ele[2]).strip().lower()
                        trg_datatype = str(ele[4]).strip().lower()

                        find_col = re.findall("\`*"+column_name+"\`*[\s]+[\w]+", file_obj, flags=re.I|re.DOTALL)
                        # print(find_col[0])

                        if find_col[0].split(' ')[1].lower() in src_datatype:
                            ele = re.sub(r"[\s]+[\w]+", " "+trg_datatype,  find_col[0], flags=re.I|re.DOTALL)

                            input_log.append(find_col[0])
                            input_list.append(ele.replace('`', '').lower())


        if filename in outputFile:
            for i in range(0,len(df)):
                ele = df.iloc[i]
                table_name = str(ele[0]).strip().lower()

                file_obj = common_obj.Read_file(OutputFolderPath+filename)

                find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.([\w]+)`*", file_obj, flags=re.I|re.DOTALL)
                # print(find_table)

                if find_table:
                    if find_table[0].lower() in table_name:
                        column_name = str(ele[1]).strip().lower()
                        trg_datatype = str(ele[4]).strip().lower()

                        find_col = re.findall("\`*"+column_name+"\`*[\s]+[\w]+", file_obj, flags=re.I|re.DOTALL)
                        # print(find_col[0])
                        if find_col:
                            output_list.append(find_col[0].lower())
                            output_log.append(find_col[0].lower())

                
                #     output_log.append(find_table[0])
                
        # print("input_list=", len(input_list), input_list)
        # print("output_list=", len(output_list), output_list)

        if len(input_list)==0 and len(output_list)==0:
            result_partition_col_datatype.append("NA")
            log_result.append(Result_NA())
        elif input_list == output_list:
            # print("YYYY")
            result_partition_col_datatype.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_partition_col_datatype.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_partition_col_datatype.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_Check_Partitioned_BY_to_Partition_By(log_result,filename,result_Check_Partitioned_BY_to_Partition_By,native_list):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partitioned to partition by"))

    input_partitioned_list = []
    output_partition_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            query = common_obj.Read_file(inputFolderPath+filename)

            # find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+`*[\w]+\.([\w]+)`*", query, flags=re.I|re.DOTALL)
            # if find_table:
            #     if find_table[0].lower() in native_list:
            #         pass
            #     else:


            query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'", "", query, flags=re.I|re.DOTALL)
            query = re.sub(r"array\<(.*?)\>", " ", query, flags=re.I|re.DOTALL)
            find_par_col = re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)", query, flags=re.I|re.DOTALL)
            try:
                if find_par_col:
                    # print(find_par_col)
                    for ele in find_par_col:
                        # print("ele", ele)
                        ele = re.sub(r"STRING|INTEGER|BIGINT|SMALLINT|TINYINT|DOUBLE|INT|TIMESTAMP|\\n\s|\,|(CO.{3}NT)+\s*\'(.*?)\'|\`", "", ele, flags=re.I)
                        # print("ele", ele)
                        input_partitioned_list = ele.split()
            except:
                pass

        Log_input=input_partitioned_list
        # input_partition_list=input_partition_list[:4]
        # print("input_partition_list :", input_partitioned_list)

        if filename in outputFile:
            query = common_obj.Read_file(OutputFolderPath+filename)
            query = re.sub(r"CLUSTER[\s]+BY[\s]+(.*?)\;", ';', query, flags=re.DOTALL|re.I)
            query = re.sub(r"RANGE_BUCKET[\s]*\(", '', query, flags=re.DOTALL|re.I)
            query = re.sub(r"GENERATE_ARRAY\(.*?\)\)", '', query, flags=re.DOTALL|re.I)
            query = re.sub(r"DATE[\s]*\(",'',query,flags=re.DOTALL|re.I)
            # print(query)
            find_cols1 = re.findall(r"PARTITION[\s]+BY[\s]+(.*?)\;", query, flags=re.DOTALL|re.I)
            find_cols2 = re.findall(r"WITH[\s]+PARTITION[\s]+COLUMNS[\s]*(.*?)[\s]*OPTIONS", query, flags=re.DOTALL|re.I)
            # print(query)
            find_cols = find_cols1+find_cols2
            # print("find_cols", find_cols)

            try:
                if find_cols:
                    list1 = find_cols[0].split(",")
                    for i in list1:
                        # print(i)
                        i = re.sub(r"[\s]+INT64|[\s]+STRING|[\s]+FLOAT64|[\s]+DATETIME|DATETIME_TRUNC[\s]*\(", "", i, flags=re.I)
                        i = re.sub(r"MONTH", "", i)
                        i = re.sub(r"[\s]+|\(|\)", "", i)
                        # print('iiii', i)
                        if '' == i:
                            pass
                        else:
                            i = re.sub(r"\(|\)|\n|[\s]+", "", i)
                            output_partition_list.append(i)
                    # print("output_cluster_by_list:", output_partition_list)
                    Log_output = output_partition_list
            except:
                pass

        # print("input_partition_list:", input_partitioned_list)
        # print("output_partition_list:", output_partition_list)

        if len(find_cols)==0:   #len(output_partition_list)==0 and len(input_partitioned_list)==0:
            log_result.append(Result_NA())
            result_Check_Partitioned_BY_to_Partition_By.append("NA")
        elif input_partitioned_list==output_partition_list:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Check_Partitioned_BY_to_Partition_By.append("Y("+str(len(input_partitioned_list))+"-"+str(len(output_partition_list))+")")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Check_Partitioned_BY_to_Partition_By.append("N("+str(len(input_partitioned_list))+"-"+str(len(output_partition_list))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_Partitioned_BY_to_Partition_By.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_Check_PartitionBy_to_ClusterBy(log_result,filename,result_Check_PartitionBy_to_ClusterBy,native_list):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partition to cluster by check"))

    input_partition_list = []
    output_cluster_by_list = []
    Log_input = []
    Log_output = []
    try:
        if filename in inputFile:
            query = common_obj.Read_file(inputFolderPath+filename)

            find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+`*[\w]+\.([\w]+)`*", query, flags=re.I|re.DOTALL)
            if find_table:
                if find_table[0].lower() in native_list:
                  

                    query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'", "", query, flags=re.I|re.DOTALL)
                    query = re.sub(r"array\<(.*?)\>", " ", query, flags=re.I|re.DOTALL)
                    find_par_col = re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)", query, flags=re.I|re.DOTALL)
                    try:
                        if find_par_col:
                            for ele in find_par_col:
                                # print(ele)
                                ele = re.sub(r"\`*[\w]+\`*[\s]+DATE|\`*[\w]+\`*[\s]+INT|\`*[\w]+\`*[\s]+TIMESTAMP|\`*[\w]+\`*[\s]+BIGINT|\\n\s|STRING|\,|(CO.{3}NT)+\s*\'(.*?)\'|\`", "", ele, flags=re.I)   #
                                # print("ele",ele)
                                input_partition_list = ele.split()
                    except:
                        pass

                    try:
                        find_clusted_col = re.findall(r"CLUSTERED[\s]+BY[\s]*\((.*?)\)", query, flags=re.I|re.DOTALL)
                        find_clusted_col = find_clusted_col[0].split()
                        if find_clusted_col:
                            for ele in find_clusted_col:
                                ele = re.sub(r"\n|\s+|\,", "", ele)
                                input_partition_list.append(ele)
                    except:
                        pass

        Log_input = input_partition_list
        input_partition_list = input_partition_list[:4]

        # print("input_partition_list :", input_partition_list)

        if filename in outputFile:
            query = common_obj.Read_file(OutputFolderPath+filename)
            # find_create_col=re.findall(r"CREATE[\s]+TABLE[\s]+\`*[\w]+\`*\.\`*[\w]+\`*[\s]*\((.*?)\)|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\`*\.\`*[\w]+\`*[\s]*\((.*?)\)|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\`*\.\`*[\w]+\`*\.\`*[\w]+`[\s]*\((.*?)\)",query,flags=re.DOTALL|re.I)
            find_clust = re.findall(r"CLUSTER[\s]+BY[\s]+(.*?)\;",query,flags=re.DOTALL|re.I)
            # print(find_clust)
            try:
                if find_clust:
                    list_col = find_clust[0].split(",")
                    # print(list_col)
                    if list_col:
                        for ele in list_col:
                            ele = re.sub(r"[\s]*", "", ele)
                            output_cluster_by_list.append(ele)

                    # output_cluster_by_list
                    Log_output = output_cluster_by_list
            except:
                pass

        # print("input_partition_list:", input_partition_list)
        # print("output_cluster_by_list:", output_cluster_by_list)

        if len(output_cluster_by_list)==0: #and len(output_cluster_by_list)==0:
            log_result.append(Result_NA())
            result_Check_PartitionBy_to_ClusterBy.append("NA")
        elif input_partition_list==output_cluster_by_list:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_Check_PartitionBy_to_ClusterBy.append("Y("+str(len(input_partition_list))+"-"+str(len(output_cluster_by_list))+")")
        elif len(input_partition_list)==0 and output_cluster_by_list:
            result_Check_PartitionBy_to_ClusterBy.append("CM")
            log_result.append(Result_CM("check the input file for datatype (date, timestamp, date)."))
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_Check_PartitionBy_to_ClusterBy.append("N("+str(len(input_partition_list))+"-"+str(len(output_cluster_by_list))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_PartitionBy_to_ClusterBy.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_check_datetime_trunc_Granularity(log_result,filename,result_Check_datetime_trunc_Granularity,df):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_partition_col_datatype"))
    input_list = []
    output_list = []
    input_log = []
    output_log = []
    try:
        if filename in outputFile:
            for i in range(0,len(df)):
                ele = df.iloc[i]
                table_name = str(ele[0]).strip().lower()

                file_obj = common_obj.Read_file(OutputFolderPath+filename)

                find_table = re.findall(r"CREATE[\s]+.*?TABLE.*?[\s]+\`*[a-zA-Z0-9-_]+\.[\w]+\.([\w]+)`*", file_obj, flags=re.I|re.DOTALL)
                # print(find_table)

                if find_table:
                    if find_table[0].lower() in table_name:
                        column_name = str(ele[1]).strip().lower()
                        granularity = str(ele[5]).strip().lower()
                        # print(granularity)

                        find_col = re.findall("PARTITION[\s]+BY[\s]+DATETIME_TRUNC\("+column_name+"[\s]*\,[\s]*([\w]+)", file_obj, flags=re.I|re.DOTALL)
                        # print(find_col[0])
                        if find_col:
                            if find_col[0].lower() in granularity:
                                output_list.append(find_col[0].lower())
                                output_log.append(find_col[0].lower())

                
                #     output_log.append(find_table[0])
                
        # print("input_list=", len(input_list), input_list)
        # print("output_list=", len(output_list), output_list)

        if len(input_list)==0 and len(output_list)==0:
            result_Check_datetime_trunc_Granularity.append("NA")
            log_result.append(Result_NA())
        elif output_list:
            # print("YYYY")
            result_Check_datetime_trunc_Granularity.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_Check_datetime_trunc_Granularity.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Check_datetime_trunc_Granularity.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_partition_timestamp_to_date_function(log_result,filename,result_partition_timestamp_to_date_function):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Partition timestamp to date function"))
    input_list = []
    output_partition_list = []
    Log_input=[]
    Log_output=[]
    try:
        if filename in inputFile:
            query = common_obj.Read_file(inputFolderPath+filename)
            query = re.sub(r"(CO.{3}NT)+\s*\'(.*?)\'","",query,flags=re.I|re.DOTALL)
            query = re.sub(r"array\<(.*?)\>"," ",query,flags=re.I|re.DOTALL)
            find_par_col=re.findall(r"PARTITIONED[\s]+BY[\s]*\((.*?)\)",query,flags=re.I|re.DOTALL)
            # print(find_par_col[0].split(','))
            if find_par_col:
                first_col=[]
                all_timestamp_col=find_par_col[0].split(',')
                Log_input=all_timestamp_col
                for ele in find_par_col[0].split(','):
                    if ' TIMESTAMP' in ele:
                        first_col.append(ele)

                if len(first_col) == len(all_timestamp_col):
                    first_col[0] = re.sub(r" TIMESTAMP|\`|[\s]*", "", first_col[0], flags=re.I)
                    new_string = re.sub(first_col[0], "DATE("+first_col[0]+")", first_col[0])
                    # print("new_string", new_string)
                    input_list.append(new_string)

        # print("input_list :", input_list)

        if filename in outputFile:
            query = common_obj.Read_file(OutputFolderPath+filename)
            query =re.sub(r"CLUSTER[\s]+BY[\s]+(.*?)\;",';',query,flags=re.DOTALL|re.I)
            query =re.sub(r"RANGE_BUCKET[\s]*\(",'',query,flags=re.DOTALL|re.I)
            output_list =re.findall(r"PARTITION[\s]+BY[\s]+(DATE[\s]*\([\w]+\))",query,flags=re.DOTALL|re.I)
            # print(output_list)
            Log_output=output_list

        # print("input_list:", input_list)
        # print("output_list:", output_list)

        if len(output_list)==0:
            log_result.append(Result_NA())
            result_partition_timestamp_to_date_function.append("NA")
        elif input_list == output_list:
            # print("YYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_partition_timestamp_to_date_function.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_partition_timestamp_to_date_function.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_partition_timestamp_to_date_function.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_match_table_view_count(log_result,filename,result_match_table_view_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Match table View Count"))
    input_objects=[]
    output_objects=[]
    input_log=[]
    output_log=[]
    w_w = "\`*[\w]+\`*\.\`*[\w]+\`*"
    proj_id = "\`*[a-zA-Z0-9-_]+\`*\."
    try:
        if filename in inputFile:
            file_obj = common_obj.Remove_comments(inputFolderPath+filename)

            find_objects_ip = re.findall(r"OVERWRITE[\s]+TABLE[\s]+"+w_w+""
                                         r"|VIEW[\s]+"+w_w+""
                                         r"|FROM[\s]+"+w_w+""
                                         r"|JOIN[\s]+"+w_w+"", file_obj, flags=re.I|re.DOTALL)
            # print("find_objects_ip", find_objects_ip)
            if find_objects_ip:
                for ele in find_objects_ip:
                    # print(ele)
                    ele =re.sub(r"[\s]+", " ", ele)
                    input_log.append(ele)
                    if 'OVERWRITE TABLE' in ele.upper():
                        # print(ele)
                        ele = re.sub(r"OVERWRITE[\s]+TABLE[\s]+\`*|\`", "", ele, flags=re.I)
                        input_objects.append(ele.lower())
                        input_objects.append(ele.lower()) # in o/p 2 statements

                    else:            
                        ele = re.sub(r"VIEW[\s]+\`*|FROM[\s]+\`*|JOIN[\s]+\`*|\`", "", ele, flags=re.I)
                        input_objects.append(ele.lower())

        if filename in outputFile:
            file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
            file_obj = re.sub(r"EXTRACT\(.*?\)", "", file_obj, flags=re.I)

            find_objects_op = re.findall(r"TABLE[\s]+"+proj_id+"\`*[\w]+\`*\.\`*[\w]+\`*"
                                         r"|IF[\s]+NOT[\s]+EXISTS[\s]+"+proj_id+"\`*[\w]+\`*\.\`*[\w]+\`*"
                                         r"|INSERT[\s]+INTO[\s]+"+proj_id+"\`*[\w]+\`*\.\`*[\w]+\`*"
                                         r"|FROM[\s]+"+proj_id+"\`*[\w]+\`*\.\`*[\w]+\`*"
                                         r"|JOIN[\s]+"+proj_id+"\`*[\w]+\`*\.\`*[\w]+\`*", file_obj, flags=re.I|re.DOTALL)
            # print("find_objects_op", find_objects_op)
            
            if find_objects_op:
                for ele in find_objects_op:
                    ele =re.sub(r"[\s]+", " ", ele)
                    output_log.append(ele)
                    ele =re.sub(r"TABLE[\s]+"+proj_id+""
                                r"|IF[\s]+NOT[\s]+EXISTS[\s]+"+proj_id+""
                                r"|INSERT[\s]+INTO[\s]+"+proj_id+""
                                r"|FROM[\s]+"+proj_id+""
                                r"|JOIN[\s]+"+proj_id+"|\`", "", ele, flags=re.I)
                    
                    output_objects.append(ele.lower())
                
        input_objects=list(set(input_objects))
        output_objects=list(set(output_objects))

        # print("input_objects", len(input_objects), input_objects)
        # print("output_objects", len(output_objects), output_objects)   

        if len(input_objects) and len(output_objects)==0:
            log_result.append(Result_NA())
            result_match_table_view_count.append("NA")
        elif (len(input_objects)==len(output_objects)): #and (input_objects == output_objects):
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_match_table_view_count.append("Y("+str(len(input_objects))+"-"+str(len(output_objects))+")")
        else:
            log_result.append(Result_No(input_log,output_log))
            result_match_table_view_count.append("N("+str(len(input_objects))+"-"+str(len(output_objects))+")")        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_match_table_view_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_Match_join_count(log_result,filename,result_Match_join_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Match Join Count"))
    # print(Check_features_of_SF_to_BQ)
    input_joins=[]
    output_joins=[]
    input_log=[]
    output_log=[]
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        file_obj = re.sub(r"AS[\s]+[\w]+", "", file_obj, flags=flagID)
        file_obj = re.sub(r"\,[\s]*[\w]+", "", file_obj, flags=flagID)
        file_obj = re.sub(r"\-\-.*?\n", "", file_obj, flags=flagID)

        find_lv = re.findall(r"LATERAL[\s]+VIEW[\s]+EXPLODE"
                             r"|LATERAL[\s]+VIEW[\s]+OUTER[\s]+EXPLODE"
                             r"|LATERAL[\s]+VIEW[\s]+POST[\s]+EXPLODE"
                             r"|LATERAL[\s]+VIEW[\s]+OUTER[\s]+POST[\s]+EXPLODE", file_obj, flags=re.I|re.DOTALL)

        # print("find_lv", find_lv)
        if find_lv:
            for ele in find_lv:
                input_log.append(ele)
                ele = re.sub(r"LATERAL[\s]+VIEW[\s]+EXPLODE", "INNER JOIN UNNEST", ele, flags=re.I)
                ele = re.sub(r"LATERAL[\s]+VIEW[\s]+OUTER[\s]+EXPLODE", "LEFT JOIN UNNEST", ele, flags=re.I)
                ele = re.sub(r"LATERAL[\s]+VIEW[\s]+POST[\s]+EXPLODE", "INNER JOIN UNNEST", ele, flags=re.I)
                ele = re.sub(r"LATERAL[\s]+VIEW[\s]+OUTER[\s]+POST[\s]+EXPLODE", "LEFT OUTER JOIN UNNEST", ele, flags=re.I)
                ele = re.sub(r"[\s]+", " ", ele)
                input_joins.append(ele.upper())

        find_in_joins = re.findall(r"INNER[\s]+JOIN|LEFT[\s]+JOIN|RIGHT[\s]+JOIN|LEFT[\s]+OUTER[\s]+JOIN|FULL[\s]+OUTER[\s]+JOIN|[\s]+JOIN[\s]+", file_obj, flags=re.I|re.DOTALL)
        # print("find_in_joins", find_in_joins)

        if find_in_joins:
            for ele in find_in_joins:
                ele = re.sub(r"[\s]+", " ", ele)
                ele = re.sub(r"LEFT[\s]+OUTER[\s]+JOIN", "LEFT JOIN", ele, flags=re.I)
                ele = re.sub(r"FULL[\s]+OUTER[\s]+JOIN", "FULL JOIN", ele, flags=re.I)
                if ' JOIN ' == ele.upper():
                    input_log.append(ele.upper())
                    ele = re.sub(r"[\s]+JOIN[\s]+", "INNER JOIN", ele, flags=re.I)
                    # print(ele)
                    input_joins.append(ele.upper())
                else:
                    input_log.append(ele.upper())
                    input_joins.append(ele.upper())

        
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        find_delete = re.findall(r"DELETE[\s]+FROM", file_obj, flags=re.I|re.DOTALL)

        find_op_joins = re.findall(r"LEFT[\s]+JOIN[\s]+UNNEST"
                                   r"|INNER[\s]+JOIN[\s]+UNNEST"
                                   r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+UNNEST"
                                   r"|LEFT[\s]+OUTER[\s]+JOIN"
                                   r"|INNER[\s]+JOIN"
                                   r"|LEFT[\s]+JOIN"
                                   r"|RIGHT[\s]+JOIN"
                                   r"|FULL[\s]+JOIN"
                                   r"|[\s]+JOIN[\s]+", file_obj, flags=re.I|re.DOTALL)
        # print("find_op_joins", find_op_joins)

        if find_op_joins:
            for ele in find_op_joins:
                ele = re.sub(r"[\s]+", " ", ele)
                output_log.append(ele.upper())
                output_joins.append(ele.upper())

        input_joins.sort()
        output_joins.sort()

        # print("input_joins", len(input_joins), input_joins)
        # print("output_joins", len(output_joins), output_joins)   

        if len(input_joins)==0 and len(output_joins)==0:
            log_result.append(Result_NA())
            result_Match_join_count.append("NA")
        elif len(input_joins) == len(output_joins):
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_Match_join_count.append("Y("+str(len(input_joins))+"-"+str(len(output_joins))+")")
        elif len(input_joins) < len(output_joins) and find_delete:
            result_Match_join_count.append("CM")
            log_result.append(Result_CM("check Output file multiple joins bcz of select in delete statement."))
        else:
            log_result.append(Result_No(input_log,output_log))
            result_Match_join_count.append("N("+str(len(input_joins))+"-"+str(len(output_joins))+")")        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Match_join_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_del_ins_sol_for_partition_tbl(log_result,filename,result_del_ins_sol_for_partition_tbl):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("del_ins_sol_for_partition_tbl"))
    input = []
    output = []
    input_log = []
    output_log = []
    try:
        # Input 
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_insert_par_col = re.findall(r"INSERT[\s]+OVERWRITE[\s]+TABLE[\s]*\`*[\w]+\`*\.\`*[\w]+\`*[\s]+PARTITION[\s]*([\w]+)", file_obj, flags=re.I)
        
        if find_insert_par_col:
            input_log.append(find_insert_par_col[0])
            input.append(find_insert_par_col[0])
   

        find_insert_par_col1 = re.findall(r"INSERT[\s]+OVERWRITE[\s]+TABLE[\s]*\`*[\w]+\`*\.\`*[\w]+\`*[\s]+PARTITION[\s]*\((.*?)\)", file_obj, flags=re.I)
        
        if find_insert_par_col1:
            # print(find_insert_par_col1[0])
            input_log.append(find_insert_par_col1[0])
            ele = re.sub(r"[\s]+", "", find_insert_par_col1[0])
            input.append(find_insert_par_col1[0])
         

        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
        
        find_delete_par_col = re.findall(r"DELETE[\s]+FROM[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+[\s]*\`[\s]+AS[\s]+[\w]+[\s]+WHERE[\s]+([\w]+)", file_obj, flags=flagID)
        
        if find_delete_par_col:
            output_log.append(find_delete_par_col[0])
            output.append(find_delete_par_col[0])
         
        find_delete_par_col1 = re.findall(r"DELETE[\s]+FROM[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+[\s]*\`[\s]+AS[\s]+[\w]+[\s]+WHERE[\s]+STRUCT[\s]*\((.*?)\)", file_obj, flags=flagID)
        
        if find_delete_par_col1:
            if output[0] == 'STRUCT':
                output.remove('STRUCT')
                output_log.remove('STRUCT')

            output_log.append(find_delete_par_col1[0])
            ele = re.sub(r"[\s]+", "",find_delete_par_col1[0])
            output.append(ele)

        # print("input", input)
        # print("output", output)
            
        if len(input) == 0:
            log_result.append(Result_NA())
            result_del_ins_sol_for_partition_tbl.append("NA")
        elif input == output:
            # print("YYYY")
            result_del_ins_sol_for_partition_tbl.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_del_ins_sol_for_partition_tbl.append("N")
            log_result.append(Result_No(input_log,output_log))
                    
    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_del_ins_sol_for_partition_tbl.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_ins_sol_for_without_partition_tbl(log_result,filename,result_ins_sol_for_without_partition_tbl):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("ins_sol_for_without_partition_tbl"))
    input=[]
    output=[]
    input_log=[]
    output_log=[]
    try:
        if filename in inputFile:
            obj = common_obj.Remove_comments(inputFolderPath+filename)

            find_insert_par_col = re.findall(r"INSERT[\s]+OVERWRITE[\s]+TABLE[\s]*\`*[\w]+\`*\.\`*[\w]+\`*[\s]+PARTITION", obj, flags=re.I)

            find_insert_par_col1 = re.findall(r"INSERT[\s]+OVERWRITE[\s]+TABLE[\s]*\`*[\w]+\`*\.\`*[\w]+\`*", obj, flags=re.I)
            if find_insert_par_col1:
                # print("find_insert_par_col1", find_insert_par_col1)
                for ele in find_insert_par_col1:
                    input_log.append(ele.upper())
                    ele = re.sub(r"INSERT[\s]+OVERWRITE[\s]+TABLE[\s]*", "TRUNCATE TABLE `"+Append_project_id+".", ele, flags=re.I)
                    ele+='`'
                    # print("ele", ele)
                    input.append(ele.upper())
                
        if filename in outputFile:
            obj = common_obj.Remove_comments(OutputFolderPath+filename)
            find_truncate_statement = re.findall(r"TRUNCATE[\s]+TABLE[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+[\s]*\`", obj, flags=re.I|re.DOTALL)

            # print("find_truncate_statement", find_truncate_statement)
            if find_truncate_statement:
                for ele in find_truncate_statement:
                    ele = re.sub(r"[\s]+", " ", ele)
                    # print("ele", ele)
                    output.append(ele.upper())
                    output_log.append(ele.upper())
               
        # print("input", input)
        # print("output", output)

        if find_insert_par_col:
            log_result.append(Result_NA())
            result_ins_sol_for_without_partition_tbl.append("NA")
        elif input == output:
            # print("YYYY")
            result_ins_sol_for_without_partition_tbl.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_ins_sol_for_without_partition_tbl.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        # print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_ins_sol_for_without_partition_tbl.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_Append_project_ID_to_udf_function(log_result,filename,result_Append_project_ID_to_udf_function):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Udf function check"))
    input_udf_list = []
    output_udf_list = []
    Log_input=[]
    Log_output=[]
    try:
        query = common_obj.Remove_comments(OutputFolderPath+filename)

        # find_udf = re.findall(r"\`[a-zA-Z0-9-]+\.[\w]+\.[\w]+\`[\s]*\(", query, flags=re.DOTALL|re.I)
        # cf-raven-flipkart.fdp_udf.
        query = re.sub(r"FROM[\s]+\`.*?\`|JOIN[\s]+\`.*?\`|INTO[\s]+\`.*?\`|TABLE[\s]+\`.*?\`", "", query, flags=re.DOTALL|re.I)
        find_udf = re.findall(r"\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]*\(", query, flags=re.DOTALL|re.I)
        # print(find_udf)
        for ele in find_udf:
            if 'DESER_' in ele or 'SERIALIZE' in ele:
                pass
            else:
                Log_output.append(ele)
                ele = re.sub(r'\(', '', ele, flags=re.DOTALL|re.I)
                # print(ele)
                output_udf_list.append(ele.lower())


        # if filename in inputFile:
        query = common_obj.Remove_comments(inputFolderPath+filename)

        find_udf_name = re.findall(r"[\W]business_date_diff[\s]*\(|"
                                   r"[\W]ceil[\s]*\(|"
                                   r"[\W]lookup_date[\s]*\(|"
                                   r"[\W]lookupkey[\s]*\(|"
                                   r"[\W]aggregate_filter[\s]*\(|"
                                   r"[\W]aggreggate_filter_list[\s]*\(|"
                                   r"[\W]agg_theta[\s]*\(|"
                                   r"[\W]combine[\s]*\(|"
                                   r"[\W]dayofweek[\s]*\(|"
                                   r"[\W]index_of[\s]*\(|"
                                   r"[\W]isotosqlts[\s]*\(|"
                                   r"[\W]json_map[\s]*\(|"
                                   r"[\W]normalize[\s]*\(|"
                                   r"[\W]to_json[\s]*\(|"
                                   r"[\W]arrToString[\s]*\(|"
                                   r"[\W]stringCaster[\s]*\(|"
                                   r"[\W]aggregate[\s]*\(|"
                                   r"[\W]lookup_time[\s]*\(|"
                                   r"[\W]uuid[\s]*\(|"
                                   r"[\W]indexOf[\s]*\(|"
                                   r"[\W]aggregate_filter_list[\s]*\(|"
                                   r"[\W]tojson[\s]*\(", query, flags=re.I|re.DOTALL)  #r"[\W]timestamp[\s]*\(|"

        if find_udf_name:
            # print(find_udf_name)
            for ele in find_udf_name:
                ele =re.sub(r"^[\W]|[\s]*\(", "", ele)
                Log_input.append(ele)
                ele1 = '`'+Append_project_id+'.fdp_udf.' + ele + '`'
                # print("ele1", ele1)
                input_udf_list.append(ele1.lower())
                

        # print("input_all_datatype =", len(input_udf_list), input_udf_list)
        # print("output_all_datatype=", len(output_udf_list), output_udf_list)
        # print(set(output_udf_list).issubset(input_udf_list))

        if len(input_udf_list)==0 and len(output_udf_list)==0:
            log_result.append(Result_NA())
            result_Append_project_ID_to_udf_function.append("NA")
        elif list(set(input_udf_list)) == list(set(output_udf_list)):#set(output_udf_list).issubset(input_udf_list)==True:
            # print("YYYY")
            log_result.append(Result_Yes(list(set(Log_input)),list(set(Log_output))))
            result_Append_project_ID_to_udf_function.append("Y")
        else:
            log_result.append(Result_No(list(set(Log_input)),list(set(Log_output))))
            result_Append_project_ID_to_udf_function.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_Append_project_ID_to_udf_function.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_safe_with_function(log_result,filename,result_safe_with_function):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check safe with function"))
    input_lst=[]
    output_lst=[]
    input_log=[]
    try:
        if filename in outputFile:
            obj = common_obj.Remove_comments(OutputFolderPath+filename)
            find_safe_func = re.findall(r"SAFE\.[\w]+[\s]*\(", obj, flags=re.I|re.DOTALL)
            # print("find_safe_func", find_safe_func)

            if find_safe_func:
                output_lst=find_safe_func

        if find_safe_func:
            log_result.append(Result_No(input_log,output_lst))
            result_safe_with_function.append("N")
        else:
            log_result.append(Result_NA())
            result_safe_with_function.append("NA")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_safe_with_function.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_lateral_view(log_result,filename,result_lateral_view):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Lateral_View"))
    input_lv=[]
    output_lv=[]
    input_log=[]
    output_log=[]
    try:
        if filename in inputFile:
            file_obj = common_obj.Read_file(inputFolderPath+filename)
        
            find_lv = re.findall(r"LATERAL[\s]+VIEW[\s]+EXPLODE[\s]*\("
                                 r"|LATERAL[\s]+VIEW[\s]+OUTER[\s]+EXPLODE[\s]*\("
                                 r"|LATERAL[\s]+VIEW[\s]+POST[\s]+EXPLODE[\s]*\("
                                 r"|LATERAL[\s]+VIEW[\s]+OUTER[\s]+POST[\s]+EXPLODE[\s]*\(", file_obj, flags=re.I|re.DOTALL)

            # print("find_lv", find_lv)
            if find_lv:
                for ele in find_lv:
                    input_log.append(ele)
                    ele = re.sub(r"LATERAL[\s]+VIEW[\s]+EXPLODE[\s]*", "INNER JOIN UNNEST ", ele, flags=re.I)
                    ele = re.sub(r"LATERAL[\s]+VIEW[\s]+OUTER[\s]+EXPLODE[\s]*", "LEFT JOIN UNNEST ", ele, flags=re.I)
                    ele = re.sub(r"LATERAL[\s]+VIEW[\s]+POST[\s]+EXPLODE[\s]*", "INNER JOIN UNNEST ", ele, flags=re.I)
                    ele = re.sub(r"LATERAL[\s]+VIEW[\s]+OUTER[\s]+POST[\s]+EXPLODE[\s]*", "LEFT OUTER JOIN UNNEST ", ele, flags=re.I)
                    # print(ele)
                    ele = re.sub(r"[\s]+|\`", "", ele)
                    input_lv.append(ele.lower())


        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            find_joins = re.findall(r"INNER[\s]+JOIN[\s]+UNNEST[\s]*\("
                                    r"|LEFT[\s]+JOIN[\s]+UNNEST[\s]*\("
                                    r"|INNER[\s]+JOIN[\s]+UNNEST[\s]*\("
                                    r"|LEFT[\s]+OUTER[\s]+JOIN[\s]+UNNEST[\s]*\(", file_obj, flags=re.I|re.DOTALL)
            
            if find_joins:
                for ele in find_joins:
                    # print("ele", ele)
                    output_log.append(ele)
                    ele = re.sub(r"[\s]+", "", ele)
                    output_lv.append(ele.lower())

        input_lv = list(set(input_lv))
        output_lv = list(set(output_lv))

        # print("input_lv=", len(input_lv), input_lv)
        # print("output_lv=", len(output_lv),output_lv)

        if len(input_lv)==0 and len(output_lv)==0:
            result_lateral_view.append("NA")
            log_result.append(Result_NA())
        elif input_lv == output_lv:
            # print("YYYY")
            result_lateral_view.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_lateral_view.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_lateral_view.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_match_schema_table_case(log_result,filename,result_match_schema_table_case):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("match_schema_table_case"))
    input_schema_table = []
    output_schema_table = []
    Log_input = []
    Log_output = []
    try:
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_schema_table_ip = re.findall(r"IF[\s]+NOT[\s]+EXISTS[\s]+\`*[\w]+\.[\w]+\`*"
                                        r"|REPLACE[\s]+VIEW[\s]+\`[\w]+\.[\w]+\`"
                                        r"|CREATE[\s]+VIEW[\s]+\`*[\w]+\`*\.\`*[\w]+\`*"
                                        r"|INSERT[\s]+OVERWRITE[\s]+TABLE[\s]+\`*[\w]+\`*\.\`*[\w]+\`*", file_obj, flags=re.I)
            
        # print("find_schema_table_ip", find_schema_table_ip)
        for ele in find_schema_table_ip:
            ele = re.sub(r"IF[\s]+NOT[\s]+EXISTS[\s]*|REPLACE[\s]+VIEW[\s]+|CREATE[\s]+VIEW[\s]+|INSERT[\s]+OVERWRITE[\s]+TABLE[\s]+|\`", "", ele, flags=re.I)
            input_schema_table.append(ele)
            Log_input.append(ele)


        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
        # print(Append_project_id)

        find_schema_table_op = re.findall(r"IF[\s]+NOT[\s]+EXISTS[\s]+\`*"+Append_project_id+"\.[\w]+\.[\w]+\`*"
                                        r"|REPLACE[\s]+VIEW[\s]+\`*"+Append_project_id+"\.[\w]+\.[\w]+\`*"
                                        r"|INSERT[\s]+INTO[\s]+\`*"+Append_project_id+"\.[\w]+\.[\w]+\`*", file_obj, flags=re.I)
        # print(find_schema_table_op)
        for ele in find_schema_table_op:
            ele = re.sub(r"IF[\s]+NOT[\s]+EXISTS[\s]*|REPLACE[\s]+VIEW[\s]+|INSERT[\s]+INTO[\s]+|"+Append_project_id+"\.|\`","",ele,flags=re.I)
            output_schema_table.append(ele)
            Log_output.append(ele)

        # print("input_schema_table-", input_schema_table)
        # print("output_schema_table-",output_schema_table)

        if len(input_schema_table) == 0 and len(output_schema_table) == 0:
            log_result.append(Result_NA())
            result_match_schema_table_case.append("NA")
        elif input_schema_table == output_schema_table:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_match_schema_table_case.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_match_schema_table_case.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_match_schema_table_case.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_Check_table_alias_column(log_result,filename,result_check_table_alias):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Table Alias Column Checking"))
    input_table_alias_column=[]
    output_table_alias_column=[]
    Log_input=[]
    Log_output=[]
    try:
        # Output
        fileLines = common_obj.Read_file(OutputFolderPath+filename)
        find_table_alias_op_0 = re.findall(r"FROM[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]+AS[\s]+[\w]+|JOIN[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]+AS[\s]+[\w]+",fileLines,flags=re.I|re.DOTALL)
        if find_table_alias_op_0:
            for ele in find_table_alias_op_0:
                # print("ele", ele)
                ele = re.sub(r"FROM[\s]+|JOIN[\s]+|\`", "", ele, flags=re.I)
                ele = re.sub(r""+Append_project_id+"\.[\w]+\.", "", ele, flags=re.I)
                ele = re.sub(r"[\s]+", " ", ele, flags=re.I)

                input_table_alias_column.append('AS '+ele.upper())
                Log_input.append('AS '+ele.upper())

        find_table_alias_op = re.findall(r"FROM[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]+AS[\s]+[\w]+|JOIN[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]+AS[\s]+[\w]+",fileLines,flags=re.I|re.DOTALL)

        if find_table_alias_op:
            for ele in find_table_alias_op:
                ele = re.sub(r"FROM[\s]+|JOIN[\s]+", "", ele, flags=re.I)
                ele = re.sub(r"\`"+Append_project_id+"\.[\w]+\.[\w]+\`[\s]+", "", ele, flags=re.I)
                ele = re.sub(r"[\s]+", " ", ele, flags=re.I)
                
                output_table_alias_column.append(ele)
                Log_output.append(ele.upper())

       
        # print("input_all_datatype =", len(input_table_alias_column), input_table_alias_column)
        # print("output_all_datatype=", len(output_table_alias_column), output_table_alias_column)

        if len(input_table_alias_column)==0: #and len(output_table_alias_column)==0:
            log_result.append(Result_NA())
            result_check_table_alias.append("NA")
        elif len(input_table_alias_column) > len(output_table_alias_column):
            result_check_table_alias.append("CM")
            log_result.append(Result_CM("Check Input Output file for alias."))
        elif len(input_table_alias_column) == len(output_table_alias_column) and input_table_alias_column != output_table_alias_column:
            result_check_table_alias.append("CM")
            log_result.append(Result_CM("Check from Input file taking default alias in output."))
        elif input_table_alias_column==output_table_alias_column:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_table_alias.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_table_alias.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_table_alias.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_schema_in_seriliser_deser(log_result,filename,result_check_schema_in_seriliser_deser):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_schema_append_in_seriliser_deseriliser"))
    try:
    # if 1==1:
        if filename in outputFile:
            fileLines = common_obj.Readlines_file(OutputFolderPath+filename)
            # counter=0
            comment=0
            obj=''
            for fileLine in fileLines:
                # print(fileLine)
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
            # print("obj", obj)
            find_serialize_deser = re.findall(r"DESER_[\d]+[\s]*[\W]+|SERIALIZE[\s]*[\W]+", obj, flags=re.I|re.DOTALL)
            # print("find_serialize_deser", len(find_serialize_deser), find_serialize_deser)
            # print(Append_project_id)

            find_serialize_deser1 = re.findall(r"\`"+Append_project_id+"\.fdp_udf\.DESER"
                                               r"|\`"+Append_project_id+"\.fdp_udf\.SERIALIZE", obj, flags=re.I|re.DOTALL)
            # print("find_serialize_deser1", len(find_serialize_deser1), find_serialize_deser1)


        if len(find_serialize_deser)==0 and len(find_serialize_deser1)==0:
            log_result.append(Result_NA())
            result_check_schema_in_seriliser_deser.append("NA")
        elif len(find_serialize_deser)==len(find_serialize_deser1):
            # print("YYYY")
            log_result.append(Result_Yes(['Check Output File'],find_serialize_deser1))
            result_check_schema_in_seriliser_deser.append("Y")
        else:
            log_result.append(Result_No(['Check Output File'],find_serialize_deser1))
            result_check_schema_in_seriliser_deser.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_schema_in_seriliser_deser.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_comparison_par_col_using_sheet(log_result,filename,result_check_comparesion_par_col_using_sheet,table_column_dict):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("comparesion_par_col_using_sheet"))
    input_list=[]
    output_list=[]
    try:
        #Input
        # file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        # for ele in table_column_dict:

        #     find_table = re.findall(r"FROM[\s]+\`*[\w]+\."+ele.split('::')[0]+"\`*", file_obj, flags=re.I|re.DOTALL)
        #     # print("find_table", find_table)

        #     if find_table:
        #         find_col = re.findall(ele.split('::')[1]+"[\s]+[<>=]+[\s]+\d{8}"
        #                               , file_obj, flags=re.I|re.DOTALL)   # "|"+ele.split('::')[1]+"[\s]+[<>=]+[\s]+CURRENT\_DATE"

        #         # print("find_col", find_col)
        #         for ele in find_col:
        #             input_list.append(ele)
        
        #Output
        table=[]
        col=[]
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  

        for ele in table_column_dict:

            find_table = re.findall(r"FROM[\s]+\`*[\w]+\."+ele.split('::')[0]+"\`*", file_obj, flags=re.I|re.DOTALL)
            # print("find_table", find_table)

            if find_table:
                table.append(find_table[0])
                find_col = re.findall(ele.split('::')[1]+"[\s]+[<>=]+[\s]+date[\s]+\'\d{4}\-\d{2}\-\d{2}\'"
                                      "|"+ele.split('::')[1]+"[\s]+[<>=]+[\s]+CURRENT\_DATE", file_obj, flags=re.I|re.DOTALL)  # "|"+ele.split('::')[1]+"[\s]+[<>=]+[\s]+CURRENT\_DATE"
                # print("find_col", find_col)

                if find_col:
                    for ele in find_col:
                        # print(ele)
                        col.append(ele)
                        if 'CURRENT_DATE' in ele.upper():
                            ele = re.sub(r'fdp\_udf\.[\w]+[\s]*\(|\)', '', ele, flags=re.I|re.DOTALL)
                            output_list.append(ele)

                        else:
                            output_list.append(ele)

        input_list.sort()
        output_list.sort()

        # print("input_list=", len(list(set(input_list))), list(set(input_list)))
        # print("output_list=", len(list(set(output_list))),list(set(output_list)))

        # print(table)
        # print(col)

        if len(table)==0 and len(col)==0:
            result_check_comparesion_par_col_using_sheet.append("NA")
            log_result.append(Result_NA())
        elif table and col:  #len(list(set(input_list))) == len(list(set(output_list))):
            # print("YYYY")
            result_check_comparesion_par_col_using_sheet.append("Y")
            log_result.append(Result_Yes(col,col))
        else:
            result_check_comparesion_par_col_using_sheet.append("N")
            log_result.append(Result_No(col,col))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_comparesion_par_col_using_sheet.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_nullif_column(log_result,filename,result_check_nullif_column):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_insert_column_names"))
    not_nullif_columns=[]
    nullif_columns=[]
    input_log=[]
    output_log=[]
    try:
        # Input
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=flagD)  #rmv inline comments
        # find_slash1 = re.findall(r"[\w]+\\\'", file_obj, flags=flagID)
        # print(find_slash1)
        file_obj = re.sub(r"[\w]+\\\'", "", file_obj, flags=re.DOTALL) #rmv ''
        file_obj = re.sub(r"\'.*?\'", "", file_obj, flags=re.DOTALL) #rmv ''
        # print("file_obj", file_obj)
        
        find_slash = re.findall(r"\/", file_obj, flags=flagID)
        # print("find_slash", len(find_slash), find_slash)

        if find_slash:
            find_slash_col = re.findall(r"\/[\s]*[\w]+", file_obj, flags=flagID)
            # print("find_slash_col", find_slash_col)
            if find_slash_col:
                for ele in find_slash_col:
                    ele = re.sub(r"[\s]+|[\d]+", "", ele)
                    ele = ele.split('/')[1]
                    
                    if '' == ele:
                        pass
                    elif 'NULLIF' in ele.upper() or ele.isdigit():
                        nullif_columns.append(ele)
                        output_log.append(ele)
                        
                        pass
                    else:
                        # print("ele", ele)
                        not_nullif_columns.append(ele)
                        
                        input_log.append(ele)

       
        not_nullif_columns.sort()
        nullif_columns.sort()

        # print("not_nullif_columns=", len(not_nullif_columns), not_nullif_columns)
        # print("nullif_columns=", len(nullif_columns), nullif_columns)

        if len(not_nullif_columns)==0 and len(nullif_columns)==0:
            result_check_nullif_column.append("NA")
            log_result.append(Result_NA())
        elif len(not_nullif_columns) == 0 and nullif_columns:
            # print("YYYY")
            result_check_nullif_column.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_check_nullif_column.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_nullif_column.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_limit_count(log_result,filename,result_check_limit_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Match limit Count"))
    input_limits = []
    output_limits = []
    input_log = []
    output_log = []
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_limit_ip = re.findall(r"LIMIT[\s]+[\d]+", file_obj, flags=flagID)
        # print("find_limit_ip", find_limit_ip)
        if find_limit_ip:
            for ele in find_limit_ip:
                input_log.append(ele)
                input_limits.append(ele.lower())

        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
     
        find_limit_op = re.findall(r"LIMIT[\s]+[\d]+", file_obj, flags=flagID)
        # print("find_limit_op", find_limit_op)
        
        if find_limit_op:
            for ele in find_limit_op:
                output_log.append(ele)
                output_limits.append(ele.lower())
                
        input_limits.sort()
        output_limits.sort()

        # print("input_limits", len(input_limits), input_limits)
        # print("output_limits", len(output_limits), output_limits) 

        if len(input_limits)==0 and len(output_limits)==0:
            log_result.append(Result_NA())
            result_check_limit_count.append("NA")
        elif (len(input_limits)==len(output_limits)): #and (input_limits == output_limits):
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_check_limit_count.append("Y("+str(len(input_limits))+"-"+str(len(output_limits))+")")
        else:
            log_result.append(Result_No(input_log,output_log))
            result_check_limit_count.append("N("+str(len(input_limits))+"-"+str(len(output_limits))+")")        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_limit_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_parse_date_to_case_when(log_result,filename,result_check_parse_date_to_case_when):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Parse_date to Case When"))
    case_when_not_present = []
    case_when_present = []
    input_log = []
    output_log = []
    try:
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
     
        find_case_when = re.findall(r"PARSE_DATETIME[\s]*\(.*?\)|PARSE_DATE[\s]*\(.*?\)", file_obj, flags=flagID)
        # print("find_case_when", find_case_when)
        
        if find_case_when:
            for ele in find_case_when:
                # print(ele)
                find_per_f = re.findall(r"\%F", ele, flags=flagI)
                if find_per_f:
                    pass
                else:
                    if 'case when length' in ele.lower():
                        output_log.append(ele)
                        # print(ele)
                        case_when_present.append(ele.lower())
                    else:
                        output_log.append(ele)
                        case_when_not_present.append(ele.lower())
                
        case_when_not_present.sort()
        case_when_present.sort()

        # print("case_when_not_present", len(case_when_not_present), case_when_not_present)
        # print("case_when_present", len(case_when_present), case_when_present) 

        if len(case_when_not_present)==0 and len(case_when_present)==0:
            log_result.append(Result_NA())
            result_check_parse_date_to_case_when.append("NA")
        elif not case_when_not_present and  case_when_present:
            # print("YYYY")
            log_result.append(Result_Yes(output_log,output_log))
            result_check_parse_date_to_case_when.append("Y")
        else:
            log_result.append(Result_No(output_log,output_log))
            result_check_parse_date_to_case_when.append("N")        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_parse_date_to_case_when.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_match_cte_table_count(log_result,filename,result_fkt_match_cte_table_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Match_cte_table_count"))
    input_cte = []
    output_cte = []
    input_log = []
    output_log = []
    try:
        #Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=flagD)  #rmv inline comments
        # print("file_obj", file_obj)
        
        find_cte_in = re.findall(r"WITH[\s]+[\w]+[\s]+AS[\s]+|\)[\s]*\,[\s]*[\w]+[\s]+AS[\s]*", file_obj, flags=flagID)
        # print("find_cte_in", find_cte_in)

        if find_cte_in:
            for ele in find_cte_in:
                input_log.append(ele.lower())
                ele = re.sub(r"AS[\s]*$", "", ele, flags=flagI)
                ele = re.sub(r"WITH[\s]+|[\s]+AS[\s]+|\n|\)[\s]*\,", "", ele, flags=flagI)
                # print(ele)
                ele = re.sub(r"[\s]+", "", ele)
                input_cte.append(ele.lower())
    
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  
        file_obj = re.sub(r"DELETE[\s]+FROM.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)
        file_obj = re.sub(r"TRUNCATE[\s]+TABLE.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)

        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # print("file_obj", file_obj)

        find_cte_op = re.findall(r"WITH[\s]+[\w]+[\s]+AS[\s]+|\)[\s]*\,[\s]*[\w]+[\s]+AS[\s]*", file_obj, flags=flagID)

        if find_cte_op:
            for ele in find_cte_op:
                output_log.append(ele.lower())
                ele = re.sub(r"WITH[\s]+|[\s]+AS[\s]+|\n|\)[\s]*\,", "", ele, flags=flagI)
                # print(ele)
                ele = re.sub(r"[\s]+", "", ele)
                output_cte.append(ele.lower())


        input_cte.sort()
        output_cte.sort()

        # print("input_cte=", len(input_cte), input_cte)
        # print("output_cte=", len(output_cte),output_cte)

        if len(input_cte)==0 and len(output_cte)==0:
            result_fkt_match_cte_table_count.append("NA")
            log_result.append(Result_NA())
        elif input_cte == output_cte:
            # print("YYYY")
            result_fkt_match_cte_table_count.append("Y("+str(len(input_cte))+"-"+str(len(output_cte))+")")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_fkt_match_cte_table_count.append("N("+str(len(input_cte))+"-"+str(len(output_cte))+")")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_fkt_match_cte_table_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_match_case_when_count(log_result, filename, result_fkt_match_case_when_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Match_case_when_count"))
    input_case_when = []
    output_case_when = []
    input_log = []
    output_log = []
    try:
        #Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=flagD)
        
        find_cw_in = re.findall(r"CASE[\s]+WHEN[\s]+", file_obj, flags=flagID)
        # print("find_cw_in", len(find_cw_in), find_cw_in)

        find_en_in = re.findall(r"[\s]+END", file_obj, flags=flagID)
        # print("find_en_in", len(find_en_in), find_en_in)

        if len(find_cw_in) == len(find_en_in):
            input_case_when = find_cw_in
            input_log = find_cw_in
        else:
            input_case_when = find_cw_in
            input_log = find_cw_in

    
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  
        file_obj = re.sub(r"DELETE[\s]+FROM.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)
        file_obj = re.sub(r"TRUNCATE[\s]+TABLE.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)

        file_obj = re.sub(r"PARSE_DATETIME[\s]*\([\s]*CASE[\s]+WHEN.*?end|PARSE_DATE[\s]*\([\s]*CASE[\s]+WHEN.*?end", "", file_obj, flags=flagID)

        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)

        find_cw_op = re.findall(r"CASE[\s]+WHEN", file_obj, flags=flagID)
        # print("find_cw_op", len(find_cw_op), find_cw_op)

        find_en_op = re.findall(r"[\s]+END", file_obj, flags=flagID)
        # print("find_en_op", len(find_en_op), find_en_op)

        if len(find_cw_op) == len(find_en_op):
            output_case_when = find_cw_op
            output_log = find_cw_op
        else:
            output_case_when = find_cw_op
            output_log = find_cw_op


        input_case_when.sort()
        output_case_when.sort()

        # print("input_case_when=", len(input_case_when), input_case_when)
        # print("output_case_when=", len(output_case_when),output_case_when)

        if len(find_cw_in)==0 and len(find_cw_op)==0:
            result_fkt_match_case_when_count.append("NA")
            log_result.append(Result_NA())
        elif len(input_case_when) == len(output_case_when):
            # print("YYYY")
            result_fkt_match_case_when_count.append("Y("+str(len(input_case_when))+"-"+str(len(output_case_when))+")")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_fkt_match_case_when_count.append("N("+str(len(input_case_when))+"-"+str(len(output_case_when))+")")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_fkt_match_case_when_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

# def parse_sql_hv(sql_obj,group_by_column_count,group_by_column):
#     parsed = sqlparse.parse(sql_obj)
#     # print("parsed", parsed)
#     for ele in parsed:
#         # print("ele", ele)
#         group_by_found=0
#         for i in ele.tokens:
#             # print("TTTTTT",i)
#             str_i=str(i).strip().upper()
#             # print("str_i",str_i)
#             if str_i != '':
#                 # print("str_i",str_i)
#                 if group_by_found == 0:
#                     find_group_by=re.findall(r'^GROUP[\s]+BY$',str_i,flags=re.I|re.DOTALL)
#                     # print("find_group_by", find_group_by)
#                     if find_group_by:
#                         # print("tt")
#                         group_by_found=1
#                     else:
#                         find_select=re.findall(r'\([\s]*SELECT[\s]+|\([\s]*SEL[\s]+',str_i,flags=re.I|re.DOTALL)
#                         find_group_by=re.findall(r'GROUP[\s]+BY',str_i,flags=re.I|re.DOTALL)
#                         if find_select and find_group_by:
#                             # print("ss")
#                             # select_list.append(str_i)
#                             # print("str_i",str_i)
#                             select_str=''
#                             # print(find_select[0])
#                             find_index=str_i.index(find_select[0])
#                             # print(find_index)
#                             b_o=0
#                             b_c=0
#                             for i in str_i[find_index+1:]:
#                                 # print(i)
#                                 if i == '(':
#                                     b_o+=1
#                                 elif i == ')':
#                                     b_c+=1
#                                 if b_o == b_c-1:
#                                     # print(select_str)
#                                     find_group_by=re.findall(r'GROUP[\s]+BY',select_str,flags=re.I|re.DOTALL)
#                                     # print(find_group_by)
#                                     if find_group_by:
#                                         parse_sql_hv(select_str,group_by_column_count,group_by_column)
#                                     break
#                                 else:
#                                     select_str+=i
#
#                 elif group_by_found ==1:
#                     # print(str_i)
#                     group_by_found=0
#                     str_i=re.sub(r'[\s]+',' ',str_i.upper(),flags=re.I|re.DOTALL)
#                     str_i=re.sub(r'[\s]*\,[\s]*',',',str_i,flags=re.I|re.DOTALL)
#                     str_i=re.sub(r'QUALIFY[\s]*$','',str_i,flags=re.I|re.DOTALL)
#                     fun_to_remove=['LOWER','UPPER']
#                     for fun in fun_to_remove:
#                         # print(fun)
#                         find_fun_to_remove=re.findall(r'('+fun+'[\s]*\(((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*)\))',str_i,flags=re.I|re.DOTALL)
#                         for ele in find_fun_to_remove:
#                             str_i=str_i.replace(ele[0],ele[1])
#                     # for fun in fun_to_remove:
#                     #     str_i=re.sub(r''+fun+'[\s]*\(',fun+'(',str_i,flags=re.I|re.DOTALL)
#                     #     find_fun=re.findall(r''+fun+'\(',str_i,flags=re.I|re.DOTALL)
#                     #     # remove_fun=str_i
#                     #     for ele in find_fun:
#                     #         # print(str_i)
#                     #         # print(ele)
#                     #         find_index=str_i.find(ele)
#                     #         if find_index == -1:
#                     #             break
#                     #         b_o=0
#                     #         b_c=0
#                     #         fun_str=''
#                     #         for i in str_i[find_index:]:
#                     #             fun_str+=i
#                     #             if i == '(':
#                     #                 b_o+=1
#                     #             elif i == ')':
#                     #                 b_c+=1
#                     #             if b_o == b_c:
#                     #                 remove_fun=re.sub(r'^'+fun+'\(|\)$','',fun_str)
#                     #                 str_i=str_i.replace(fun_str,remove_fun)
#                     # print(str_i)
#                     column_lst=[]
#                     column_str=''
#                     b_o=0
#                     b_c=0
#                     for i in str_i:
#                         if i == '(':
#                             b_o+=1
#                         elif i == ')':
#                             b_c+=1
#                         if i==',' and b_o == b_c:
#                             b_o=0
#                             b_c=0
#                             column_lst.append(column_str)
#                             column_str=''
#                         else:
#                             column_str+=i
#                     column_lst.append(column_str)
#                     column_lst.sort()
#                     new_str=','
#                     # print(new_str)
#                     new_str=new_str.join(column_lst)
#                     # print(new_str)
#                     group_by_column_count.append(len(column_lst))
#                     print("column_lst", column_lst)
#                     group_by_column.append(""+str(len(column_lst))+" Columns In GROUP BY "+new_str)
def parse_sql_hv(sql_obj,group_by_column_count,group_by_column):
    parsed = sqlparse.parse(sql_obj)[0]
    # print(parsed.tokens)
    group_by_found=0
    for ele in parsed.tokens:
        if str(ele).strip() != '':
            # print(ele.ttype,ele)
            if group_by_found==0:
                # print("SSSS", ele.ttype, ele)
                if isinstance(ele, sqlparse.sql.Parenthesis) or ele.ttype is None and re.findall(r'^[\s]*\([\s]*SELECT|^[\s]*\([\s]*SEL[\s]+|^WHERE[\s]+',str(ele),flags=re.I|re.DOTALL):
                    # print("TTTTTT", ele)
                    ele = re.sub(r'^[\s]*\(|\)[\s]*[\s]*[\w]*$', '', str(ele))
                    
                    # ele = ele.replace('\n', ' ')
                    # if ele.upper().startswith('SELECT'):
                    #     # print("WW")
                    #     gpby = re.findall(r"(GROUP[\s]+BY.*)", ele, flags=flagI)
                    #     print(gpby)
                    #     parse_sql(gpby[0],group_by_column_count,group_by_column)


                    parse_sql(str(ele),group_by_column_count,group_by_column)
                    
                elif re.findall(r'^GROUP[\s]+BY$',str(ele),flags=re.I|re.DOTALL):
                    # print("SS")
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

def fun_fkt_check_group_by_in_file(log_result, filename, result_fkt_check_group_by_in_file):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check GROUP BY Column Count"))
    input_group_by_column=[]
    output_group_by_column=[]
    input_group_by_column_count=[]
    output_group_by_column_count=[]
    try:
    # if 1==1:
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        # splitted_sql = regex.findall(r"\((?:[^()]+|(?R))*\)", file_obj, flags=flagID)
        # print(splitted_sql)
        # for ele in splitted_sql:
        #     if 'group by' in ele.lower():
        #         print(ele)
        #         group_by_column_count = []
        #         group_by_column = []
                # parse_sql_hv(ele, group_by_column_count, group_by_column)
                # print(group_by_column)
                # print(len(group_by_column))

        file_obj = re.sub(r'\([\s]*CASE[\s]+WHEN', 'CASE WHEN', file_obj, flags=flagID)
        file_obj = re.sub(r'[\s]+END[\s]*\)', ' END', file_obj, flags=flagID)

        find_cte_table = re.findall(r"WITH[\s]+[\w]+[\s]+AS[\s]+", file_obj, flags=flagID)
        # print(find_cte_table)
        if find_cte_table:
            find_nested_cte = re.findall(r"\)[\s]*\,[\s]*[\w]+[\s]+AS[\s]*", file_obj, flags=flagID)
            # print(find_nested_cte)
            if find_nested_cte:
                for ele in find_nested_cte:
                    new_ele = re.sub(r"\)[\s]*\,[\s]*", "), \nWITH ", ele)
                    file_obj = file_obj.replace(ele, new_ele)
                    # print(file_obj)
                    # print(ele)
                    # print(new_ele)

        group_by_column_count = []
        group_by_column = []
        select_list = []
        for sql in sqlparse.split(file_obj):
            # print("SQL", sql)
            parse_sql_hv(sql, group_by_column_count, group_by_column)
            # print(select_list)


        # print("group_by_column_count", group_by_column_count)
        input_group_by_column_count = group_by_column_count
        input_group_by_column = group_by_column
  
        
        # Output
        OutputFileObj = common_obj.Remove_comments(OutputFolderPath+filename)

        OutputFileObj=re.sub(r'\([\s]*CASE[\s]+WHEN','CASE WHEN',OutputFileObj,flags=re.I|re.DOTALL)
        OutputFileObj=re.sub(r'[\s]+END[\s]*\)',' END',OutputFileObj,flags=re.I|re.DOTALL)

        find_cte_table = re.findall(r"WITH[\s]+[\w]+[\s]+AS[\s]+", OutputFileObj, flags=flagID)
        # print(find_cte_table)
        if find_cte_table:
            find_nested_cte = re.findall(r"\)[\s]*\,[\s]*[\w]+[\s]+AS[\s]*", OutputFileObj, flags=flagID)
            # print(find_nested_cte)
            if find_nested_cte:
                for ele in find_nested_cte:
                    new_ele = re.sub(r"\)[\s]*\,[\s]*", "), \nWITH ", ele)
                    OutputFileObj = OutputFileObj.replace(ele, new_ele)
                    # print(OutputFileObj)
                    # print(ele)
                    # print(new_ele)


        group_by_column_count = []
        group_by_column = []
        for sql in sqlparse.split(OutputFileObj):
            # print("OP")
            parse_sql_hv(sql,group_by_column_count,group_by_column)

        output_group_by_column_count = group_by_column_count
        output_group_by_column = group_by_column
        
        
        input_group_by_column.sort()
        output_group_by_column.sort()

        # print(len(input_group_by_column), input_group_by_column)
        # print(len(output_group_by_column), output_group_by_column)

        if len(input_group_by_column) == 0 and len(output_group_by_column) == 0:
            log_result.append(Result_NA())
            result_fkt_check_group_by_in_file.append("NA")
        elif input_group_by_column_count == output_group_by_column_count:  #len(input_group_by_column) == len(output_group_by_column) and
            # print("YYYY")
            log_result.append(Result_Yes(input_group_by_column,output_group_by_column))
            result_fkt_check_group_by_in_file.append("Y("+str(len(input_group_by_column_count))+"/"+str(len(output_group_by_column_count))+")")
        else:
            log_result.append(Result_No(input_group_by_column,output_group_by_column))
            result_fkt_check_group_by_in_file.append("N("+str(len(input_group_by_column_count))+"/"+str(len(output_group_by_column_count))+")")

    except Exception as e:
        print(e)
        print("Unexpected error:", sys.exc_info()[0])
        result_fkt_check_group_by_in_file.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_truncate_with_begin_transaction(log_result,filename,result_fkt_check_truncate_with_begin_transaction):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Trancate with transaction"))
    input_cte = []
    output_cte = []
    input_log = []
    output_log = []
    try:
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  
        # file_obj = re.sub(r"DELETE[\s]+FROM.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)
        # file_obj = re.sub(r"TRUNCATE[\s]+TABLE.*?\;[\s]+INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=flagID)

        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # print("file_obj", file_obj)

        find_bt = re.findall(r"Begin[\s]+Transaction[\s]*\;", file_obj, flags=flagID)
        # print(find_bt)

        find_trancate = re.findall(r"TRUNCATE[\s]+TABLE[\s]+", file_obj, flags=flagID)
        # print(find_trancate)


        if find_bt and find_trancate:
            result_fkt_check_truncate_with_begin_transaction.append("N")
            log_result.append(Result_No(input_log,find_bt+find_trancate))
        else:
            # print("YYYY")
            result_fkt_check_truncate_with_begin_transaction.append("Y")
            log_result.append(Result_Yes(input_log,find_bt+find_trancate))
        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_fkt_check_truncate_with_begin_transaction.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_insert_column_names(log_result,filename,result_check_insert_column_names):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_insert_column_names"))
    input_columns=[]
    output_columns=[]
    input_log=[]
    output_log=[]
    try:
        extra_words=['SELECT', 'FROM', 'FLOAT64']
        #Input
        file_obj = common_obj.Read_file(inputFolderPath+filename)

        file_obj = re.sub(r"CONCAT[\s]*\(.*?\)|COALESCE[\s]*\(.*?\)|REGEXP_REPLACE[\s]*\(.*?\)", "", file_obj, flags=re.I|re.DOTALL)  #rmv functions
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        file_obj = re.sub(r"CASE.*?WHEN.*?ELSE.*?END", "", file_obj, flags=re.DOTALL) #rmv case when
        file_obj = re.sub(r'\n[\s]*"', '"', file_obj, flags=re.DOTALL)
        file_obj = re.sub(r'\n[\s]*\)', ')', file_obj, flags=re.DOTALL)
        # print("file_obj", file_obj)
        
        find_select_from_in = re.findall(r"SELECT.*?FROM", file_obj, flags=re.I|re.DOTALL)
        # print("find_select_from_in", len(find_select_from_in), find_select_from_in)

        if find_select_from_in:
            for ele in find_select_from_in[0].split('\n'):
                ele = re.sub(r"IF[\s]*\(.*\)", "", ele, flags=re.I)
                ele = re.sub(r"[\s]*\,[\s]*|[\s]+$", "", ele)
                # print("ele", ele)
            
                # print("ele", ele)

                if '' == ele or ele.upper() in extra_words:     # '' / extra_words
                    pass

                elif len(re.findall(r"\.", ele)) == 2 and re.findall(r"[\w]+\.[\w]+\.[\w]+", ele):          # w.w.w
                    ele = re.sub(r"[\w]+\.[\w]+\.|[\s]*\,[\s]*", "", ele)
                    # print("ele", ele)
                    input_log.append(ele.lower())
                    input_columns.append(ele.lower())

                else:                                           
                    ele = ele.split(' ')[-1]
                    # print("ele", ele)
                    if re.findall(r"[\w]+\.[\w]+", ele):  # w.w
                        ele = re.sub(r"[\w]+\.", "", ele)
                        input_log.append(ele.lower())
                        input_columns.append(ele.lower())

                    else:
                        input_log.append(ele.lower())
                        input_columns.append(ele.lower())


        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  

        file_obj = re.sub(r"DELETE[\s]+FROM.*?INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=re.DOTALL)
        file_obj = re.sub(r"\n^[\s]*\.", ".", file_obj, flags=re.DOTALL) 
        file_obj = re.sub(r" AS\n", " AS ", file_obj, flags=re.DOTALL)   #|[\w]+\.[\w]+[\s]+AS[\s]+
        file_obj = re.sub(r"\([\s]*CASE.*?WHEN.*?ELSE.*?END[\s]*\)", "", file_obj, flags=re.DOTALL) #rmv case when
        file_obj = re.sub(r"COALESCE[\s]*\(.*?\)|CAST[\s]*\(.*?\)|REGEXP_REPLACE[\s]*\(.*?\)|[\s]+AND[\s]+.*?[\s]+AND", "", file_obj, flags=re.I|re.DOTALL)  #rmv functions
        # print("file_obj", file_obj)

        
        find_select_from_op = re.findall(r"SELECT.*?FROM", file_obj, flags=re.I|re.DOTALL)
        # print("find_select_from_op", find_select_from_op)

        if find_select_from_op:
            for ele in find_select_from_op[0].split('\n'):
                # print("ele", ele)
                ele = re.sub(r".*?\)|SELECT[\s]+", "", ele)
                # print("ele", ele)
                if 'AS' in ele :
                    ele = ele.split(' AS ')[-1]
                    ele = re.sub(r"\,|[\s]+", "", ele)
                    # print("ele", ele)
                
                    if '' == ele or ele.upper() in extra_words:
                        pass
                    else:
                        output_log.append(ele.lower())
                        output_columns.append(ele.lower())
                
                elif re.findall(r"[\w]+\.[\w]+", ele):
                    # print("ele", ele)
                    ele = re.sub(r"[\w]+\.|[\s]*\,[\s]*|[\s]+", "", ele)
                    output_log.append(ele.lower())
                    output_columns.append(ele.lower())

        input_columns.sort()
        output_columns.sort()

        # print("input_columns=", len(input_columns), input_columns)
        # print("output_columns=", len(output_columns),output_columns)

        if len(input_columns)==0 and len(output_columns)==0:
            result_check_insert_column_names.append("NA")
            log_result.append(Result_NA())
        elif input_columns == output_columns:
            # print("YYYY")
            result_check_insert_column_names.append("Y("+str(len(input_columns))+"-"+str(len(output_columns))+")")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_check_insert_column_names.append("N("+str(len(input_columns))+"-"+str(len(output_columns))+")")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_insert_column_names.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_match_aggregate_func_count(log_result,filename,result_match_aggregate_func_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("match_aggregate_func_count"))
    input_func=[]
    output_func=[]
    try:
        #Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # print("file_obj", file_obj)
        
        aggregator_fun_lst=['SUM','COUNT','AVG','MIN','MAX']
    
        for fun in aggregator_fun_lst:
            find_fun=re.findall(r'[\W]'+fun+'[\s]*\(', file_obj, flags=re.I|re.DOTALL)
        
            for i in find_fun:
                input_func.append(re.sub('^[\W]|[\s]+', '', i.upper(), flags=re.DOTALL))

    
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  

        file_obj = re.sub(r"DELETE[\s]+FROM.*?INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=re.DOTALL)
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # print("file_obj", file_obj)

        for fun in aggregator_fun_lst:
            find_fun=re.findall(r'[\W]'+fun+'[\s]*\(',file_obj,flags=re.I|re.DOTALL)
            for i in find_fun:
                output_func.append(re.sub('^[\W]|[\s]+','',i.upper(),flags=re.DOTALL))

        input_func.sort()
        output_func.sort()

        # print("input_func=", len(input_func), input_func)
        # print("output_func=", len(output_func),output_func)

        if len(input_func)==0 and len(output_func)==0:
            result_match_aggregate_func_count.append("NA")
            log_result.append(Result_NA())
        elif input_func == output_func:
            # print("YYYY")
            result_match_aggregate_func_count.append("Y("+str(len(input_func))+"-"+str(len(output_func))+")")
            log_result.append(Result_Yes(input_func,output_func))
        else:
            result_match_aggregate_func_count.append("N("+str(len(input_func))+"-"+str(len(output_func))+")")
            log_result.append(Result_No(input_func,output_func))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_match_aggregate_func_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_match_clauses_count(log_result,filename,result_match_clauses_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("match_clauses_count"))
    input_clauses=[]
    output_clauses=[]
    try:
        #Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # print("file_obj", file_obj)
        
        clauses_lst=['WHERE', 'HAVING', 'ORDER BY', 'GROUP BY']
    
        for clause in clauses_lst:
            find_clause = re.findall(r'[\W]'+clause+'[\W]', file_obj, flags=re.I|re.DOTALL)
            # print("find_clause", find_clause)
        
            for ele in find_clause:
                ele = re.sub('^[\W]|[\W]$', '', ele.upper(), flags=re.DOTALL)
                ele = re.sub(r'[\s]+', ' ', ele, flags=re.I|re.DOTALL)
                input_clauses.append(ele)

    
        #Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)  

        file_obj = re.sub(r"DELETE[\s]+FROM.*?INSERT[\s]+INTO", "INSERT INTO", file_obj, flags=re.DOTALL)
        file_obj = re.sub(r"\-\-.*?\n", "\n", file_obj, flags=re.DOTALL)  #rmv inline comments
        # # print("file_obj", file_obj)

        for clause in clauses_lst:
            find_clause = re.findall(r'[\W]'+clause+'[\W]', file_obj, flags=re.I|re.DOTALL)
            # print("find_clause", find_clause)

            for ele in find_clause:
                ele = re.sub('^[\W]|[\W]$', '', ele.upper(), flags=re.DOTALL)
                ele = re.sub(r'[\s]+', ' ', ele, flags=re.I|re.DOTALL)
                output_clauses.append(ele)

        input_clauses.sort()
        output_clauses.sort()

        # print("input_clauses=", len(input_clauses), input_clauses)
        # print("output_clauses=", len(output_clauses),output_clauses)

        if len(input_clauses)==0 and len(output_clauses)==0:
            result_match_clauses_count.append("NA")
            log_result.append(Result_NA())
        elif input_clauses == output_clauses:
            # print("YYYY")
            result_match_clauses_count.append("Y("+str(len(input_clauses))+"-"+str(len(output_clauses))+")")
            log_result.append(Result_Yes(input_clauses,output_clauses))
        else:
            result_match_clauses_count.append("N("+str(len(input_clauses))+"-"+str(len(output_clauses))+")")
            log_result.append(Result_No(input_clauses,output_clauses))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_match_clauses_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_date_sub_func(log_result,filename,result_check_date_sub_func):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("check_date_sub_func"))
    input_date_sub=[]
    output_date_sub=[]
    Log_input=[]
    Log_output=[]
    try:
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_date_sub_ip=re.findall(r"DATE_SUB[\s]*\([\s]*[\w]+[\s]*\,[\s]*[\d]+[\s]*\)", file_obj, flags=re.I|re.DOTALL)
        print("find_date_sub_ip", find_date_sub_ip)

        if find_date_sub_ip:
            for ele in find_date_sub_ip:
                Log_input.append(ele)
                ele = re.sub(r"\,", ",INTERVAL", ele, flags=re.I)
                ele = re.sub(r"\)", "DAY)", ele, flags=re.I)
                ele = re.sub(r"[\s]+","", ele, flags=re.I)
                input_date_sub.append(ele.upper())

        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        find_date_sub_op = re.findall(r"DATE_SUB[\s]*\([\s]*[\w]+[\s]*\,[\s]*INTERVAL[\s]*[\d]+[\s]*DAY[\s]*\)", file_obj, flags=re.I|re.DOTALL)
        # print("find_date_sub_op", find_date_sub_op)

        for ele in find_date_sub_op:
            ele = re.sub(r"[\s]+", "", ele)
            Log_output.append(ele.upper())
            output_date_sub.append(ele.upper())

        # print("input_all_datatype =",input_date_sub)
        # print("output_all_datatype=",output_date_sub)

        if len(input_date_sub)==0 and len(output_date_sub)==0:
            log_result.append(Result_NA())
            result_check_date_sub_func.append("NA")
        elif input_date_sub==output_date_sub:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_date_sub_func.append("Y("+str(len(input_date_sub))+"-"+str(len(output_date_sub))+")")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_date_sub_func.append("N("+str(len(input_date_sub))+"-"+str(len(output_date_sub))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_date_sub_func.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fkt_check_cast_count(log_result,filename,result_check_cast_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Cast"))
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
                # for fileline in fileLines:
                # print(fileline)  #|\[[\s]*OFFSET.*?\]
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
                        find_cast_ip=re.findall(r"CAST[\s]*\(.*?AS[\s]+INT64[\s]*\)|CAST[\s]*\(.*?AS[\s]+INT[\s]*\)|CAST[\s]*\(.*?AS[\s]+BIGINT[\s]*\)|CAST[\s]*\(.*?AS[\s]+INTEGER[\s]*\)", fileline, flags=re.I|re.DOTALL)
                        # print(find_cast_ip)
                        if find_cast_ip:
                            for ele in find_cast_ip:
                                # print(ele)
                                ele = re.sub(r" BIGINT[\s]*\)| INTEGER[\s]*\)| INT[\s]*\)"," INT64)", ele, flags=re.I)
                                ele = re.sub(r"[\s]+"," ", ele, flags=re.I)
                                # if "safe_cast" in ele.lower() or "safe_offset" in ele.lower():
                                #     pass
                                # else:
                                Log_input.append(ele)
                                input_cast.append(ele.lower())


        if filename in outputFile:
            with open(OutputFolderPath+filename, "r") as f:
                fileLines = f.readlines()
                # for fileline in fileLines:
                # print(fileline)
                #|\[[\s]*SAFE_OFFSET.*?\]
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
                        find_cast_op=re.findall(r"SAFE_CAST[\s]*\(.*?AS[\s]+INT64[\s]*\)", fileline, flags=re.I|re.DOTALL)
                        for ele in find_cast_op:
                            ele=re.sub(r"[\s]+"," ",ele,flags=re.I)
                            Log_output.append(ele)
                            output_cast.append(ele.lower())


        # print("input_all_datatype =", len(input_cast), input_cast)
        # print("output_all_datatype=", len(output_cast), output_cast)

        if len(input_cast)==0 and len(output_cast)==0:
            log_result.append(Result_NA())
            result_check_cast_count.append("NA")
        elif len(input_cast)==len(output_cast):
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_check_cast_count.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_check_cast_count.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_cast_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_hash_parameter(log_result,filename,result_check_hash_parameter,hash_parameter_csv_path):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Hash Parameter"))
    input_hash=[]
    output_hash=[]
    input_log=[]
    output_log=[]
    hash_value_list=[]
    try:
    # if 1==1:
        df = pd.read_csv(hash_parameter_csv_path, sep=',', header=None)
        hash = df[0]
        value = df[2]
        hash_value_name=hash+"::"+value
        
        for ele in hash_value_name:
            hash_value_list.append(ele)

        if filename in inputFile:
            file_obj = common_obj.Read_file(inputFolderPath+filename)

            if '#' in file_obj.upper():
                for ele in hash_value_list:
                    key = ele.split('::')[0]
                    value = ele.split('::')[1]
            
                    if key in file_obj:
                        file_obj = re.sub(key, value, file_obj)
                        input_log.append(key)
        
                    if value in file_obj:
                        input_hash.append(value)
                        

        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            if 'CAST' in file_obj.upper():
                for ele in hash_value_list:
                    value = ele.split('::')[1]

                    if value in file_obj:
                        output_hash.append(value)
                        output_log.append(value)

        # print("input_hash=", len(input_hash), input_hash)
        # print("output_hash=", len(output_hash),output_hash)

        if len(input_hash)==0 and len(output_hash)==0:
            result_check_hash_parameter.append("NA")
            log_result.append(Result_NA())
        elif len(input_hash) == len(output_hash):
            # print("YYYY")
            result_check_hash_parameter.append("Y")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_check_hash_parameter.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_hash_parameter.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_timezone(log_result,filename,result_check_timezone):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("Check Timezone"))
    try:
    # if 1==1:
        if filename in outputFile:
            fileLines = common_obj.Readlines_file(OutputFolderPath+filename)
            counter=0
            comment=0
            obj=''
            for fileLine in fileLines:
                # print(fileLine)
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
            # print("obj", obj)
            find_current_op=re.findall(r"CURRENT_TIME\(\"Asia\/Kolkata\"\)|CURRENT_TIME\(\'Asia\/Kolkata\'\)|CURRENT_DATETIME\(\"Asia\/Kolkata\"\)|CURRENT_DATETIME\(\'Asia\/Kolkata\'\)|"
                                       r"CURRENT_TIME[\s]*[\W]+|CURRENT_DATETIME[\s]*[\W]+|CURRENT_DATE[\s]*[\W]+|CURRENT_TIMESTAMP[\s]*[\W]+", obj, flags=re.I|re.DOTALL)
            # print("find_current_op", len(find_current_op), find_current_op)

            find_current_op1=re.findall(r"CURRENT_TIME\(\"Asia\/Kolkata\"\)|CURRENT_DATETIME\(\"Asia\/Kolkata\"\)|CURRENT_DATE\(\"Asia\/Kolkata\"\)|"
                                       r"CURRENT_TIME\(\'Asia\/Kolkata\'\)|CURRENT_DATETIME\(\'Asia\/Kolkata\'\)|CURRENT_DATE\(\'Asia\/Kolkata\'\)", obj, flags=re.I|re.DOTALL)
            # print("find_current_op1", len(find_current_op1), find_current_op1)


        if len(find_current_op)==0 and len(find_current_op1)==0:
            log_result.append(Result_NA())
            result_check_timezone.append("NA")
        elif len(find_current_op)==len(find_current_op1):
            log_result.append(Result_Yes(find_current_op,find_current_op1))
            result_check_timezone.append("Y")
        else:
            log_result.append(Result_No(find_current_op,find_current_op1))
            result_check_timezone.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_timezone.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_fkt_check_safe_cast(log_result,filename,result_check_safe_cast):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
    log_result.append(Common("safe cast"))
    output_safe_cast=[]
    output_log=[]
    try:
    # if 1==1:
        if filename in outputFile:
            file_obj = common_obj.Read_file(OutputFolderPath+filename)

            if 'CAST' in file_obj.upper():
                find_safe_cast=re.findall(r"[\S]+[\s]*CAST", file_obj, flags=re.I|re.DOTALL)
                # print(find_safe_cast)
                for ele in find_safe_cast:
                    output_safe_cast.append(ele.upper())
                    output_log.append(ele)

        # print("output_safe_cast=", len(output_safe_cast),output_safe_cast)

        if len(output_safe_cast)==0:
            result_check_safe_cast.append("NA")
            log_result.append(Result_NA())
        elif 'SAFE_CAST' in output_safe_cast:
            # print("YYYY")
            result_check_safe_cast.append("Y")
            log_result.append(Result_Yes(["SAFE_CAST"],output_log))
        else:
            result_check_safe_cast.append("N")
            log_result.append(Result_No(["SAFE_CAST"],output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_check_safe_cast.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)


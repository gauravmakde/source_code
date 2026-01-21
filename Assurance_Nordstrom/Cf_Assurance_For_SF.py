from CF_Assurance_Common_Functions import *

#Automation_SF_to_BQ_assurance --Starts
def fun_sf_match_schema_table_case(log_result,filename,directory,result_sf_match_schema_table_case):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Match_schema_table_case"))
    input_schema_table = []
    output_schema_table = []
    Log_input = []
    Log_output = []
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_schema_table_ip = re.findall(r"CREATE[\s]+.*?TABLE[\s]+[\w]+[\s]*\(", file_obj, flags=flagID)
        find_schema = re.findall(r"USE[\s]+SCHEMA[\s]+([\w]+)[\s]*\;", file_obj, flags=flagID)
            
        # print("find_schema_table_ip", find_schema_table_ip)
        # print("find_schema", find_schema)

        if find_schema_table_ip and find_schema:
            for ele in find_schema_table_ip:
                # print(ele)
                if 'TRANSIENT TABLE' in ele.upper():
                    ele = re.sub(r"CREATE[\s]+.*?TABLE[\s]+|[\s]*\(", "", ele, flags=flagI)
                    ele = re.sub(r"[\s]+", " ", ele)
                    input_schema_table.append(ele)
                    Log_input.append(ele)
                else:
                    ele = re.sub(r"CREATE[\s]+.*?TABLE[\s]+|[\s]*\(", "", ele, flags=flagI)
                    # print(ele)
                    # ele = re.sub(r"CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE[\s]+", "CREATE OR REPLACE TABLE `"+Append_project_id+"."+find_schema[0]+".", ele, flags=flagI)
                    ele = "`"+Append_project_id+"."+find_schema[0]+"."+ele
                    ele = re.sub(r"[\s]+", " ", ele)
                    ele = ele + "`"
                    # print(ele)
                    input_schema_table.append(ele)
                    Log_input.append(ele)

        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
        
        find_schema_table_op = re.findall(r"CREATE[\s]+.*?TABLE[\s]+\`"+Append_project_id+"\.[\w]+\.[\w]+\`"
                                          r"|CREATE[\s]+TEMPORARY[\s]+TABLE[\s]+[\w]+", file_obj, flags=flagID)
        
        # print(find_schema_table_op)
        for ele in find_schema_table_op:
            if 'TEMPORARY TABLE' in ele.upper():
                # print(ele)
                ele = re.sub(r"CREATE[\s]+.*?TABLE[\s]+", "", ele, flags=flagI)
                ele = re.sub(r"[\s]+", " ", ele)
                output_schema_table.append(ele)
                Log_output.append(ele)
            else:
                ele = re.sub(r"CREATE[\s]+.*?TABLE[\s]+", "", ele, flags=flagI)
                ele = re.sub(r"[\s]+", " ", ele)
                output_schema_table.append(ele)
                Log_output.append(ele)

        # print("input_schema_table-", input_schema_table)
        # print("output_schema_table-",output_schema_table)

        if len(input_schema_table) == 0 and len(output_schema_table) == 0:
            log_result.append(Result_NA())
            result_sf_match_schema_table_case.append("NA")
        elif input_schema_table == output_schema_table:
            # print("YYYY")
            log_result.append(Result_Yes(Log_input,Log_output))
            result_sf_match_schema_table_case.append("Y")
        else:
            log_result.append(Result_No(Log_input,Log_output))
            result_sf_match_schema_table_case.append("N")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_match_schema_table_case.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_project_id(log_result,filename,directory,result_sf_project_id):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Check Project ID"))
    try:
        # Output
        file_obj = common_obj.Read_file(OutputFolderPath+filename)

        file_obj = re.sub(r"\'.*?\'", "", file_obj)
        file_obj = re.sub(r'\".*?\"', '', file_obj)

        project_id = re.findall(r"JOIN[\s]+\`*[\w]+\.[\w]+\`*\;"
                                r"|FROM[\s]+\`*[\w]+\.[\w]+\`*\;"
                                r"|PROCEDURE[\s]+\`*[\w]+\.[\w]+\`*\("
                                r"|FROM[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|MERGE[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|INTO[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|VIEW[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|UPDATE[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|JOIN[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|EXISTS[\s]+\`*[\w]+\.[\w]+\`*[\s]+"
                                r"|TABLE[\s]+\`*[\w]+\.[\w]+\`*[\s]+", file_obj, flags=flagI)

        # print("project_id", project_id)
        if project_id:
            result_sf_project_id.append("N")
            log_result.append(Result_No(project_id,project_id))
        else:
            project_id = re.findall(r'JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;'
                                    r'|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*\;'
                                    r'|PROCEDURE[\s]+\`*'+Append_project_id+'\`*\.[\w]+\.[\w]+\`*\('
                                    r'|FROM[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+'
                                    r'|MERGE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+'
                                    r'|EXISTS[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*'
                                    r'|INTO[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*'
                                    r'|VIEW[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]*'
                                    r'|UPDATE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+'
                                    r'|JOIN[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+'
                                    r'|TABLE[\s]+\`*'+Append_project_id+'\.[\w]+\.[\w]+\`*[\s]+', file_obj, flags=flagI)
            
            # print("project_id", project_id)
            # print("Append_project_id", Append_project_id)
            if project_id:
                result_sf_project_id.append("Y")
                log_result.append(Result_Yes(project_id,project_id))
            else:
                result_sf_project_id.append("NA")
                log_result.append(Result_NA())

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_project_id.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_check_datatype(log_result,filename,directory,result_sf_check_datatype):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Datatype Checking"))
    number_column = []
    char_varchar_datatype = []
    boolean_column = []
    variant_column = []
    timestamp_column = []
    date_column = []
    input_datatype = []
    output_datatype = []
    input_log=[]
    output_log=[]
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        # print(file_obj)
        find_table = re.findall(r"CREATE[\s]+MULTISET[\s]+VOLATILE[\s]+TABLE"
                                r"|CREATE[\s]+SET[\s]+VOLATILE[\s]+TABLE"
                                r"|CREATE[\s]+VOLATILE[\s]+TABLE"
                                r"|CREATE[\s]+OR[\s]+REPLACE[\s]+TABLE"
                                r"|CREATE[\s]+OR[\s]+REPLACE[\s]+[\w]+[\s]+TABLE"
                                r"|CREATE[\s]+TABLE[\s]+IF[\s]+NOT[\s]+EXISTS"
                                r"|CREATE[\s]+SET[\s]+TABLE"
                                r"|CREATE[\s]+TEMP[\s]+TABLE"
                                r"|CREATE[\s]+TEMPORARY[\s]+TABLE"
                                r"|CREATE[\s]+TABLE[\s]+"
                                r"|CREATE[\s]+VOLATILE[\s]+MULTISET[\s]+TABLE"
                                r"|CREATE[\s]+MULTISET[\s]+TABLE", file_obj, flags=flagID)

        # print(find_table)

        if find_table:
            file_obj = re.sub(r"\#|\$", "_", file_obj, flags=flagID)
            file_obj = re.sub(r"COMMENT[\s]*\'.*?\'|COMMENT[\s]*\=[\s]*\'.*?\'", "", file_obj, flags=flagID)

            find_number = re.findall(r"[\w]+[\s]+NUMBER[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DEC[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]*\([\d]+\)|[\w]+[\s]+DEC[\s]*\([\d]+\)|[\w]+[\s]+NUMERIC[\s]*\([\d]+\,[\s]*[\d]+\)|[\w]+[\s]+DECIMAL[\s]+|[\w]+[\s]+NUMBER[\s]+|[\w]+[\s]+DECIMAL\,|[\w]+[\s]+NUMBER\,|[\w]+[\s]+DECIMAL\)|[\w]+[\s]+NUMBER\)|[\w]+[\s]+NUMERIC[\s]+|[\w]+[\s]+NUMEIC\,|[\w]+[\s]+NUMEIC\)", file_obj, flags=flagID)
            
            for ele in find_number:
                # print("ele", ele)
                ele = re.sub(r"\)\)", ")", ele, flags=flagI)
                number_column.append(ele.upper())
                # print("number_column", len(number_column), number_column)


            find_char = re.findall(r"[\w]+[\s]+VARCHAR[\s]*\([\d]+\)|[\w]+[\s]+CHAR[\s]*\([\d]+\)|[\w]+[\s]+VARCHAR[\s]+|[\w]+[\s]+VARCHAR\)|[\w]+[\s]+VARCHAR\,|[\w]+[\s]+CHAR[\s]+|[\w]+[\s]+CHAR\,|[\w]+[\s]+CHAR\)", file_obj, flags=flagID)
            
            for ele in find_char:
                ele = re.sub(r"\)\)", ")", ele, flags=flagI)
                char_varchar_datatype.append(ele.upper())
                # print("char_varchar_datatype", len(char_varchar_datatype), char_varchar_datatype)


            find_boolean = re.findall(r"[\w]+[\s]+BOOLEAN\)*[\s]+|\_[\w]+[\s]+BOOLEAN\)*[\s]+|[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN\,|\_[\w]+[\s]+BOOLEAN", file_obj, flags=flagID)
            
            for ele in find_boolean:
                ele = re.sub(r"\)", ",", ele, flags=flagI)
                boolean_column.append(ele.upper())
                # print("boolean_column", boolean_column)


            find_variant = re.findall(r"[\w]+[\s]+VARIANT\)*[\s]+|[\w]+[\s]+VARIANT\,", file_obj, flags=flagID)

            for ele in find_variant:
                ele = re.sub(r"\)", ",", ele, flags=flagI)
                ele = re.sub(r"\,", "", ele, flags=flagI)
                variant_column.append(ele.upper())
                # print("variant_column:", variant_column)


            find_timestamp = re.findall(r"[\w]+[\s]+TIMESTAMP|[\w]+[\s]+TIMESTAMP\)*[\s]+|[\w]+[\s]+TIMESTAMP\,", file_obj, flags=flagID)
            
            for ele in find_timestamp:
                ele = re.sub(r"\)", "," , ele, flags=flagI)
                timestamp_column.append(ele.upper())
                # print("timestamp_column", timestamp_column)

            
            find_Date = re.findall(r"[\w]+[\s]+DATE\)*[\s]+|[\w]+[\s]+DATE\,", file_obj, flags=flagID)
            
            for ele in find_Date:
                ele = re.sub(r"\)", "", ele, flags=flagI)
                date_column.append(ele.upper())
                # print("date_column", date_column)

        for ele in number_column:
            # print("ele", ele)
            # ele = re.sub(r"[\s]+NUMBER[\s]*\(", " BIGNUMERIC(", ele, flags=flagI)
            # ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            # ele = re.sub(r"\`", "", ele, flags=flagI)

            # # print("ele",ele)
            # if ele.lower() == "asnumber" or ele.lower() == "andnumber":
            #     pass
            # else:
            #     input_log.append(ele.lower())
            #     input_datatype.append(ele.lower())

            if "as number" in ele.lower() or "as number" in ele.lower() or "as number" in ele.lower():
                    pass
            else:

                find_column_value = re.findall(r"[\d]+\,[\d]+", ele.upper())

                if find_column_value:
                    find_column_value = "".join(find_column_value)
                    find_column_value = find_column_value.split(",")
                    if int(find_column_value[0])>=38: #or int(find_column_value[1])>=9:
                        # print(int(find_column_value[0]))
                        replce_decimal = re.sub("[\s]+NUMBER"," BIGNUMERIC",ele,flags=flagI)
                        replce_decimal = re.sub(r"[\s]+","",replce_decimal,flags=flagI)
                        replce_decimal = re.sub(r"\)\,",")",replce_decimal,flags=flagI)
                        input_log.append(ele.lower())
                        input_datatype.append(replce_decimal.lower())
                    else:
                        replce_decimal = re.sub("[\s]+NUMBER"," NUMERIC",ele,flags=flagI)
                        replce_decimal = re.sub(r"[\s]+","",replce_decimal,flags=flagI)
                        replce_decimal = re.sub(r"\)\,",")",replce_decimal,flags=flagI)
                        input_log.append(ele.lower())
                        input_datatype.append(replce_decimal.lower())
                else:

                    replce_decimal = re.sub("[\s]+NUMBER"," NUMERIC",ele,flags=flagI)
                    replce_decimal = re.sub(r"[\s]+","",replce_decimal,flags=flagI)
                    replce_decimal = re.sub(r"\)\,",")",replce_decimal,flags=flagI)
                    input_log.append(ele.lower())
                    input_datatype.append(replce_decimal.lower())


        for ele in char_varchar_datatype:
            ele = re.sub(r"[\s]+CHAR|[\s]+VARCHAR", " STRING", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r"\,", "", ele, flags=flagI)

            # print("ele",ele)
            if ele.lower() == "asstring" or ele.lower() == "andstring":
                pass
            else:
                input_log.append(ele.lower())
                input_datatype.append(ele.lower())

        for ele in boolean_column:
            ele = re.sub(r"[\s]+BOOLEAN", "BOOL", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r"\,", "", ele, flags=flagI)

            if ele.lower()=="andboolean" or ele.lower()=="asboolean" or ele=="":
                pass
            else:
                input_datatype.append(ele.lower())
                input_log.append(ele.lower())

        for ele in variant_column:
            # print(ele)
            ele = re.sub(r"[\s]+json,", "JSON", ele, flags=flagI)
            ele = re.sub(r"[\s]+VARIANT", "JSON", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r",", "", ele, flags=flagI)
        
            if ele.lower() == "asjson" or ele.lower() == "andjson" or "ofjson" in ele.lower():
                pass
            else:
                input_log.append(ele.lower())
                input_datatype.append(ele.lower())

        for ele in timestamp_column:
            # print(ele)
            ele = re.sub(r"[\s]+TIMESTAMP", "TIMESTAMP", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r"\,", "", ele, flags=flagI)

            if ele.lower()=="andtimestamp" or ele.lower()=="astimestamp" or ele=="":
                pass
            else:
                input_datatype.append(ele.lower())
                input_log.append(ele.lower())
        
        for ele in date_column:
            ele = re.sub(r"[\s]+DATE", "DATE", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r"\,", "", ele, flags=flagI)

            if ele.lower()=="anddate" or ele.lower()=="asdate" or ele==""  or "elsedate" in ele.lower():
                pass
            else:
                input_datatype.append(ele.lower())
                input_log.append(ele.lower())
                # print(input_datatype)
        
        
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        find_bignumeric = re.findall(r"[\w]+[\s]+NUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+NUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)"
                                     r"|[\w]+[\s]+BIGNUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)\)|[\w]+[\s]+BIGNUMERIC[\s]*\(\s*[\d]+\s*\,[\d]+\)", file_obj, flags=flagI)
            
        for ele in find_bignumeric:
            # print("ele", ele)
            ele = re.sub(r"\)\)",")", ele, flags=flagI)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
                                                
            if "asnumeric" in ele.lower() or "bybignumeric" in ele.lower() or "asbignumeric" in ele.lower():
                pass
            else:
                ele = re.sub(r"\)\)",")", ele, flags=flagI)
                output_log.append(ele)
                output_datatype.append(ele.lower())

       

        find_string = re.findall(r"[\w]+[\s]+STRING\([\d]+\)|[\w]+[\s]+STRING\([\d]+\)\)|[\w]+[\s]+STRING[\s]*|[\w]+[\s]+STRING[\s]*\)", file_obj, flags=flagI)
        for ele in find_string:
            # print("ele", ele)
            ele = re.sub(r"[\s]+", "", ele, flags=flagI)
            ele = re.sub(r"\)\)",")", ele, flags=flagI)
        
            if ele.lower() == "asstring" or ele.lower() == "andstring" or ele.lower() == "bystring" or "ofstring" in ele.lower():
                pass
            else:
                ele = re.sub(r"\)\)", ")", ele, flags=flagI)
                output_log.append(ele)
                output_datatype.append(ele.lower())


        find_bool = re.findall(r"[\w]+[\s]+BOOL[\s]*|[\w]+[\s]+BOOL[\s]*\)", file_obj, flags=flagI)
        for ele in find_bool:
            # print("ele", ele)
            ele = re.sub(r"[\s]+", "", ele, flags=re.I)

            if ele.lower() == "asbool" or ele.lower()=="bybool" or ele.lower()=="andbool":
                pass
            else:
                ele = re.sub(r"\)",",",ele,flags=re.I)
                output_log.append(ele)
                output_datatype.append(ele.lower())
                #print("output_datatype :" , output_datatype)

        find_json = re.findall(r"[\w]+[\s]+JSON[\s]*|[\w]+[\s]+JSON[\s]*\)", file_obj, flags=flagI)
        for ele in find_json:
            # print("ele", ele)
            ele = re.sub(r"[\s]+", "", ele, flags=re.I)

            if ele.lower() == "asjson" or ele.lower()=="byjson" or ele.lower()=="andjson":
                pass
            else:
                ele = re.sub(r"\)",",",ele,flags=re.I)
                output_log.append(ele)
                output_datatype.append(ele.lower())
                #print("output_datatype :" , output_datatype)

        find_timestamp = re.findall(r"[\w]+[\s]+TIMESTAMP[\s]*|[\w]+[\s]+TIMESTAMP[\s]*\)", file_obj, flags=flagI)
        for ele in find_timestamp:
            # print("ele", ele)
            ele = re.sub(r"[\s]+", "", ele, flags=re.I)
        
            if ele.lower() == "astimestamp" or ele.lower()=="bytimestamp" or ele.lower()=="andtimestamp":
                pass
            else:
                ele = re.sub(r"\)", ",", ele, flags=re.I)
                output_log.append(ele)
                output_datatype.append(ele.lower())

        find_date = re.findall(r"[\w]+[\s]+DATE[\s]*\,|[\w]+[\s]+DATE[\s]*\)|[\w]+[\s]+DATE[\s]*", file_obj, flags=flagI)
        for ele in find_date:
            # print("ele", ele)
            ele = re.sub(r"[\s]+", "", ele, flags=re.I)
            ele = re.sub(r"\,|\)", "", ele, flags=re.I)

            if ele.lower() == "asdate" or ele.lower()=="anddate" or ele.lower()=="bydate" or "elsedate" in ele.lower() :
                pass
            else:
                ele = re.sub(r"\)", ",", ele, flags=re.I)
                output_log.append(ele)
                output_datatype.append(ele.lower())
                #print("output_datatype :" , output_datatype)

        input_datatype.sort()
        output_datatype.sort()
        input_log.sort()
        output_log.sort()
                  
        # print("input_datatype =", len(input_datatype), input_datatype)
        # print("output_datatype =", len(input_datatype), output_datatype)

        # print("input_datatype =", input_datatype)
        # print("output_datatype =", output_datatype)

        # print("input_log",input_log)
        # print("output_log",output_log)
        
        if len(input_datatype)==0 and len(output_datatype) == 0:
            result_sf_check_datatype.append("NA")
            log_result.append(Result_NA())
        elif list(set(input_datatype))==list(set(output_datatype)):
            # print("YYYY")
            result_sf_check_datatype.append("Y("+str(len(list(set(input_datatype))))+"-"+str(len(list(set(output_datatype))))+")")
            log_result.append(Result_Yes(input_log,output_log))
        else:
            result_sf_check_datatype.append("N("+str(len(list(set(input_datatype))))+"-"+str(len(list(set(output_datatype))))+")")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_check_datatype.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_check_no_of_statements_in_script(log_result,filename,directory,result_sf_check_no_of_statements_in_script):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Number of Statements in SF-BQ Script"))
    input_sql_statements = []
    output_sql_statements = []
    input_log = []
    output_log = []
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

        find_in_state = re.findall(r"CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+.*?\.[\w]+\.[\w]+"
                                   r"|var[\s]+create_tgt_wrk_table[\s]*[=]"
                                   r"|var[\s]+sql_updates[\s]*[=]"
                                   r"|var[\s]+sql_sameday[\s]*[=]"
                                   r"|var[\s]+sql_inserts[\s]*[=]"
                                   r"|var[\s]+sql_exceptions[\s]*[=]"
                                   r"|var[\s]+sql_commit[\s]*[=][\W]*COMMIT[\W]*[;]?"
                                   r"|var[\s]+sql_rollback[\s]*[=][\W]*ROLLBACK[\W]*[;]?", file_obj, flags=flagID)
        
        if find_in_state:
            for ele in find_in_state:
                input_log.append(ele)
                ele = re.sub(r"PROCEDURE[\s]+.*?\.", "PROCEDURE `"+Append_project_id+"`.", ele, flags=flagI)
                ele = re.sub(r"var[\s]+", "SET ", ele, flags=flagI)
                ele = re.sub(r"[\s]+", " ", ele)
                # print(ele)
                input_sql_statements.append(ele)

        find_try_catch = re.findall(r"try.*?\{.*?catch.*?\{.*?return.*?\;", file_obj, flags=flagID)
        if find_try_catch:
            for ele in find_try_catch:
                # print(ele)
                ele = re.sub(r"try.*?\{", "BEGIN ", ele, flags=flagID)
                ele = re.sub(r"\}[\s]*\)[\s]*\;", ");", ele, flags=flagID)
                ele = re.sub(r"catch.*?\{", "EXCEPTION WHEN ERROR THEN ", ele, flags=flagID)
                ele = re.sub(r"return[\s]+", "SELECT ", ele, flags=flagID)
                ele = re.sub(r"\{[\s]*err[\s]*\}", "${@@error.message }", ele, flags=flagID)
                ele = re.sub(r"snowflake\.execute[\s]*\([\s]*\{[\s]*sqlText\:[\s]*", "EXECUTE IMMEDIATE ( ", ele, flags=flagID)
                input_log.append(ele)
                ele = re.sub(r"[\s]+", " ", ele)
                # print(ele)
                input_sql_statements.append(ele.lower())
                
                
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        find_op_state = re.findall(r"CREATE[\s]+OR[\s]+REPLACE[\s]+PROCEDURE[\s]+\`"+Append_project_id+"\`\.[\w]+\.[\w]+"
                                   r"|set[\s]+create_tgt_wrk_table[\s]*[=]"
                                   r"|set[\s]+sql_updates[\s]*[=]"
                                   r"|set[\s]+sql_sameday[\s]*[=]"
                                   r"|set[\s]+sql_inserts[\s]*[=]"
                                   r"|set[\s]+sql_exceptions[\s]*[=]"
                                   r"|set[\s]+sql_commit[\s]*[=][\W]*COMMIT[\s]*TRANSACTION[\W]*[;]?"
                                   r"|set[\s]+sql_rollback[\s]*[=][\W]*ROLLBACK[\s]*TRANSACTION[\W]*[;]?", file_obj, flags=flagID)

        if find_op_state:
            for ele in find_op_state:
                output_log.append(ele)
                # print(ele)
                ele = re.sub(r"[\s]+", " ", ele)
                output_sql_statements.append(ele)

        find_begin = re.findall(r"(?<=BEGIN).*?EXCEPTION[\s]*WHEN[\s]*ERROR[\s]*THEN.*?RETURN[\s]*;", file_obj, flags=flagID)

        if find_begin:
            for ele in find_begin:
                # print(ele)
                output_log.append(ele)
                ele = re.sub(r"SELECT.*?\;", "", ele, flags=flagID)
                # ele = re.sub(r"\}[\s]*\)[\s]*\;", ");", ele, flags=flagID)
                # ele = re.sub(r"catch.*?\{", "EXCEPTION WHEN ERROR THEN ", ele, flags=flagID)
                # ele = re.sub(r"return[\s]+", "SELECT ", ele, flags=flagID)
                # ele = re.sub(r"\{[\s]*err[\s]*\}", "${@@error.message }", ele, flags=flagID)
                # ele = re.sub(r"snowflake\.execute[\s]*\([\s]*\{[\s]*sqlText\:[\s]*", "EXECUTE IMMEDIATE ( ", ele, flags=flagID)
                
                ele = re.sub(r"[\s]+", " ", ele)
                # print(ele)
                output_sql_statements.append(ele.lower())


        input_sql_statements.sort()
        output_sql_statements.sort()

        # print("input_sql_statements",len(input_sql_statements),input_sql_statements)
        # print("output_sql_statements",len(output_sql_statements),output_sql_statements)
   
        # print("input_log",input_log)
        # print("output_log",output_log)
    
        if len(input_sql_statements)>0:
            if len(input_sql_statements) == len(output_sql_statements):
                # print("YYYY")
                result_sf_check_no_of_statements_in_script.append("Y")
                log_result.append(Result_Yes(input_log,output_log))
            else:
                result_sf_check_no_of_statements_in_script.append("N")
                log_result.append(Result_No(input_log,output_log))
        else:
            result_sf_check_no_of_statements_in_script.append("N")
            log_result.append(Result_No(input_log,output_log))

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_check_no_of_statements_in_script.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_Match_join_count(log_result,filename,directory,result_sf_Match_join_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Match Join Count"))
    input_joins=[]
    output_joins=[]
    input_log=[]
    output_log=[]
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)

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

        find_in_joins = re.findall(r"INNER[\s]+JOIN|LEFT[\s]+JOIN|RIGHT[\s]+JOIN|LEFT[\s]+OUTER[\s]+JOIN|FULL[\s]+OUTER[\s]+JOIN|[\s]+JOIN", file_obj, flags=re.I|re.DOTALL)
        # print("find_in_joins", find_in_joins)

        if find_in_joins:
            for ele in find_in_joins:
                ele = re.sub(r"[\s]+", " ", ele)
                ele = re.sub(r"LEFT[\s]+OUTER[\s]+JOIN", "LEFT JOIN", ele, flags=re.I)
                ele = re.sub(r"FULL[\s]+OUTER[\s]+JOIN", "FULL JOIN", ele, flags=re.I)
                if ' JOIN' == ele.upper():
                    input_log.append(ele.upper())
                    ele = re.sub(r"[\s]+JOIN", "INNER JOIN", ele, flags=re.I)
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
                                   r"|FULL[\s]+JOIN", file_obj, flags=re.I|re.DOTALL)
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
            result_sf_Match_join_count.append("NA")
        elif len(input_joins) == len(output_joins):
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_sf_Match_join_count.append("Y("+str(len(input_joins))+"-"+str(len(output_joins))+")")
        elif len(input_joins) < len(output_joins) and find_delete:
            result_sf_Match_join_count.append("CM")
            log_result.append(Result_CM("check Output file multiple joins bcz of select in delete statement."))
        else:
            log_result.append(Result_No(input_log,output_log))
            result_sf_Match_join_count.append("N("+str(len(input_joins))+"-"+str(len(output_joins))+")")        

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_Match_join_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_check_join_count(log_result,filename,directory,result_sf_check_join_count):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("Match Join Count"))
    # print(Check_features_of_SF_to_BQ)
    input_joins=[]
    output_joins=[]
    input_log=[]
    output_log=[]
    try:
        # Input
        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        file_obj = re.sub(r"AS[\s]+[\w]+|\/\/.*?\n", "", file_obj, flags=flagID)
        file_obj = re.sub(r"\,[\s]*[\w]+", "", file_obj, flags=flagID)

        find_in_joins = re.findall(r"INNER[\s]+JOIN"
                                   r"|LEFT[\s]+JOIN"
                                   r"|RIGHT[\s]+JOIN"
                                   r"|LEFT[\s]+OUTER[\s]+JOIN"
                                   r"|FULL[\s]+OUTER[\s]+JOIN"
                                   r"|[\s]+JOIN", file_obj, flags=re.I|re.DOTALL)
        # print("find_in_joins", find_in_joins)

        if find_in_joins:
            for ele in find_in_joins:
                ele = re.sub(r"[\s]+", " ", ele)
                ele = re.sub(r"LEFT[\s]+OUTER[\s]+JOIN", "LEFT JOIN", ele, flags=re.I)
                ele = re.sub(r"FULL[\s]+OUTER[\s]+JOIN", "FULL JOIN", ele, flags=re.I)
                if ' JOIN' == ele.upper():
                    input_log.append(ele.upper())
                    ele = re.sub(r"[\s]+JOIN", "INNER JOIN", ele, flags=re.I)
                    # print(ele)
                    input_joins.append(ele.upper())
                else:
                    input_log.append(ele.upper())
                    input_joins.append(ele.upper())

        
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)

        find_op_joins = re.findall(r"LEFT[\s]+OUTER[\s]+JOIN"
                                   r"|INNER[\s]+JOIN"
                                   r"|LEFT[\s]+JOIN"
                                   r"|RIGHT[\s]+JOIN"
                                   r"|FULL[\s]+JOIN"
                                   r"|[\s]+JOIN", file_obj, flags=re.I|re.DOTALL)

        # print("find_op_joins", find_op_joins)

        if find_op_joins:
            for ele in find_op_joins:
                if ' JOIN' == ele.upper():
                    input_log.append(ele.upper())
                    ele = re.sub(r"[\s]+JOIN", "INNER JOIN", ele, flags=re.I)
                    # print(ele)
                    input_joins.append(ele.upper())
                else:
                    ele = re.sub(r"[\s]+", " ", ele)
                    output_log.append(ele.upper())
                    output_joins.append(ele.upper())

        input_joins.sort()
        output_joins.sort()

        # print("input_joins", len(input_joins), input_joins)
        # print("output_joins", len(output_joins), output_joins)   

        if len(input_joins)==0 and len(output_joins)==0:
            log_result.append(Result_NA())
            result_sf_check_join_count.append("NA")
        elif len(input_joins) == len(output_joins):
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_sf_check_join_count.append("Y("+str(len(input_joins))+"-"+str(len(output_joins))+")")
        elif len(input_joins) < len(output_joins):
            result_sf_check_join_count.append("CM")
            log_result.append(Result_CM("check Output file multiple joins bcz of select in delete statement."))
        else:
            # print("NNNN")
            # print("input_joins", len(input_joins), input_joins)
            # print("output_joins", len(output_joins), output_joins)
            log_result.append(Result_No(input_log,output_log))
            result_sf_check_join_count.append("N("+str(len(input_joins))+"-"+str(len(output_joins))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_check_join_count.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)

def fun_sf_check_TRY_TO_functions(log_result,filename,directory,result_sf_check_TRY_TO_functions):
    same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup(filename,directory)
    log_result.append(Common("check_TRY_TO_functions"))
    input_function = []
    output_function = []
    input_log = []
    output_log = []
    try:

        file_obj = common_obj.Remove_comments(inputFolderPath+filename)
        # print(file_obj)  r"RIGHT[\s]*\([\s]*[\w]+.*?\)"
        file_obj = re.sub(r"RIGHT[\s]*\(.*?\)", "", file_obj, flags=flagID)


        find_try_to_boolean = re.findall(r"TRY_TO_BOOLEAN[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_try_to_boolean:
            input_log.append(ele)
            ele = re.sub(r"TRY_TO_BOOLEAN[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS BOOL)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        find_try_to_date = re.findall(r"TRY_TO_DATE[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_try_to_date:
            input_log.append(ele)
            ele = re.sub(r"TRY_TO_DATE[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS DATE)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        find_try_to_decimal_number = re.findall(r"TRY_TO_DECIMAL[\s]*\([\s]*[\w]+.*?\)"
                                                r"|TRY_TO_NUMBER[\s]*\([\s]*[\w]+.*?\)"
                                                r"|TRY_TO_NUMERIC[\s]*\([\s]*[\w]+.*?\)", file_obj, flags=flagID)
        for ele in find_try_to_decimal_number:
            input_log.append(ele)
            # print("ele", ele)
            ele = re.sub(r"TRY_TO_DECIMAL[\s]*\(|TRY_TO_NUMBER[\s]*|TRY_TO_NUMERIC[\s]*\(", "ROUND(SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\(\(", "(", ele)
            ele = re.sub(r"\)|\,[\s]*[\d]+", "", ele)
            ele = ele + ' AS NUMERIC)'
            ele = re.sub(r"[\s]+", " ", ele)
            # print("ele", ele)
            input_function.append(ele)

        find_try_to_double = re.findall(r"TRY_TO_DOUBLE[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_try_to_double:
            input_log.append(ele)
            ele = re.sub(r"TRY_TO_DOUBLE[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS FLOAT64)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        find_try_to_time = re.findall(r"TRY_TO_TIME[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_try_to_time:
            input_log.append(ele)
            ele = re.sub(r"TRY_TO_TIME[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS TIME)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        
        find_try_to_timestamp = re.findall(r"TRY_TO_TIMESTAMP[\s]*\(.*?\)"
                                           r"|TRY\_TO\_TIMESTAMP\_LTZ[\s]*\(.*?\)"
                                           r"|TRY\_TO\_TIMESTAMP\_NTZ[\s]*\(.*?\)"
                                           r"|TRY\_TO\_TIMESTAMP\_TZ[\s]*\(.*?\)", file_obj, flags=flagID)

        for ele in find_try_to_timestamp:
            input_log.append(ele)
            
            ele = re.sub(r"TRY_TO_TIMESTAMP[\s]*[\w]*[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS TIMESTAMP)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        find_nvl = re.findall(r"NVL[\s]*\(", file_obj, flags=flagID)
        for ele in find_nvl:
            input_log.append(ele)
            ele = re.sub(r"NVL[\s]*\(", "IFNULL(", ele, flags=flagI)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        find_to_timestamp_ntz = re.findall(r"TO_TIMESTAMP_NTZ[\s]*\(.*?\)|TO_TIMESTAMP_LTZ[\s]*\(.*?\)", file_obj, flags=flagID)
        # print("find_to_timestamp_ntz", find_to_timestamp_ntz)
        for ele in find_to_timestamp_ntz:
            input_log.append(ele)
            # print("ele", ele)
            if '${' in ele:
                ele = re.sub(r"TO_TIMESTAMP_NTZ[\s]*\(|TO_TIMESTAMP_LTZ[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
                ele = re.sub(r"\$\{", "''' || ", ele)
                ele = re.sub(r"\}", " || '''", ele)
                ele = re.sub(r"\)", " AS TIMESTAMP)", ele)
                ele = re.sub(r"\'\'\'\'", "'''", ele)
                ele = re.sub(r"[\s]+", " ", ele)
                # print("ele", ele)
                input_function.append(ele)

            else:
                ele = re.sub(r"TO_TIMESTAMP_NTZ[\s]*\(|TO_TIMESTAMP_LTZ[\s]*\(", "SAFE_CAST(", ele, flags=flagI)
                ele = re.sub(r"\)", " AS TIMESTAMP)", ele)
                ele = re.sub(r"[\s]+", " ", ele)
                # print("ele", ele)
                input_function.append(ele)


        find_to_varchar = re.findall(r"TO_VARCHAR[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_to_varchar:
            input_log.append(ele)
            ele = re.sub(r"TO_VARCHAR[\s]*\(", "CAST(", ele, flags=flagI)
            ele = re.sub(r"\)", " AS STRING)", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)


        find_to_date = re.findall(r"[\W]TO_DATE[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_to_date:
            # print("ele", ele)
            input_log.append(ele)
            if '${' in ele:
                ele = re.sub(r"[\W]TO_DATE[\s]*\(", "CAST(", ele, flags=flagI)
                ele = re.sub(r"\$\{", "''' || ", ele)
                ele = re.sub(r"\}", " || '''", ele)
                ele = re.sub(r"\)", " AS DATE)", ele)
                ele = re.sub(r"\'\'\'\'", "'''", ele)
                ele = re.sub(r"[\s]+", " ", ele)
                # print("ele", ele)
                input_function.append(ele)

            else:
                ele = re.sub(r"[\W]TO_DATE[\s]*\(", "CAST(", ele, flags=flagI)
                ele = re.sub(r"\)", " AS DATE)", ele)
                ele = re.sub(r"[\s]+", " ", ele)
                # print("ele", ele)
                input_function.append(ele)


        find_to_timestamp = re.findall(r"[\W]TO_TIMESTAMP[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_to_timestamp:
            input_log.append(ele)
            # print("ele", ele)
            ele = re.sub(r"[\W]TO_TIMESTAMP[\s]*\(", "CAST(", ele, flags=flagI)
            ele = re.sub(r"\$\{", "''' || ", ele)
            ele = re.sub(r"\}", " || '''", ele)
            ele = re.sub(r"\)", " AS TIMESTAMP)", ele)
            ele = re.sub(r"\'\'\'\'", "'''", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            # print("ele", ele)
            input_function.append(ele)

        find_len = re.findall(r"LEN[\s]*\(.*?\)", file_obj, flags=flagID)
        for ele in find_len:
            input_log.append(ele)
            ele = re.sub(r"LEN[\s]*\(", "LENGTH(", ele, flags=flagI)
            ele = re.sub(r"[\s]+", " ", ele)
            input_function.append(ele)

        # find_cast = re.findall(r"CAST[\s]*\([\w]+\.[\w]+[\s]+AS[\s]+DATE\)", file_obj, flags=flagID)
        # for ele in find_cast:
        #     input_log.append(ele)
        #     ele = re.sub(r"CAST[\s]*\(", "", ele, flags=flagI)
        #     ele = re.sub(r"[\s]+", " ", ele)
        #     input_function.append(ele)

        # find_right = re.findall(r"RIGHT[\s]*\([\s]*[\w]+.*?\)", file_obj, flags=flagID)
        # for ele in find_right:
        #     input_log.append(ele)
        #     # print("ele", ele)
        #     ele = re.sub(r"RIGHT[\s]*\(", "RIGHT(CAST(", ele, flags=flagI)
        #     ele = re.sub(r"\,[\s]*[\d]+", "", ele)
        #     ele = re.sub(r"\)", " AS STRING)", ele)
        #     ele = re.sub(r"[\s]+", " ", ele)
        #     # print("ele", ele)
        #     input_function.append(ele)

                
        # Output
        file_obj = common_obj.Remove_comments(OutputFolderPath+filename)
        file_obj = re.sub(r"RIGHT[\s]*\(.*?\)", "", file_obj, flags=flagID)
        
        find_function_op = re.findall(r"ROUND[\s]*\([\s]*SAFE\_CAST[\s]*\(.*?\)"
                                      r"|SAFE\_CAST[\s]*\(.*?\)"
                                      r"|CAST[\s]*\(.*?\)" #r"|(?<=CAST)[\s]*\(.*?\)"
                                      r"|IFNULL[\s]*\(", file_obj, flags=flagID)  #r"|RIGHT[\s]*\(CAST[\s]*\(.*?\)"  #r"|CAST[\s]*\(.*?\)"
        
        # print("find_function_op", find_function_op)

        for ele in find_function_op:
            output_log.append(ele)
            # print(ele)
            ele = re.sub(r"^\(", "", ele)
            ele = re.sub(r"[\s]+", " ", ele)
            output_function.append(ele)
            

        input_function.sort()
        output_function.sort()

        # print("input_function-", len(input_function), input_function)
        # print("output_function-", len(output_function), output_function)

        if len(input_function) == 0 and len(output_function) == 0:
            log_result.append(Result_NA())
            result_sf_check_TRY_TO_functions.append("NA")
        elif input_function == output_function:
            # print("YYYY")
            log_result.append(Result_Yes(input_log,output_log))
            result_sf_check_TRY_TO_functions.append("Y("+str(len(input_function))+"-"+str(len(output_function))+")")
        else:
            print("NNNN")
            print("input_function-", len(input_function), input_function)
            print("output_function-", len(output_function), output_function)
            log_result.append(Result_No(input_log,output_log))
            result_sf_check_TRY_TO_functions.append("N("+str(len(input_function))+"-"+str(len(output_function))+")")

    except Exception as e:
        print("Unexpected error:", sys.exc_info()[0])
        result_sf_check_TRY_TO_functions.append("Encoding")
        log_result.append(Result_Encoding())
        print("Error in processing: ", filename)


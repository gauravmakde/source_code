from Cf_Assurance_For_TD import *
from CF_Assurance_For_NZ import *
from Cf_Assurance_For_HV import *
from Cf_Assurance_For_SF import *
from CF_Assurance_Common_Functions import *
import pandas as pd
from rich.progress import track
import configparser


config = configparser.ConfigParser()
execPath = os.getcwd()
print(execPath)
config.read(os.path.join(execPath, 'Config_Automation.ini'))
print(config)

Create_Database=(config['DATABASE']['Create_Database'])
Extracted_AVRO_File_Path=(config['DATABASE']['Extracted_AVRO_File_Path'])
Enter_Project_Name=(config['DATABASE']['Enter_Project_Name'])



Project_Name_For_Assurance_Report=(config['ExcelName']['Project_Name_For_Assurance_Report'])


check_TD_to_BQ = (config['Teradata']['Check_Features_Of_TD_To_BQ'])# check the td and bq
check_NZ_to_BQ = (config['Netezza']['Check_Features_Of_NZ_To_BQ'])


fun_project_ID = (config['Teradata']['Project_ID'])
fun_Datatype = (config['Teradata']['Datatype']) # For datatype checking
fun_TZ_Check = (config['Teradata']['Check_TZ_Column_check'])
check_decimal_datatype_without_scale = (config['Teradata']['decimal_datatype_without_scale'])
fun_Index_and_partition = (config['Teradata']['Index_And_Partition'])
No_Of_Statements_In_scripts = (config['Teradata']['No_Of_Statements_In_Scripts'])
fun_pivot = (config['Teradata']['Pivot'])
fun_strtok = (config['Teradata']['Strtok'])
fun_hashrow = (config['Teradata']['Hashrow'])
fun_BT_and_ET = (config['Teradata']['BT_And_ET'])
fun_View_Column_List = (config['Teradata']['View_Column_List'])
fun_sys_calender = (config['Teradata']['Sys_Calender'])
fun_cross_join = (config['Teradata']['Cross_Join'])
fun_comment = (config['Teradata']['Comment'])
fun_not_between = (config['Teradata']['Not_Between'])
fun_Hard_Coded_Value = (config['Teradata']['Hard_Coded_Value'])
fun_Distinct_Not_Handled_In_The_Converted_Codes= (config['Teradata']['Check_Distinct'])
fun_Default_Coloumn_Values= (config['Teradata']['Default_Coloumn_Values'])

fun_Unique_Primary_Index_Cloumn_FileList_Path= (config['Teradata']['Unique_Primary_Index_Cloumn_FileList_Path'])
fun_Unique_Primary_Index= (config['Teradata']['Unique_Primary_Index'])
fun_Coalesce = (config['Teradata']['Coalesce'])
fun_Check_Exception = (config['Teradata']['Check_Exception'])
fun_System_Table = (config['Teradata']['System_Table'])
Validate_On_BigQuery = (config['ValidateQueryOnBQ']['Validate_On_BigQuery'])
Execute_On_BigQuery = (config['ValidateQueryOnBQ']['Execute_On_BigQuery'])
Param_BQ_change = (config['ValidateQueryOnBQ']['Param_BQ_change'])
folder_configs = (config['ValidateQueryOnBQ']['folder_configs'])
config_file_name = (config['ValidateQueryOnBQ']['config_file_name'])
static_config_filename = (config['ValidateQueryOnBQ']['static_config_filename'])
folder_structure = (config['ValidateQueryOnBQ']['folder_structure'])


fun_CS_Table_DataType_Check= (config['Teradata']['CS_Table_DataType_Check'])
fun_CS_Table_Column_Case_Check= (config['Teradata']['CS_Table_Column_Case_Check'])
fun_CS_View_Column_Case_Check= (config['Teradata']['CS_View_Column_Case_Check'])
fun_CS_Table_Column_Names_In_Lower_Case= (config['Teradata']['CS_Table_Column_Names_In_Lower_Case'])
fun_CS_star_columns= (config['Teradata']['CS_star_columns'])
fun_CS_View_Source_Object_Alias_Check= (config['Teradata']['CS_View_Source_Object_Alias_Check'])
fun_IF_Activity_Count= (config['Teradata']['IF_Activity_Count'])
fun_For_Null_And_Not_Null= (config['Teradata']['For_Null_And_Not_Null'])
fun_For_ON_After_Join= (config['Teradata']['For_ON_Condition_After_JOIN'])
fun_Check_Default_Value_For_TD= (config['Teradata']['Check_Default_Value_For_TD'])
fun_Check_Timezone= (config['Teradata']['Check_Timezone'])
fun_Primary_Index_To_Cluster_By_CSWG= (config['Teradata']['Primary_Index_To_Cluster_By_CSWG'])
fun_Joins_CSWG= (config['Teradata']['Joins_CSWG'])
fun_Check_No_Of_Statements_For_CSWG= (config['Teradata']['Check_No_Of_Statements_For_CSWG'])
fun_Check_Table_And_View_In_Upper_Case= (config['Teradata']['Check_Table_And_View_In_Upper_Case'])
BQ_JSON_PATH_CSWG= (config['Teradata']['BQ_JSON_PATH_CSWG'])
Parameter_JSON_File_Path_CSWG= (config['Teradata']['Parameter_JSON_File_Path_CSWG'])
Excel_For_Other_Table_Schema= (config['Teradata']['Excel_For_Other_Table_Schema'])
fun_BQ_Validation_For_CSWG= (config['Teradata']['BQ_Validation_For_CSWG'])
fun_check_cast_to_safe_cast= (config['Teradata']['check_cast_to_safe_cast'])
fun_check_procedure_added_In_Output_File= (config['Teradata']['check_procedure_added_In_Output_File'])
fun_check_current_timestamp_to_current_datetime= (config['Teradata']['check_current_timestamp_to_current_datetime'])
fun_Check_auther_comment_wells_fargo= (config['Teradata']['Check_auther_comment_wells_fargo'])
fun_Check_Lower_For_Special_Characters= (config['Teradata']['Check_Lower_For_Special_Characters'])
fun_Check_DATEDIFF_Function_In_Output_File= (config['Teradata']['Check_DATEDIFF_Function_In_Output_File'])
fun_Check_Group_By_Columns_In_Insert_Statement= (config['Teradata']['Check_Group_By_Columns_In_Insert_Statement'])
fun_CS_View_Column_Names_Check= (config['Teradata']['CS_View_Column_Names_Check'])
fun_CS_View_Objects_Compare= (config['Teradata']['CS_View_Objects_Compare'])
fun_CS_Join_Type_Count= (config['Teradata']['CS_Join_Type_Count'])
fun_CS_Check_Union_And_Union_All= (config['Teradata']['CS_Check_Union_And_Union_All'])
fun_CS_No_Of_Lines_In_File= (config['Teradata']['CS_No_Of_Lines_In_File'])
fun_CS_Compare_Hardcoded_Values= (config['Teradata']['CS_Compare_Hardcoded_Values'])
fun_CS_Check_Date_Format= (config['Teradata']['CS_Check_Date_Format'])
fun_CS_Check_Backtick_In_BTEQ= (config['Teradata']['CS_Check_Backtick_In_BTEQ'])
fun_CS_Count_Doller_Variable= (config['Teradata']['CS_Count_Doller_Variable'])
fun_CS_Check_IF_FALSE_In_Output= (config['Teradata']['CS_Check_IF_FALSE_In_Output'])
fun_CS_Check_Lower_Upper_Trim_In_Where= (config['Teradata']['CS_Check_Lower_Upper_Trim_In_Where'])
fun_CS_Check_Group_By_In_File= (config['Teradata']['CS_Check_Group_By_In_File'])
fun_CS_Check_Aggregator_Functions= (config['Teradata']['CS_Check_Aggregator_Functions'])
fun_CS_Parenthesis_Count_In_Input_And_Output= (config['Teradata']['CS_Parenthesis_Count_In_Input_And_Output'])
fun_CS_CLAUSES_Count_In_INPUT_And_Output= (config['Teradata']['CS_CLAUSES_Count_In_INPUT_And_Output'])
fun_CS_Check_Date_Functions= (config['Teradata']['CS_Check_Date_Functions'])



#fastload Assurance
Enable_For_Fload_Assurance= (config['Fload_Assurance']['Enable_For_Fload_Assurance'])

Schema_And_Value_Json= (config['Fload_Assurance']['Schema_And_Value_Json'])
Translate_Table_Folder_Path= (config['Fload_Assurance']['Translate_Table_Folder_Path'])
fun_Check_Database_Values_Added_Or_NOT= (config['Fload_Assurance']['Check_Database_Values_Added_Or_NOT'])
fun_Check_Delimiter_Added_In_sed_Command= (config['Fload_Assurance']['Check_Delimiter_Added_In_sed_Command'])
fun_Check_Drop_And_Create_Table= (config['Fload_Assurance']['Check_Drop_And_Create_Table'])
fun_Check_Error1_Table_Statement= (config['Fload_Assurance']['Check_Error1_Table_Statement'])
fun_Check_Error2_Table_Statement= (config['Fload_Assurance']['Check_Error2_Table_Statement'])
fun_Check_Partition_By_And_Order_By_In_INSERT= (config['Fload_Assurance']['Check_Partition_By_And_Order_By_In_INSERT'])


#KSH_Assurance
Assurance_For_KSH= (config['KSH_Assurance']['Assurance_For_KSH'])
fun_trap_commented_or_not= (config['KSH_Assurance']['Trap_commented_or_not'])
fun_bteq_commented_or_not= (config['KSH_Assurance']['Bteq_commented_or_not'])
fun_error_check_added_or_not_in_file= (config['KSH_Assurance']['error_check_added_or_not_in_file'])
fun_sed_added_in_function_call_or_not= (config['KSH_Assurance']['sed_added_in_function_call_or_not'])
fun_create_table_added_after_os_and_export= (config['KSH_Assurance']['create_table_added_after_os_and_export'])
fun_email_logic= (config['KSH_Assurance']['email_logic'])
fun_f_get_aedw_open_batches= (config['KSH_Assurance']['find_f_get_aedw_open_batches'])
fun_f_execute_bteq_calling= (config['KSH_Assurance']['find_f_execute_bteq_calling'])
# fun_fn_sql= (config['KSH_Assurance']['find_fn_sql'])
# fun_Seq_MstSeq= (config['KSH_Assurance']['check_Seq_MstSeq'])
fun_comment_for_usrid_pwd_tdpid_logon= (config['KSH_Assurance']['check_comment_for_usrid_pwd_tdpid_logon'])
# fun_cheak_function_call= (config['KSH_Assurance']['cheak_function_call'])
fun_check_comment_present_in_echo= (config['KSH_Assurance']['check_comment_present_in_echo'])


Assurance_For_KSH_Sadiaa = (config['KSH_Assurance']['Assurance_For_KSH_Sadiaa'])
fun_execute_query_before_sql = (config['KSH_Assurance']['execute_query_before_sql'])
fun_check_bteq_logon_database = (config['KSH_Assurance']['check_bteq_logon_database'])
fun_Check_auther_comment_sadiaa = (config['KSH_Assurance']['Check_auther_comment_sadiaa'])


#Netezza To BQ
fun_project_id_for_NZ=(config['Netezza']['Check_Project_Id_For_NZ'])
fun_datatype_for_NZ=(config['Netezza']['Check_Datatype_For_NZ'])
fun_cluster_and_partition_for_NZ=(config['Netezza']['Cluster_And_Partition'])
fun_check_comments=(config['Netezza']['Check_Comments_For_NZ'])
fun_no_of_statements_for_NZ=(config['Netezza']['No_Of_Statements_For_NZ'])
fun_cross_join_for_NZ=(config['Netezza']['Cross_Join_For_NZ'])
fun_not_like_for_NZ=(config['Netezza']['Check_Not_Like_Predicate'])
fun_now_for_NZ=(config['Netezza']['Check_Now_For_NZ'])
fun_check_default_schema_for_NZ=(config['Netezza']['Check_Default_Schema_For_NZ'])
fun_Case_when_for_NZ=(config['Netezza']['Check_Case_when_For_NZ'])
fun_Table_In_Case_for_NZ=(config['Netezza']['Check_Table_In_Lower_Case'])
fun_Word_After_As_In_Lower_for_NZ=(config['Netezza']['Check_Alise_In_Lower_For_NZ'])
fun_Check_BQ_ProjectID_For_Virgin_Media=(config['Netezza']['Check_BQ_ProjectID_For_Virgin_Media'])
fun_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION=(config['Netezza']['Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION'])

#MemSQL To BQ
Check_Features_For_MemSQL_To_BQ=(config['MemSQL']['Check_Features_For_MemSQL_To_BQ'])
fun_Check_No_Of_Statements_For_MemSQL=(config['MemSQL']['Check_No_Of_Statements_For_MemSQL'])
fun_Check_Count_Of_Case_Statement=(config['MemSQL']['Check_Count_Of_Case_Statement'])
fun_Check_Count_Of_Doller_Parameters=(config['MemSQL']['Check_Count_Of_Doller_Parameters'])
fun_Check_Count_Of_Coalesce_Function=(config['MemSQL']['Check_Count_Of_Coalesce_Function'])



#Hive To BQ ##########################################
check_HV_to_BQ = (config['Hive']['Check_features_of_HV_to_BQ'])
Enable_Verizon = (config['Hive']['Enable_Verizon'])
Enable_Flipkart = (config['Hive']['Enable_Flipkart'])
#For Verizon
fun_datatype_for_HV=(config['Hive']['check_datatype_for_HV'])
charles_timestamp_to_datetime = (config['Hive']['check_timestamp_to_datetime'])
Flag_comment_to_option_for_HV=(config['Hive']['check_comment_to_option_for_HV'])
Flag_drop_raw_format_or_location_from_HV=(config['Hive']['drop_raw_format_or_location_from_HV'])
Flag_create_table_external_for_HV = (config['Hive']['check_create_table_external_for_HV'])
Flag_No_of_statements_in_Script_for_HV = (config['Hive']['check_no_of_statements_in_script'])
Flag_cross_join_for_HV = (config['Hive']['check_cross_join_for_HV'])

fun_project_ID_Hive = (config['Hive']['Project_ID_Hive'])
Flag_check_colan_to_space=(config['Hive']['check_colan_to_space'])
Flag_check_map_to_array=(config['Hive']['check_map_to_array'])
par_col_datatype_sheet_path = (config['Hive']['par_col_datatype_sheet_path'])
Flag_Check_par_col_datatype_change = (config['Hive']['Check_par_col_datatype_change'])
add_option_sheet_path = (config['Hive']['add_option_sheet_path'])
Flag_Check_add_option_change = (config['Hive']['Check_add_option_change'])

Flag_Check_PartitionBy_to_ClusterBy=(config['Hive']['Check_PartitionBy_to_ClusterBy'])
Flag_Check_clusterBy_create_cols=(config['Hive']['Check_clusterBy_create_cols'])
Flag_Check_Partitioned_BY_to_Partition_By=(config['Hive']['Check_Partitioned_BY_to_Partition_By'])

Flag_check_for_current_functions=(config['Hive']['check_for_current_functions'])

#Functions
Flag_schema_table_case=(config['Hive']['schema_table_case'])
Flag_column_and_its_alias_check=(config['Hive']['column_and_its_alias_check'])
#Flag_column_alias_case=(config['Hive']['column_alias_case'])
Flag_From_Unixtime=(config['Hive']['From_Unixtime'])
Flag_NVL_Columns=(config['Hive']['NVL_Columns'])
Flag_DATE_SUB_Columns=(config['Hive']['DATE_SUB_Columns'])
Flag_To_Date_Columns=(config['Hive']['To_Date_Columns'])
Flag_Date_Add_Columns=(config['Hive']['Date_Add_Columns'])
Flag_Date_Diff_Columns=(config['Hive']['Date_Diff_Columns'])

#For Flipkart
Native_table_text_path=(config['Hive']['Native_table_text_path'])
Partition_Cluster_Change_Datatype_Path=(config['Hive']['Partition_Cluster_Change_Datatype_Path'])
External_Table_GS_Path=(config['Hive']['External_Table_GS_Path'])
fkt_check_native_external_table=(config['Hive']['fkt_check_native_external_table'])
fkt_fun_check_external_table_path_for_HV=(config['Hive']['fkt_check_external_table_path_for_HV'])
fkt_Flag_Check_Partitioned_BY_to_Partition_By=(config['Hive']['fkt_Check_Partitioned_BY_to_Partition_By'])
fkt_Flag_Check_PartitionBy_to_ClusterBy=(config['Hive']['fkt_Check_PartitionBy_to_ClusterBy'])
fkt_flag_check_datetime_trunc_Granularity=(config['Hive']['fkt_check_datetime_trunc_Granularity'])
fkt_fun_datatype_for_HV=(config['Hive']['fkt_check_datatype_for_HV'])
fkt_check_timestamp_to_datetime=(config['Hive']['fkt_check_timestamp_to_datetime'])
fkt_flag_check_struct_datatype=(config['Hive']['fkt_check_struct_datatype'])
fkt_Flag_comment_to_option_for_HV=(config['Hive']['fkt_check_comment_to_option_for_HV'])
fkt_Flag_drop_raw_format_or_location_from_HV=(config['Hive']['fkt_drop_raw_format_or_location_from_HV'])
fkt_Flag_create_table_external_for_HV = (config['Hive']['fkt_check_create_table_external_for_HV'])
fkt_Flag_No_of_statements_in_Script_for_HV = (config['Hive']['fkt_check_no_of_statements_in_script'])
fkt_Flag_cross_join_for_HV = (config['Hive']['fkt_check_cross_join_for_HV'])
fkt_fun_project_ID = (config['Hive']['fkt_Project_ID'])
fkt_Flag_Append_project_ID_to_udf_function=(config['Hive']['fkt_Append_project_ID_to_udf_function'])
fkt_Flag_check_colan_to_space=(config['Hive']['fkt_check_colan_to_space'])
fkt_Flag_check_map_to_array=(config['Hive']['fkt_check_map_to_array'])
fkt_check_d_added_for_number_in_column=(config['Hive']['fkt_check_d_added_for_number_in_column'])
fkt_check_keyword_columns=(config['Hive']['fkt_check_keyword_columns'])

fkt_check_partition_col_datatype=(config['Hive']['fkt_check_partition_col_datatype'])

fkt_Flag_match_table_view_count=(config['Hive']['fkt_match_table_view_count'])
fkt_Match_join_count=(config['Hive']['fkt_Match_join_count'])
fkt_ins_sol_for_without_partition_tbl=(config['Hive']['fkt_ins_sol_for_without_partition_tbl'])
fkt_del_ins_sol_for_partition_tbl=(config['Hive']['fkt_del_ins_sol_for_partition_tbl'])
fkt_flag_check_safe_with_function=(config['Hive']['fkt_check_safe_with_function'])
fkt_flag_check_lateral_view=(config['Hive']['fkt_check_lateral_view'])
fkt_Flag_match_schema_table_case=(config['Hive']['fkt_match_schema_table_case'])
fkt_Flag_check_table_alias=(config['Hive']['fkt_check_table_alias'])
fkt_check_schema_in_seriliser_deser=(config['Hive']['fkt_check_schema_in_seriliser_deser'])
fkt_check_comparison_par_col_using_sheet=(config['Hive']['fkt_check_comparison_par_col_using_sheet'])
fkt_check_nullif_column = (config['Hive']['fkt_check_nullif_column'])
fkt_check_limit_count = (config['Hive']['fkt_check_limit_count'])
fkt_check_parse_date_to_case_when = (config['Hive']['fkt_check_parse_date_to_case_when'])
fkt_match_cte_table_count = (config['Hive']['fkt_match_cte_table_count'])
fkt_match_case_when_count = (config['Hive']['fkt_match_case_when_count'])
fkt_check_group_by_in_file = (config['Hive']['fkt_check_group_by_in_file'])
fkt_check_truncate_with_begin_transaction = (config['Hive']['fkt_check_truncate_with_begin_transaction'])
fkt_check_insert_column_names=(config['Hive']['fkt_check_insert_column_names'])
fkt_match_aggregate_func_count=(config['Hive']['fkt_match_aggregate_func_count'])
fkt_match_clauses_count=(config['Hive']['fkt_match_clauses_count'])
fkt_check_date_sub_func=(config['Hive']['fkt_check_date_sub_func'])


fkt_Flag_check_cast_count=(config['Hive']['fkt_check_cast_count'])
hash_parameter_csv_path=(config['Hive']['hash_parameter_csv_path'])
fkt_check_hash_parameter=(config['Hive']['fkt_check_hash_parameter'])
fkt_check_timezone=(config['Hive']['fkt_check_timezone'])
fkt_check_safe_cast=(config['Hive']['fkt_check_safe_cast'])
fkt_Flag_partition_timestamp_to_date_function=(config['Hive']['fkt_partition_timestamp_to_date_function'])


#Snowflake To BQ
Check_features_of_SF_to_BQ = (config['Snowflake']['Check_features_of_SF_to_BQ']) 
sf_check_datatype = (config['Snowflake']['sf_check_datatype'])
sf_project_id = (config['Snowflake']['sf_project_id'])
sf_check_no_of_statements_in_script = (config['Snowflake']['sf_check_no_of_statements_in_script'])
sf_check_join_count = (config['Snowflake']['sf_check_join_count'])
sf_match_schema_table_case = (config['Snowflake']['sf_match_schema_table_case'])
sf_check_TRY_TO_functions = (config['Snowflake']['sf_check_TRY_TO_functions'])


#create database
if Create_Database=="Y":
    create_database(Extracted_AVRO_File_Path,Enter_Project_Name)

    DB_Conection=create_connection(Enter_Project_Name)

same_file,OutputFolderPath,inputFolderPath,Append_project_id,inputFile,outputFile,cred_ValidateQueryOnBQ=Config_Setup()
print("Configuration is done and fetched") # check the configuration

Log_Files_Folder()
DateTime()
file_name=[]
final_result=[]
project_ID=[]
DataType_result=[]
TZ_result=[]
index=[]
partition_result=[]
Result_No_Of_Statement=[]
Number_Pivot_in_Script=[]
strtok_final_result=[]
Hashrow_in_Script=[]
BT_ET_Result=[]
comment_result=[]
View_Column_List_result=[]
sys_calender_result=[]
JoinResult=[]
not_between_result=[]
Hard_Coded_Value_Result=[]
Check_Distinct_Result=[]
Default_Coloumn_Values_Result=[]
Unique_Primary_Index_Result=[]
Coalesce_Result=[]
check_Exception_Result=[]
check_System_Table_Result=[]
BQ=[]
executeOnBQ=[]
paramsBQ=[]
result_Cs_DataType_Check=[]
result_CS_Col_Name_Case=[]
result_CS_View_Case_Check=[]
result_Cs_Lower_Check=[]
result_CS_star_columns=[]
result_CS_View_Source_Object_Alias_Check=[]
result_for_if_activity=[]
result_for_null=[]
result_for_not_null=[]
result_for_on_condition=[]
result_for_default_value=[]
result_Check_Timezone=[]
result_Primary_Index_To_Cluster_By_CSWG=[]
result_Joins_CSWG=[]
result_no_of_statement_cswg=[]
result_table_and_view_in_upper=[]
result_bq_validation_cswg=[]
result_auther_comment_wells_fargo=[]
result_lower_for_special_character=[]
result_datediff_function=[]
result_group_by_columns=[]
result_check_cast_to_safe_cast=[]
result_check_procedure_added_In_Output_File=[]
result_check_current_timestamp_to_current_datetime=[]
result_view_columns_order_in_input_and_output=[]
result_view_columns_in_input_and_output=[]
result_view_column_count=[]
result_from_and_join_in_view=[]
result_from_and_join_count_in_view=[]
result_inner_join_count=[]
result_cross_join_count=[]
result_left_join_count=[]
result_right_join_count=[]
result_union_and_union_all=[]
result_cs_file_line_count=[]
result_cs_compare_hardcoded_value=[]
result_cs_check_date_format=[]
result_cs_check_backtick_in_BTEQ=[]
result_cs_count_doller_variable=[]
result_cs_check_if_false_in_output=[]
result_cs_check_lower_upper_trim_in_where=[]
result_cs_group_by=[]
result_cs_aggregator_function=[]
result_cs_parenthesis_count=[]
result_cs_clause_count=[]
result_cs_date_functions=[]


#Fload_Assurance
fload_database_value_result=[]
fload_delimiter_in_sed_result=[]
fload_drop_and_create_table_result=[]
fload_error1_table_result=[]
fload_error2_table_result=[]
fload_partition_and_order_by_result=[]

#KSH_Assurance
result_of_trap_comment=[]
result_of_bteq_comment=[]
result_for_error_check_added_or_not_in_file=[]
result_for_sed_added_in_function_call_or_not=[]
result_for_create_table_added_after_os_and_export=[]
result_for_email_logic=[]
result_for_f_get_aedw_open_batches=[]
result_for_f_execute_bteq_calling=[]
# result_for_fn_sql=[]
# result_for_Seq_MstSeq=[]
result_for_comment_for_usrid_pwd_tdpid_logon=[]
# result_cheak_function_call=[]
result_check_echo_comment=[]
result_execute_query_before_sql=[]
result_check_bteq_logon_database=[]
result_Check_auther_comment_sadiaa=[]



#Netezza To BQ
project_id_result_for_NZ=[]
Datatype_result_for_NZ=[]
Cluster_and_partition_result_for_NZ=[]
Check_comment_result_for_NZ=[]
no_of_statements_result_for_NZ=[]
cross_join_result_for_NZ=[]
not_like_result_for_NZ=[]
now_result_for_NZ=[]
default_schema_result_for_NZ=[]
Case_when_result_for_NZ=[]
lower_case_result_for_NZ=[]
alice_in_lower_case_for_NZ=[]
result_Check_BQ_ProjectID_For_Virgin_Media=[]
result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION=[]

#MemSQL To BQ
result_check_no_of_statement_for_memsql=[]
result_count_of_case_statement_for_memsql=[]
result_count_of_doller_parameter_for_memsql=[]
resut_count_of_coalesce_fun_for_memsql=[]


# HIVE To BQ ##############
#For Verizon
comment_to_option_result_for_HV = []
Datatype_result_for_HV=[]
drop_row_format_or_location_result_for_HV= []
create_table_external_for_HV = []
change_partition_by_string_for_HV = []
Result_No_Of_Statement = []
JoinResult = []
project_id_check_for_bq = []
Result_check_colan_to_space=[]
Result_check_map_to_array=[]
result_check_par_col_datatype_change=[]
result_Check_add_option_change=[]
result_Check_PartitionBy_to_ClusterBy=[]
result_Check_clusterBy_create_cols=[]
result_Check_Partitioned_BY_to_Partition_By=[]
result_check_for_current_functions=[]
result_schema_table_case=[]
result_column_and_its_alias_check=[]
#result_column_alias_case=[]
result_From_Unixtime=[]
result_NVL_Columns=[]
result_DATE_SUB_Columns=[]
result_To_Date_Columns=[]
result_Date_Add_Columns=[]
result_Date_Diff_Columns=[]

#For Flipkart
result_native_external_table=[]
external_table_path_result_for_hv=[]
result_Check_PartitionBy_to_ClusterBy=[]
result_Check_Partitioned_BY_to_Partition_By=[]
result_Check_datetime_trunc_Granularity=[]
comment_to_option_result_for_HV = []
Datatype_result_for_HV=[]
result_struct_datatype=[]
# result_decimal_datatype_with_scale=[]
drop_row_format_or_location_result_for_HV= []
create_table_external_for_HV = []
Result_No_Of_Statement = []
JoinResult = []
project_id_check_for_bq = []
result_Append_project_ID_to_udf_function=[]
Result_check_colan_to_space=[]
Result_check_map_to_array=[]
result_d_added_for_number_in_column=[]
result_keyword_columns=[]
result_partition_col_datatype=[]

result_check_cast_count=[]
result_alias=[]

result_match_table_view_count=[]
result_Match_join_count=[]
result_ins_sol_for_without_partition_tbl=[]
result_del_ins_sol_for_partition_tbl=[]
result_safe_with_function=[]
result_lateral_view=[]
result_match_schema_table_case=[]
result_check_table_alias=[]
result_check_schema_in_seriliser_deser=[]
result_check_comparison_par_col_using_sheet=[]
result_check_nullif_column = []
result_check_limit_count = []
result_check_parse_date_to_case_when = []
result_fkt_match_cte_table_count = []
result_fkt_match_case_when_count = []
result_fkt_check_group_by_in_file = []
result_fkt_check_truncate_with_begin_transaction = []
result_check_insert_column_names=[]
result_match_aggregate_func_count=[]
result_match_clauses_count=[]
result_check_date_sub_func=[]


result_check_concat_ws=[]
result_check_hash_parameter=[]
result_check_timezone=[]
result_check_safe_cast=[]
result_partition_timestamp_to_date_function=[]

#Snowflake To BQ
result_sf_match_schema_table_case = []
result_sf_project_id=[]
result_sf_check_datatype = []
result_sf_check_no_of_statements_in_script = []
result_sf_check_join_count = []
result_sf_check_TRY_TO_functions = []

if check_TD_to_BQ == 'Y' and fun_BQ_Validation_For_CSWG == 'Y':
    # print(Excel_For_Other_Table_Schema)
    if os.path.isfile(Excel_For_Other_Table_Schema):
        df=pd.read_excel(Excel_For_Other_Table_Schema)
        all_other_table={}
        for ele in df:
            all_other_table[ele]=df[ele].tolist()
    else:
        print("File not found : "+Excel_For_Other_Table_Schema)


if Validate_On_BigQuery == "Y" or Execute_On_BigQuery == 'Y' or Param_BQ_change=='Y':
    clientBQ = bigquery.Client.from_service_account_json(json_credentials_path=cred_ValidateQueryOnBQ)


if check_HV_to_BQ == 'Y' and Enable_Flipkart == 'Y' and  fkt_fun_check_external_table_path_for_HV == "Y":
    external_table_path_dict=get_external_table_path_from_sheet(External_Table_GS_Path)
    # print(external_table_path_dict)


# print("@@@@@@@@@@@@")
print(same_file)
feature_dict={"FileName":file_name}
dic_Tz=[]
# print(DateTime())
# print(same_file)
for filename,directory in track(same_file, "[cyan]Generating Report..."):
    print("File : "+filename)

    def run():
        global dic_Tz
        log_result=[]
        file_name.append(filename)

        if Validate_On_BigQuery == "Y":
            validate_on_BQ(log_result,filename,directory,BQ, clientBQ)
            feature_dict["Is_BQ_Run"]=BQ

        if Execute_On_BigQuery == "Y":
            execute_on_BQ(log_result,filename,directory,executeOnBQ, clientBQ)
            feature_dict["Call_Execution"]=executeOnBQ

        if Param_BQ_change == "Y":
            param_change_BQ(log_result,filename,directory,paramsBQ, clientBQ,folder_configs,config_file_name,static_config_filename,folder_structure)
            feature_dict["Params_change_Run_BQ_Status"]=paramsBQ

        #TD To BQ
        if check_TD_to_BQ == "Y":
            print("Inside TD to BQ")

            if fun_project_ID == "Y":
                PROJECT_ID(log_result,project_ID,filename,directory)
                feature_dict["ProjectID"]=project_ID

            if fun_Datatype == "Y":
                print("Datatype validation started")
                Data_Type(log_result,filename,directory,DataType_result,check_decimal_datatype_without_scale)
                feature_dict["Datatype"]=DataType_result

            if fun_Index_and_partition == "Y":
                Verify_Index_And_Partition(log_result,filename,directory,partition_result,index,fun_project_ID)
                feature_dict["Index"]=index
                feature_dict["Partition"]=partition_result

            if No_Of_Statements_In_scripts == "Y":
                No_Of_Statement_In_Script(log_result,filename,directory,Result_No_Of_Statement,fun_project_ID)
                feature_dict["No_Of_Statements"]=Result_No_Of_Statement

            if fun_pivot == "Y":
                Check_Pivot(log_result,filename,directory,Number_Pivot_in_Script)
                feature_dict["Pivot"]=Number_Pivot_in_Script

            if fun_hashrow == "Y":
                Hashrow_equivalent(log_result,filename,directory,Hashrow_in_Script)
                feature_dict["Hashrow"]=Hashrow_in_Script

            if fun_strtok == "Y":
                Strtok_Function(log_result,filename,directory,strtok_final_result)
                feature_dict["STRTOK"]=strtok_final_result

            if fun_BT_and_ET == "Y":
                Checking_BT_And_ET(log_result,filename,directory,BT_ET_Result)
                feature_dict["BT / ET"]=BT_ET_Result

            if fun_comment == "Y":
                check_comment_for_TD(log_result,filename,directory,comment_result)
                feature_dict["Comments"]=comment_result

            if fun_View_Column_List == "Y":
                Check_View_Column_List(log_result,filename,directory,View_Column_List_result)
                feature_dict["View_Cloumn_List"]=View_Column_List_result

            if fun_cross_join == "Y":
                check_cross_join(log_result,filename,directory,JoinResult,fun_project_ID)
                feature_dict["Join_Type_Check"]=JoinResult

            if fun_sys_calender == "Y":
                Checking_sys_calender(log_result,filename,directory,sys_calender_result)
                feature_dict["SYS_Calender"]=sys_calender_result

            if fun_Hard_Coded_Value == "Y":
                Hard_Coded_Value(log_result,filename,directory,Hard_Coded_Value_Result)
                feature_dict["Hard_Coded_Value"]=Hard_Coded_Value_Result

            if fun_not_between == "Y":
                not_between(log_result,filename,directory,not_between_result)
                feature_dict["Not_Between"]=not_between_result

            if fun_Distinct_Not_Handled_In_The_Converted_Codes == "Y":
                Check_Distinct(log_result,filename,directory,Check_Distinct_Result,DB_Conection,fun_project_ID)
                feature_dict["Check_Distinct"]=Check_Distinct_Result

            if fun_Default_Coloumn_Values == "Y":
                Default_Coloumn_Values(log_result,filename,directory,Default_Coloumn_Values_Result,DB_Conection,fun_project_ID)
                feature_dict["Default_Coloumn_Values"]=Default_Coloumn_Values_Result

            if fun_Unique_Primary_Index == "Y":
                Unique_Primary_Index(log_result,filename,directory,Unique_Primary_Index_Result,fun_Unique_Primary_Index_Cloumn_FileList_Path)
                feature_dict["Unique_Primary_Index"]=Unique_Primary_Index_Result

            if fun_Coalesce == "Y":
                COALESCE(log_result,filename,directory,Coalesce_Result)
                feature_dict["COALESCE"]=Coalesce_Result

            if fun_Check_Exception == 'Y':
                Check_Exceptions(log_result,filename,directory,check_Exception_Result)
                feature_dict["Exceptions"]=check_Exception_Result

            if fun_System_Table == 'Y':
                System_Tables(log_result,filename,directory,check_System_Table_Result)
                feature_dict["System_Table"]=check_System_Table_Result

            if fun_CS_star_columns == 'Y':
                CS_star_columns(log_result,filename,directory,result_CS_star_columns)
                feature_dict["CS_star_columns"]=result_CS_star_columns

            if fun_CS_Table_Column_Case_Check == 'Y':
                CS_Table_Column_Case_Check(log_result,filename,directory,result_CS_Col_Name_Case)
                feature_dict["CS_Col_Name_Case"]=result_CS_Col_Name_Case

            if fun_CS_Table_DataType_Check == 'Y':
                CS_Table_DataType_Check(log_result,filename,directory,result_Cs_DataType_Check)
                feature_dict["CS_DataTypes_In_Table"]=result_Cs_DataType_Check

            if fun_CS_Table_Column_Names_In_Lower_Case == 'Y':
                CS_Table_Column_Names_In_Lower_Case(log_result,filename,directory,result_Cs_Lower_Check)
                feature_dict["Cs_Table_Column_Name_In_Lower_Case"]=result_Cs_Lower_Check

            if fun_CS_View_Column_Case_Check == 'Y':
                CS_View_Column_Case_Check(log_result,filename,directory,result_CS_View_Case_Check)
                feature_dict["CS_View_Column_Case_Check"]=result_CS_View_Case_Check

            if fun_CS_View_Source_Object_Alias_Check == 'Y':
                CS_View_Source_Object_Alias_Check(log_result,filename,directory,result_CS_View_Source_Object_Alias_Check)
                feature_dict["CS_View_Source_Object_Alias_Check"]=result_CS_View_Source_Object_Alias_Check

            if fun_CS_Check_Group_By_In_File == 'Y':
                CS_Check_Group_By_In_File(log_result, filename,directory, result_cs_group_by)
                feature_dict["GROUP_BY_Column_Count(IP/OP)"]=result_cs_group_by

            if fun_CS_Check_Aggregator_Functions == 'Y':
                CS_Check_Aggregator_Functions(log_result, filename,directory, result_cs_aggregator_function)
                feature_dict["Aggregate_Function"]=result_cs_aggregator_function

            if fun_CS_Parenthesis_Count_In_Input_And_Output == 'Y':
                CS_Parenthesis_Count_In_Input_And_Output(log_result, filename,directory, result_cs_parenthesis_count)
                feature_dict["Parenthesis_Count(IP/OP)"]=result_cs_parenthesis_count

            if fun_CS_CLAUSES_Count_In_INPUT_And_Output == 'Y':
                CS_CLAUSES_Count_In_INPUT_And_Output(log_result, filename,directory, result_cs_clause_count)
                feature_dict["CLAUSES_Count(IP/OP)"]=result_cs_clause_count

            if fun_CS_Check_Date_Functions == 'Y':
                CS_Check_Date_Functions(log_result, filename,directory, result_cs_date_functions)
                feature_dict["CHECK_DATE_FUNCTION"]=result_cs_date_functions

            if fun_CS_View_Column_Names_Check == 'Y':
                CS_View_Column_Names_Check(log_result, filename,directory, result_view_columns_order_in_input_and_output,result_view_columns_in_input_and_output,result_view_column_count)
                feature_dict["View_Column_Order"]=result_view_columns_order_in_input_and_output
                feature_dict["View_Column_Names"]=result_view_columns_in_input_and_output
                feature_dict["View_Column_Count(IP/OP)"]=result_view_column_count

            if fun_CS_View_Objects_Compare == 'Y':
                CS_View_Objects_Compare(log_result, filename,directory, result_from_and_join_in_view,result_from_and_join_count_in_view)
                feature_dict["FROM/JOIN_OBJECT_NAME_IN_VIEW"]=result_from_and_join_in_view
                feature_dict["FROM/JOIN_OBJECT_COUNT_IN_VIEW(IP/OP)"]=result_from_and_join_count_in_view

            if fun_CS_Join_Type_Count == 'Y':
                CS_Join_Type_Count(log_result, filename,directory,result_inner_join_count,result_cross_join_count,result_left_join_count,result_right_join_count)
                feature_dict["INNER_JOIN_COUNT(IP/OP)"]=result_inner_join_count
                feature_dict["CROSS_JOIN_COUNT(IP/OP)"]=result_cross_join_count
                feature_dict["LEFT_JOIN_COUNT(IP/OP)"]=result_left_join_count
                feature_dict["RIGHT_JOIN_COUNT(IP/OP)"]=result_right_join_count

            if fun_CS_Check_Union_And_Union_All == 'Y':
                CS_Check_Union_And_Union_All(log_result, filename,directory,result_union_and_union_all)
                feature_dict["UNION/UNION_ALL(IP/OP)"]=result_union_and_union_all

            if fun_CS_No_Of_Lines_In_File == 'Y':
                CS_No_Of_Lines_In_File(log_result, filename,directory,result_cs_file_line_count)
                feature_dict["FileLines_Count(IP/OP)"]=result_cs_file_line_count

            if fun_CS_Compare_Hardcoded_Values == 'Y':
                CS_Compare_Hardcoded_Values(log_result, filename,directory,result_cs_compare_hardcoded_value)
                feature_dict["Hardcoded_Values"]=result_cs_compare_hardcoded_value

            if fun_CS_Check_Date_Format == 'Y':
                CS_Check_Date_Format(log_result, filename,directory,result_cs_check_date_format)
                feature_dict["CHECK_DATE_FORMAT"]=result_cs_check_date_format

            if fun_CS_Check_Backtick_In_BTEQ == 'Y':
                CS_Check_Backtick_In_BTEQ(log_result, filename,directory,result_cs_check_backtick_in_BTEQ)
                feature_dict["'`' In BTEQ"]=result_cs_check_backtick_in_BTEQ

            if fun_CS_Count_Doller_Variable == 'Y':
                CS_Count_Doller_Variable(log_result, filename,directory,result_cs_count_doller_variable)
                feature_dict["$ Variable Count(IP/OP)"]=result_cs_count_doller_variable

            if fun_CS_Check_IF_FALSE_In_Output == 'Y':
                CS_Check_IF_FALSE_In_Output(log_result, filename,directory,result_cs_check_if_false_in_output)
                feature_dict["Check IF(false)"]=result_cs_check_if_false_in_output

            if fun_CS_Check_Lower_Upper_Trim_In_Where == 'Y':
                DB_Conection=create_connection(Enter_Project_Name)
                CS_Check_Lower_Upper_Trim_In_Where(log_result, filename,directory,result_cs_check_lower_upper_trim_in_where,DB_Conection)
                feature_dict["LOWER/UPPER/TRIM"]=result_cs_check_lower_upper_trim_in_where

            if fun_IF_Activity_Count == 'Y':
                IF_Activity(log_result,filename,directory,result_for_if_activity)
                feature_dict[".IF_ACTIVITY"]=result_for_if_activity

            if fun_For_Null_And_Not_Null == 'Y':
                null_and_not_null(log_result,filename,directory,result_for_null,result_for_not_null)
                feature_dict["IS_NULL"]=result_for_null
                feature_dict["IS_NOT_NULL"]=result_for_not_null

            if fun_For_ON_After_Join == 'Y':
                on_after_join(log_result,filename,directory,result_for_on_condition)
                feature_dict["ON_Condition"]=result_for_on_condition

            if fun_Check_Default_Value_For_TD == 'Y':
                Check_Default_Value_For_TD(log_result,filename,directory,result_for_default_value)
                feature_dict["DEFAULT_Value"]=result_for_default_value

            if fun_Check_Timezone == 'Y':
                Check_Timezone(log_result,filename,directory,result_Check_Timezone)
                feature_dict["Check_Timezone"]=result_Check_Timezone

            if fun_Primary_Index_To_Cluster_By_CSWG == 'Y':
                Primary_Index_To_Cluster_By_CSWG(log_result,filename,directory,result_Primary_Index_To_Cluster_By_CSWG)
                feature_dict["Primary_index_cswg"]=result_Primary_Index_To_Cluster_By_CSWG

            if fun_Joins_CSWG == 'Y':
                Joins_CSWG(log_result,filename,directory,result_Joins_CSWG)
                feature_dict["Joins_cswg"]=result_Joins_CSWG

            if fun_Check_No_Of_Statements_For_CSWG == 'Y':
                Check_No_Of_Statements_For_CSWG(log_result,filename,directory,result_no_of_statement_cswg)
                feature_dict["No_Of_Statements"]=result_no_of_statement_cswg

            if fun_Check_Table_And_View_In_Upper_Case == 'Y':
                Check_Table_And_View_In_Upper_Case(log_result,filename,directory,result_table_and_view_in_upper)
                feature_dict["Table_In_Upper"]=result_table_and_view_in_upper

            if fun_BQ_Validation_For_CSWG == 'Y':
                BQ_Validation_For_CSWG(log_result,filename,directory,result_bq_validation_cswg,BQ_JSON_PATH_CSWG,Parameter_JSON_File_Path_CSWG,all_other_table)
                feature_dict["Is_BQ_Run"]=result_bq_validation_cswg

            if fun_Check_auther_comment_wells_fargo == 'Y':
                Check_auther_comment_wells_fargo(log_result,filename,directory,result_auther_comment_wells_fargo)
                feature_dict["Author_comment"]=result_auther_comment_wells_fargo

            if fun_Check_Lower_For_Special_Characters == 'Y':
                Check_Lower_For_Special_Characters(log_result,filename,directory,result_lower_for_special_character)
                feature_dict["Lower_In_Special_Characters"]=result_lower_for_special_character

            if fun_Check_DATEDIFF_Function_In_Output_File == 'Y':
                Check_DATEDIFF_Function_In_Output_File(log_result,filename,directory,result_datediff_function)
                feature_dict["DATEDIFF()"]=result_datediff_function

            if fun_Check_Group_By_Columns_In_Insert_Statement == 'Y':
                Check_Group_By_Columns_In_Insert_Statement(log_result,filename,directory,result_group_by_columns)
                feature_dict["GROUP_BY_In_INSERT"]=result_group_by_columns

            if fun_check_cast_to_safe_cast == 'Y':
                check_cast_to_safe_cast(log_result, filename,directory, result_check_cast_to_safe_cast)
                feature_dict["cast_to_safecast"]=result_check_cast_to_safe_cast

            if fun_check_procedure_added_In_Output_File == 'Y':
                check_procedure_added_In_Output_File(log_result, filename,directory, result_check_procedure_added_In_Output_File)
                feature_dict["procedure_in_output_file"]=result_check_procedure_added_In_Output_File

            if fun_check_current_timestamp_to_current_datetime == 'Y':
                check_current_timestamp_to_current_datetime(log_result, filename,directory, result_check_current_timestamp_to_current_datetime)
                feature_dict["current_timestamp_to_current_datetime"]=result_check_current_timestamp_to_current_datetime

            if fun_TZ_Check == "Y":
                print("Timestamp validation started")
                dic_Tz=Timestamp_Check(log_result,filename,directory,inputFolderPath,TZ_result)
                dic_Tz=dic_Tz

        if Enable_For_Fload_Assurance == "Y":
            if fun_Check_Database_Values_Added_Or_NOT == 'Y':
                Check_Database_Values_Added_Or_NOT(log_result,filename,directory,fload_database_value_result,Schema_And_Value_Json)
                feature_dict["DatabaseValue"]=fload_database_value_result

            if fun_Check_Delimiter_Added_In_sed_Command == 'Y':
                Check_Delimiter_Added_In_sed_Command(log_result,filename,directory,fload_delimiter_in_sed_result)
                feature_dict["Delimeter_In_sed"]=fload_delimiter_in_sed_result

            if fun_Check_Drop_And_Create_Table == 'Y':
                Check_Drop_And_Create_Table(log_result,filename,directory,fload_drop_and_create_table_result)
                feature_dict["Drop/Create_Table"]=fload_drop_and_create_table_result

            if fun_Check_Error1_Table_Statement == 'Y':
                Check_Error1_Table_Statement(log_result,filename,directory,fload_error1_table_result)
                feature_dict["Error1_Table_Statement"]=fload_error1_table_result

            if fun_Check_Error2_Table_Statement == 'Y':
                Check_Error2_Table_Statement(log_result,filename,directory,fload_error2_table_result,Translate_Table_Folder_Path)
                feature_dict["Error2_Table_Statement"]=fload_error2_table_result

            if fun_Check_Partition_By_And_Order_By_In_INSERT == 'Y':
                Check_Partition_By_And_Order_By_In_INSERT(log_result,filename,directory,fload_partition_and_order_by_result,Translate_Table_Folder_Path)
                feature_dict["Partition/Order By In Insert"]=fload_partition_and_order_by_result



        #KSH_Assurance
        if Assurance_For_KSH == 'Y':
            if fun_trap_commented_or_not == 'Y':
                check_trap_comment(log_result,filename,directory,result_of_trap_comment)
                feature_dict["Trap_Comment"]=result_of_trap_comment

            if fun_bteq_commented_or_not == 'Y':
                check_bteq_comment(log_result,filename,directory,result_of_bteq_comment)
                feature_dict["bteq_Comment"]=result_of_bteq_comment

            if fun_error_check_added_or_not_in_file == 'Y':
                check_error_check_added_or_not_in_file(log_result,filename,directory,result_for_error_check_added_or_not_in_file)
                feature_dict["Error_Check_Added"]=result_for_error_check_added_or_not_in_file

            if fun_sed_added_in_function_call_or_not == 'Y':
                check_sed_added_in_function_call_or_not(log_result,filename,directory,result_for_sed_added_in_function_call_or_not)
                feature_dict["sed_In_Function_Call"]=result_for_sed_added_in_function_call_or_not

            if fun_create_table_added_after_os_and_export == 'Y':
                check_create_table_added_after_os_and_export(log_result,filename,directory,result_for_create_table_added_after_os_and_export)
                feature_dict["Create_Table_After_OS_EXPORT"]=result_for_create_table_added_after_os_and_export

            if fun_email_logic == 'Y':
                check_email_logic(log_result,filename,directory,result_for_email_logic)
                feature_dict["Email_Logic"]=result_for_email_logic

            if fun_f_get_aedw_open_batches == 'Y':
                check_f_get_aedw_open_batches(log_result,filename,directory,result_for_f_get_aedw_open_batches)
                feature_dict["f_get_aedw_open_batches"]=result_for_f_get_aedw_open_batches

            if fun_f_execute_bteq_calling == 'Y':
                check_f_execute_bteq_calling(log_result,filename,directory,result_for_f_execute_bteq_calling)
                feature_dict["f_execute_bteq_calling"]=result_for_f_execute_bteq_calling

            # if fun_fn_sql == 'Y':
            #     check_fn_sql(log_result,filename,directory,result_for_fn_sql)
            #     feature_dict["fn_sql"]=result_for_fn_sql

            # if fun_Seq_MstSeq == 'Y':
            #     check_Seq_MstSeq(log_result,filename,directory,result_for_Seq_MstSeq)
            #     feature_dict["Seq_MstSeq"]=result_for_Seq_MstSeq

            if fun_comment_for_usrid_pwd_tdpid_logon == 'Y':
                check_comment_for_usrid_pwd_tdpid_logon(log_result,filename,directory,result_for_comment_for_usrid_pwd_tdpid_logon)
                feature_dict["comment_for_usrid_pwd_tdpid_logon"]=result_for_comment_for_usrid_pwd_tdpid_logon

            # if fun_cheak_function_call == 'Y':
            #     cheak_fun_cheak_function_call(log_result,filename,directory,result_cheak_function_call)
            #     feature_dict["cheak_function_call"]=result_cheak_function_call

            if fun_check_comment_present_in_echo == 'Y':
                check_comment_present_in_echo(log_result,filename,directory,result_check_echo_comment)
                feature_dict["echo_#_comment"]=result_check_echo_comment

            
        if Assurance_For_KSH_Sadiaa == 'Y':
            if fun_execute_query_before_sql == 'Y':
                check_execute_query_before_sql(log_result,filename,directory,result_execute_query_before_sql)
                feature_dict["Execute_Query"]=result_execute_query_before_sql

            if fun_check_bteq_logon_database == 'Y':
                check_bteq_logon_database(log_result,filename,directory,result_check_bteq_logon_database)
                feature_dict["Bteq_Logon_Database"]=result_check_bteq_logon_database

            if fun_Check_auther_comment_sadiaa == 'Y':
                Check_auther_comment_sadiaa(log_result,filename,directory,result_Check_auther_comment_sadiaa)
                feature_dict["Author Comment Sadiaa"]=result_Check_auther_comment_sadiaa

        #Netezza To BQ
        if check_NZ_to_BQ == "Y":
            if fun_project_id_for_NZ == "Y":
                check_project_id_for_NZ(log_result,filename,directory,project_id_result_for_NZ)
                feature_dict["Project ID"]=project_id_result_for_NZ

            if fun_datatype_for_NZ == "Y":
                check_datatype_for_NZ(log_result,filename,directory,Datatype_result_for_NZ)
                feature_dict["Datatype"]=Datatype_result_for_NZ

            if fun_cluster_and_partition_for_NZ == "Y":
                check_cluster_and_partition_for_NZ(log_result,filename,directory,Cluster_and_partition_result_for_NZ)
                feature_dict["Cluster and Partition"]=Cluster_and_partition_result_for_NZ

            if fun_no_of_statements_for_NZ == "Y":
                no_of_statements_for_NZ(log_result,filename,directory,no_of_statements_result_for_NZ,fun_project_id_for_NZ)
                feature_dict["No_Of_Statements"]=no_of_statements_result_for_NZ

            if fun_check_comments == "Y":
                check_comments_for_NZ(log_result,filename,directory,Check_comment_result_for_NZ)
                feature_dict["Comments"]=Check_comment_result_for_NZ

            if fun_cross_join_for_NZ == "Y":
                check_cross_join_for_NZ(log_result,filename,directory,cross_join_result_for_NZ,fun_project_id_for_NZ)
                feature_dict["Joins"]=cross_join_result_for_NZ

            if fun_not_like_for_NZ == "Y":
                check_not_like_for_NZ(log_result,filename,directory,not_like_result_for_NZ)
                feature_dict["Not_Like"]=not_like_result_for_NZ

            if fun_now_for_NZ == "Y":
                check_now_for_NZ(log_result,filename,directory,now_result_for_NZ)
                feature_dict["Now"]=now_result_for_NZ

            if fun_check_default_schema_for_NZ == "Y":
                check_default_schema_for_NZ(log_result,filename,directory,default_schema_result_for_NZ)
                feature_dict["Default_Schema"]=default_schema_result_for_NZ

            if fun_Case_when_for_NZ == "Y":
                check_Case_when_for_NZ(log_result,filename,directory,Case_when_result_for_NZ)
                feature_dict["Case_When"]=Case_when_result_for_NZ

            if fun_Table_In_Case_for_NZ == "Y":
                check_lower_case_for_NZ(log_result,filename,directory,lower_case_result_for_NZ,fun_project_id_for_NZ)
                feature_dict["Lower_Case"]=lower_case_result_for_NZ

            if fun_Word_After_As_In_Lower_for_NZ == "Y":
                check_alice_in_lower_case_for_NZ(log_result,filename,directory,alice_in_lower_case_for_NZ)
                feature_dict["Alise_In_Lower"]=alice_in_lower_case_for_NZ

            if fun_Check_BQ_ProjectID_For_Virgin_Media == 'Y':
                Check_BQ_Project_For_Virgin_Media(log_result,filename,directory,result_Check_BQ_ProjectID_For_Virgin_Media)
                feature_dict["BQ_Project_Id"]=result_Check_BQ_ProjectID_For_Virgin_Media

            if fun_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION == "Y":
                Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION(log_result,filename,directory,result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION)
                feature_dict["BEGIN_And_Commit"]=result_Check_BEGIN_TRANSACTION_And_Commit_TRANSACTION


        #MemSQL To BQ
        if Check_Features_For_MemSQL_To_BQ == "Y":
            if fun_Check_No_Of_Statements_For_MemSQL == "Y":
                Check_No_Of_Statements_For_MemSQL(log_result,filename,directory,result_check_no_of_statement_for_memsql)
                feature_dict["No_Of_Statements"]=result_check_no_of_statement_for_memsql

            if fun_Check_Count_Of_Case_Statement == "Y":
                Check_Count_Of_Case_Statement(log_result,filename,directory,result_count_of_case_statement_for_memsql)
                feature_dict["CASE_Statements(IP/OP)"]=result_count_of_case_statement_for_memsql

            if fun_Check_Count_Of_Doller_Parameters == "Y":
                Check_Count_Of_Doller_Parameters(log_result,filename,directory,result_count_of_doller_parameter_for_memsql)
                feature_dict["Doller_Parameters(IP/OP)"]=result_count_of_doller_parameter_for_memsql

            if fun_Check_Count_Of_Coalesce_Function == "Y":
                Check_Count_Of_Coalesce_Function(log_result,filename,directory,resut_count_of_coalesce_fun_for_memsql)
                feature_dict["COALESCE(IP/OP)"]=resut_count_of_coalesce_fun_for_memsql


        #Hive To BQ
        if check_HV_to_BQ == 'Y':
            if Enable_Verizon == 'Y':
                if fun_datatype_for_HV == "Y":
                    check_datatype_for_HV(log_result,filename,directory,Datatype_result_for_HV,charles_timestamp_to_datetime)
                    feature_dict["DataTypes"]=Datatype_result_for_HV

                if Flag_comment_to_option_for_HV == "Y":
                    check_comment_to_option_for_HV(log_result,filename,directory,comment_to_option_result_for_HV)
                    feature_dict["Comment To Option"]=comment_to_option_result_for_HV

                if Flag_drop_raw_format_or_location_from_HV == 'Y':
                    check_drop_raw_format_or_location_for_HV(log_result,filename,directory,drop_row_format_or_location_result_for_HV)
                    feature_dict["Raw Format Drop"]=drop_row_format_or_location_result_for_HV

                if Flag_create_table_external_for_HV == "Y":
                    check_create_table_external_for_HV(log_result,filename,directory,create_table_external_for_HV)
                    feature_dict["Create External Table"]=create_table_external_for_HV

                if Flag_No_of_statements_in_Script_for_HV == "Y":
                    No_Of_Statement_In_Script_Hive(log_result,filename,directory,Result_No_Of_Statement,fun_project_ID_Hive)
                    feature_dict["Flag_No_of_statements_in_Script_for_HV"]=Result_No_Of_Statement

                if Flag_cross_join_for_HV == "Y":
                    check_cross_join_hive(log_result,filename,directory,JoinResult)
                    feature_dict["Flag_cross_join_for_HV"]=JoinResult

                if fun_project_ID_Hive == "Y":
                    PROJECT_ID_Hive(log_result,project_id_check_for_bq,filename)
                    feature_dict["project_id_check_for_bq"]=project_id_check_for_bq

                if Flag_check_colan_to_space == "Y":
                    Check_colan_to_space(log_result,filename,directory,Result_check_colan_to_space)
                    feature_dict["Flag_check_colan_to_space"]=Result_check_colan_to_space

                if Flag_check_map_to_array == "Y":
                    Check_map_to_array(log_result,filename,directory,Result_check_map_to_array)
                    feature_dict["Flag_check_map_to_array"]=Result_check_map_to_array

                if Flag_Check_par_col_datatype_change == "Y":
                    # print(par_col_datatype_sheet_path)
                    df = pd.read_excel(par_col_datatype_sheet_path, engine='openpyxl')

                    table_column_list = []
                    for i in range(0,len(df)):
                        ele = df.iloc[i]
                        table_name = str(ele[0]).strip().lower()
                        column_name = str(ele[1]).strip().lower()
                        change_dt = str(ele[2]).strip().upper()
                        table_column_list.append(table_name+':'+column_name+':'+change_dt)

                    fun_Check_par_col_datatype_change(log_result,filename,directory,result_check_par_col_datatype_change,table_column_list)
                    feature_dict["Par_col_datatype_change"]=result_check_par_col_datatype_change

                if Flag_Check_add_option_change == "Y":
                    df = pd.read_excel(add_option_sheet_path, engine='openpyxl')

                    table_option_list = []
                    for i in range(0,len(df)):
                        ele = df.iloc[i]
                        table_name = str(ele[0]).strip().lower()
                        option_name = str(ele[1]).strip().lower()
                        table_option_list.append(table_name+':'+option_name)

                    fun_Check_add_option_change(log_result,filename,directory,result_Check_add_option_change,table_option_list)
                    feature_dict["Add_option_change"]=result_Check_add_option_change

                if Flag_Check_PartitionBy_to_ClusterBy == "Y":
                    Check_PartitionBy_to_ClusterBy(log_result,filename,directory,result_Check_PartitionBy_to_ClusterBy)
                    feature_dict["Check PartitionBy to ClusterBy"]=result_Check_PartitionBy_to_ClusterBy

                if Flag_Check_clusterBy_create_cols == "Y":
                    Check_clusterBy_create_cols(log_result,filename,directory,result_Check_clusterBy_create_cols)
                    feature_dict["ClusterBy Create Columns"]=result_Check_clusterBy_create_cols

                if Flag_Check_Partitioned_BY_to_Partition_By == "Y":
                    Check_Partitioned_BY_to_Partition_By(log_result,filename,directory,result_Check_Partitioned_BY_to_Partition_By)
                    feature_dict["Flag_Check_Partitioned_BY_to_Partition_By"]=result_Check_Partitioned_BY_to_Partition_By

                if Flag_check_for_current_functions == "Y":
                    check_for_current_functions_CS(log_result,filename,directory,result_check_for_current_functions)
                    feature_dict["check for current functions"]=result_check_for_current_functions

                if Flag_schema_table_case == "Y":
                    Check_schema_table_case(log_result,filename,directory,result_schema_table_case)
                    feature_dict["Flag_schema_table_case"]=result_schema_table_case

                if Flag_column_and_its_alias_check == "Y":
                    Check_column_and_its_alias_check(log_result,filename,directory,result_column_and_its_alias_check)
                    feature_dict["Flag_column_and_its_alias_check"]=result_column_and_its_alias_check

                if Flag_From_Unixtime == "Y":
                    Check_From_Unixtime(log_result,filename,directory,result_From_Unixtime)
                    feature_dict["Flag_Select_From_Timestamp"]=result_From_Unixtime

                if Flag_NVL_Columns == "Y":
                    Check_NVL_Columns(log_result,filename,directory,result_NVL_Columns)
                    feature_dict["Flag_NVL_Columns"]=result_NVL_Columns

                if Flag_DATE_SUB_Columns == "Y":
                    Check_DATE_SUB_Columns(log_result,filename,directory,result_DATE_SUB_Columns)
                    feature_dict["Flag_DATE_SUB_Columns"]=result_DATE_SUB_Columns

                if Flag_To_Date_Columns == "Y":
                    Check_To_Date_Columns(log_result,filename,directory,result_To_Date_Columns)
                    feature_dict["Flag_To_Date_Columns"]=result_To_Date_Columns

                if Flag_Date_Add_Columns == "Y":
                    Check_Date_Add_Columns(log_result,filename,directory,result_Date_Add_Columns)
                    feature_dict["Flag_Date_Add_Columns"]=result_Date_Add_Columns

                if Flag_Date_Diff_Columns == "Y":
                    Check_Date_Diff_Columns(log_result,filename,directory,result_Date_Diff_Columns)
                    feature_dict["Flag_Date_Diff_Columns"]=result_Date_Diff_Columns

            if Enable_Flipkart == 'Y':
                if fkt_check_native_external_table == "Y":
                    text_obj = common_obj.Read_file(Native_table_text_path)
                    native_list = text_obj.lower().split('\n')
                    fun_fkt_check_native_external_table(log_result,filename,directory,result_native_external_table,native_list)
                    feature_dict["Native_External_Table"]=result_native_external_table

                if fkt_fun_check_external_table_path_for_HV == "Y":
                    fkt_check_external_table_path_for_HV(log_result,filename,directory,external_table_path_result_for_hv,external_table_path_dict)
                    feature_dict["Check_External_Table_Path"]=external_table_path_result_for_hv

                if fkt_Flag_Check_Partitioned_BY_to_Partition_By == "Y":
                    text_obj = common_obj.Read_file(Native_table_text_path)
                    native_list = text_obj.lower().split('\n')
                    fkt_Check_Partitioned_BY_to_Partition_By(log_result,filename,directory,result_Check_Partitioned_BY_to_Partition_By,native_list)
                    feature_dict["Partitioned_BY_to_Partition_By"]=result_Check_Partitioned_BY_to_Partition_By

                if fkt_Flag_Check_PartitionBy_to_ClusterBy == "Y":
                    text_obj = common_obj.Read_file(Native_table_text_path)
                    native_list = text_obj.lower().split('\n')
                    fkt_Check_PartitionBy_to_ClusterBy(log_result,filename,directory,result_Check_PartitionBy_to_ClusterBy,native_list)
                    feature_dict["PartitionBy_to_ClusterBy"]=result_Check_PartitionBy_to_ClusterBy

                if fkt_flag_check_datetime_trunc_Granularity == "Y":
                    df = pd.read_excel(Partition_Cluster_Change_Datatype_Path)
                    fkt_check_datetime_trunc_Granularity(log_result,filename,directory,result_Check_datetime_trunc_Granularity,df)
                    feature_dict["Datetime_trunc_Granularity"]=result_Check_datetime_trunc_Granularity

                if fkt_fun_datatype_for_HV == "Y":
                    fkt_check_datatype_for_HV(log_result,filename,directory,Datatype_result_for_HV,fkt_check_timestamp_to_datetime)
                    feature_dict["Check_Datatype"]=Datatype_result_for_HV

                if fkt_flag_check_struct_datatype == "Y":
                    fkt_check_struct_datatype(log_result,filename,directory,result_struct_datatype)
                    feature_dict["check_struct_datatype"]=result_struct_datatype
                
                if fkt_Flag_create_table_external_for_HV == "Y":
                    fkt_check_create_table_external_for_HV(log_result,filename,directory,create_table_external_for_HV)
                    feature_dict["Check_create_table_if_not_exists"]=create_table_external_for_HV

                if fkt_Flag_No_of_statements_in_Script_for_HV == "Y":
                    fkt_No_Of_Statement_In_Script(log_result,filename,directory,Result_No_Of_Statement,fkt_fun_project_ID)
                    feature_dict["Check_No_of_statements"]=Result_No_Of_Statement

                if fkt_Flag_cross_join_for_HV == "Y":
                    fkt_check_cross_join(log_result,filename,directory,JoinResult)
                    feature_dict["Check_cross_join"]=JoinResult

                if fkt_Flag_check_colan_to_space == "Y":
                    fkt_Check_colan_to_space(log_result,filename,directory,Result_check_colan_to_space)
                    feature_dict["Struct_colan_to_space"]=Result_check_colan_to_space

                if fkt_check_keyword_columns == "Y":
                    fun_fkt_check_keyword_columns(log_result,filename,directory,result_keyword_columns)
                    feature_dict["Backtick_for_keyword_columns"]=result_keyword_columns

                if fkt_Flag_comment_to_option_for_HV == "Y":
                    fkt_check_comment_to_option_for_HV(log_result,filename,directory,comment_to_option_result_for_HV)
                    feature_dict["Comment_to_Options"]=comment_to_option_result_for_HV

                if fkt_Flag_drop_raw_format_or_location_from_HV == 'Y':
                    fkt_check_drop_raw_format_or_location_for_HV(log_result,filename,directory,drop_row_format_or_location_result_for_HV)
                    feature_dict["Drop_raw_format"]=drop_row_format_or_location_result_for_HV

                if fkt_Flag_check_map_to_array == "Y":
                    fkt_Check_map_to_array(log_result,filename,directory,Result_check_map_to_array)
                    feature_dict["Check_map_to_array"]=Result_check_map_to_array

                if fkt_check_d_added_for_number_in_column == "Y":
                    fun_fkt_check_d_added_for_number_in_column(log_result,filename,directory,result_d_added_for_number_in_column)
                    feature_dict["d_for_numbered_column"]=result_d_added_for_number_in_column

                if fkt_check_partition_col_datatype == "Y":
                    df = pd.read_excel(Partition_Cluster_Change_Datatype_Path)
                    fun_fkt_check_partition_col_datatype(log_result,filename,directory,result_partition_col_datatype,df)
                    feature_dict["Check_partition_col_datatype"]=result_partition_col_datatype

                if fkt_Flag_partition_timestamp_to_date_function == "Y":
                    fkt_partition_timestamp_to_date_function(log_result,filename,directory,result_partition_timestamp_to_date_function)
                    feature_dict["Timestamp Partition"]=result_partition_timestamp_to_date_function

                if fkt_Flag_match_table_view_count == "Y":
                    fkt_match_table_view_count(log_result,filename,directory,result_match_table_view_count)
                    feature_dict["Match Table View Count"]=result_match_table_view_count

                if fkt_Match_join_count == "Y":
                    fun_fkt_Match_join_count(log_result,filename,directory,result_Match_join_count)
                    feature_dict["Match Join Count"]=result_Match_join_count

                if fkt_ins_sol_for_without_partition_tbl == "Y":
                    fun_fkt_ins_sol_for_without_partition_tbl(log_result,filename,directory,result_ins_sol_for_without_partition_tbl)
                    feature_dict["Ins_sol_for_without_partition_tbl"]=result_ins_sol_for_without_partition_tbl

                if fkt_del_ins_sol_for_partition_tbl == "Y":
                    fun_fkt_del_ins_sol_for_partition_tbl(log_result,filename,directory,result_del_ins_sol_for_partition_tbl)
                    feature_dict["Del_ins_sol_for_partition_tbl"]=result_del_ins_sol_for_partition_tbl

                if fkt_fun_project_ID == "Y":
                    fkt_PROJECT_ID(log_result,project_id_check_for_bq,filename)
                    feature_dict["Check_project_id"]=project_id_check_for_bq

                if fkt_Flag_Append_project_ID_to_udf_function == "Y":
                    fkt_Append_project_ID_to_udf_function(log_result,filename,directory,result_Append_project_ID_to_udf_function)
                    feature_dict["Check_project_ID_to_udf_function"]=result_Append_project_ID_to_udf_function

                if fkt_flag_check_safe_with_function =="Y":
                    fun_fkt_check_safe_with_function(log_result,filename,directory,result_safe_with_function)
                    feature_dict["Check_Safe_with_function"]=result_safe_with_function

                if fkt_flag_check_lateral_view =="Y":
                    fun_fkt_check_lateral_view(log_result,filename,directory,result_lateral_view)
                    feature_dict["Check_Lateral_View"]=result_lateral_view

                if fkt_Flag_match_schema_table_case == "Y":
                    fkt_match_schema_table_case(log_result,filename,directory,result_match_schema_table_case)
                    feature_dict["Match_Schema_Table_Case"]=result_match_schema_table_case

                if fkt_Flag_check_table_alias == "Y":
                    fkt_Check_table_alias_column(log_result,filename,directory,result_check_table_alias)
                    feature_dict["Check_Table_Alias"]=result_check_table_alias

                if fkt_check_schema_in_seriliser_deser == "Y":
                    fun_fkt_check_schema_in_seriliser_deser(log_result,filename,directory,result_check_schema_in_seriliser_deser)
                    feature_dict["Check_Schema_In_Seriliser_Deser"]=result_check_schema_in_seriliser_deser

                if fkt_check_comparison_par_col_using_sheet == "Y":
                    df = pd.read_excel(Partition_Cluster_Change_Datatype_Path, engine='openpyxl')

                    table_column_dict = []
                    for i in range(0,len(df)):
                        ele = df.iloc[i]
                        table_name = str(ele[0]).strip().lower()
                        column_name = str(ele[1]).strip().lower()
                        hive_dt = str(ele[2]).strip().upper()
                        bq_dt = str(ele[4]).strip().upper()
                        if 'INTEGER' == hive_dt and 'DATETIME' == bq_dt:
                            table_column_dict.append(table_name+'::'+column_name+'::'+hive_dt+'::'+bq_dt)

                    fun_fkt_check_comparison_par_col_using_sheet(log_result,filename,directory,result_check_comparison_par_col_using_sheet,table_column_dict)
                    feature_dict["Check_comparison_par_col_using_sheet"]=result_check_comparison_par_col_using_sheet
                
                if fkt_check_nullif_column == "Y":
                    fun_fkt_check_nullif_column(log_result,filename,directory,result_check_nullif_column)
                    feature_dict["check_nullif_column"]=result_check_nullif_column

                if fkt_check_limit_count == "Y":
                    fun_fkt_check_limit_count(log_result,filename,directory,result_check_limit_count)
                    feature_dict["Match limit count"]=result_check_limit_count
   
                if fkt_check_parse_date_to_case_when == "Y":
                    fun_fkt_check_parse_date_to_case_when(log_result,filename,directory,result_check_parse_date_to_case_when)
                    feature_dict["Parse_date to case when"]=result_check_parse_date_to_case_when

                if fkt_match_cte_table_count == "Y":
                    fun_fkt_match_cte_table_count(log_result,filename,directory,result_fkt_match_cte_table_count)
                    feature_dict["Match CTE table"]=result_fkt_match_cte_table_count

                if fkt_match_case_when_count == "Y":
                    fun_fkt_match_case_when_count(log_result,filename,directory,result_fkt_match_case_when_count)
                    feature_dict["Match Case When"]=result_fkt_match_case_when_count

                if fkt_check_group_by_in_file == "Y":
                    fun_fkt_check_group_by_in_file(log_result,filename,directory,result_fkt_check_group_by_in_file)
                    feature_dict["Group By Columns"]=result_fkt_check_group_by_in_file

                if fkt_check_truncate_with_begin_transaction == "Y":
                    fun_fkt_check_truncate_with_begin_transaction(log_result,filename,directory,result_fkt_check_truncate_with_begin_transaction)
                    feature_dict["Trancate with transaction"]=result_fkt_check_truncate_with_begin_transaction

                if fkt_check_insert_column_names == "Y":
                    fun_fkt_check_insert_column_names(log_result,filename,directory,result_check_insert_column_names)
                    feature_dict["check_insert_column_names"]=result_check_insert_column_names

                if fkt_match_aggregate_func_count == "Y":
                    fun_fkt_match_aggregate_func_count(log_result,filename,directory,result_match_aggregate_func_count)
                    feature_dict["Aggregate_func_count"]=result_match_aggregate_func_count

                if fkt_match_clauses_count == "Y":
                    fun_fkt_match_clauses_count(log_result,filename,directory,result_match_clauses_count)
                    feature_dict["Aggregate_func_count"]=result_match_clauses_count

                if fkt_check_date_sub_func == "Y":
                    fun_fkt_check_date_sub_func(log_result,filename,directory,result_check_date_sub_func)
                    feature_dict["Check_date_sub_func"]=result_check_date_sub_func

                if fkt_Flag_check_cast_count == "Y":
                    fkt_check_cast_count(log_result,filename,directory,result_check_cast_count)
                    feature_dict["Check_cast_count"]=result_check_cast_count

                if fkt_check_hash_parameter == "Y":
                    fun_fkt_check_hash_parameter(log_result,filename,directory,result_check_hash_parameter,hash_parameter_csv_path)
                    feature_dict["check_hash_parameter"]=result_check_hash_parameter

                if fkt_check_timezone == "Y":
                    fun_fkt_check_timezone(log_result,filename,directory,result_check_timezone)
                    feature_dict["check_timezone"]=result_check_timezone

                if fkt_check_safe_cast == "Y":
                    fun_fkt_check_safe_cast(log_result,filename,directory,result_check_safe_cast)
                    feature_dict["check_safe_cast"]=result_check_safe_cast

        #Snowflake To BQ
        if Check_features_of_SF_to_BQ == 'Y':
            if sf_match_schema_table_case == "Y":
                fun_sf_match_schema_table_case(log_result,filename,directory,result_sf_match_schema_table_case)
                feature_dict["Match_schema_table_case"]=result_sf_match_schema_table_case

            if sf_project_id == "Y":
                fun_sf_project_id(log_result,filename,directory,result_sf_project_id)
                feature_dict["Check_project_id"]=result_sf_project_id

            if sf_check_datatype == "Y":
                fun_sf_check_datatype(log_result,filename,directory,result_sf_check_datatype)
                feature_dict["Check Datatype"]=result_sf_check_datatype

            if sf_check_no_of_statements_in_script == "Y":
                fun_sf_check_no_of_statements_in_script(log_result,filename,directory,result_sf_check_no_of_statements_in_script)
                feature_dict["Check_no_of_statements_in_script"]=result_sf_check_no_of_statements_in_script

            if sf_check_join_count == "Y":
                fun_sf_check_join_count(log_result,filename,directory,result_sf_check_join_count)
                feature_dict["Match Join Count"]=result_sf_check_join_count

            if sf_check_TRY_TO_functions == "Y":
                fun_sf_check_TRY_TO_functions(log_result,filename,directory,result_sf_check_TRY_TO_functions)
                feature_dict["TRY_TO_Functions"]=result_sf_check_TRY_TO_functions

        Log_Files(log_result,final_result,feature_dict)

    if __name__== '__main__':
        run()

# print(DateTime())
feature_dict["Final Delivery Status"]=final_result
Excel_Write(file_name,feature_dict,Project_Name_For_Assurance_Report)
if fun_TZ_Check=="Y":
    if len(dic_Tz):
        try:
            print("Creating EXCEL")
            Excel_Write_TZ(dic_Tz,Project_Name_For_Assurance_Report)
        except Exception as e:
            print(f"An error occurred: {e}")




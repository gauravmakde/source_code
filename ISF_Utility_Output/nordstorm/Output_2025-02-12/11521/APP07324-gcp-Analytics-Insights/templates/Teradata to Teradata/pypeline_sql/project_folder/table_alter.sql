SET QUERY_BAND = 'App_ID=your_app_id;
     DAG_ID=your_dag_name_11521_ACE_ENG;
     Task_Name=your_sql_file_name_without_extension;'
     FOR SESSION VOLATILE;

---- Rename existing columns
ALTER TABLE {your_t2_schema}.{your_t2_table_name} RENAME  column_1 TO column_1_new;
ALTER TABLE {your_t2_schema}.{your_t2_table_name} RENAME  column_2 TO column_2_new;
ALTER TABLE {your_t2_schema}.{your_t2_table_name} RENAME  column_3 TO column_3_new;

----- Add new columns
ALTER TABLE {your_t2_schema}.{your_t2_table_name} 
ADD column_1_new integer compress, 
ADD column_2_new varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress,
ADD column_3_new char(10) CHARACTER SET UNICODE NOT CASESPECIFIC compress,
ADD column_4_new numeric(12,2) compress,
ADD column_5_new date,
ADD dw_sys_load_tmstp timestamp(6) default current_timestamp(6) not null;

----- Add primary key
ALTER TABLE {your_t2_schema}.{your_t2_table_name} 
ADD PRIMARY KEY (column_name);

---- Drop existing columns
ALTER TABLE {your_t2_schema}.{your_t2_table_name} 
DROP column_1,
DROP column_2;


SET QUERY_BAND = NONE FOR SESSION;
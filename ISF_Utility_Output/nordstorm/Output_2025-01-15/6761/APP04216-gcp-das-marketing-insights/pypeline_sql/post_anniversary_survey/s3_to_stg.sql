-- Read s3 csv source data
create temporary view real_survey_tbl as
select
	ResponseID as response_id,
	Email as email
from s3_read_survey_tbl;

--Sink
---Writing Data to Teradata using SQL API CODE
insert overwrite table write_teradata_survey_tbl select * from real_survey_tbl;
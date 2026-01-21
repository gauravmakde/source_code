-- Read s3 csv source data
create temporary view real_marketed_user_tbl as
select
	MobileNumber    as mobile_number, 
	Email	        as email,
	acpid           as acp_id, 	
	loyaltyid       as loyalty_id, 	
	uniquesourceid  as unique_source_id
from s3_read_marketed_user_tbl;

--Sink
---Writing Data to Teradata using SQL API CODE
insert overwrite table write_teradata_marketed_user_tbl select * from real_marketed_user_tbl;
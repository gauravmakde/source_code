-- Read s3 csv source data
create temporary view real_rsvp_tbl as
select
	EventName as event_name,
	EventLocation as event_location,
	EventDateTime as event_date,
	TransactionIDOrderNumber as transaction_id,
	FirstName as first_name,
	LastName as last_name,
	Email as email,
	Mobile as mobile,
	Howdidyouhear as how_did_you_hear,
	CASE WHEN Attended = 'NULL' THEN '' ELSE Attended END AS attended
from s3_read_rsvp_tbl;

--Sink
---Writing Data to Teradata using SQL API CODE
insert overwrite table write_teradata_rsvp_tbl select * from real_rsvp_tbl;
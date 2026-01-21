-- Read s3 csv source data
create temporary view real_icon_lounge_tbl as
select
	Date          as visit_date,
	LoyaltyStatus as loyalty_status,
	FirstName     as first_name,	  
	LastName	  as last_name,
	MobileNumber  as mobile_number, 
	Email	      as email,
	PurchasedPass as purchased_pass, 	
	VistingCity   as visiting_city, 	
	VistingState  as visiting_state, 	
	Guests        as guests
from s3_read_icon_lounge_tbl;

--Sink
---Writing Data to Teradata using SQL API CODE
insert overwrite table write_teradata_icon_lounge_tbl select * from real_icon_lounge_tbl;
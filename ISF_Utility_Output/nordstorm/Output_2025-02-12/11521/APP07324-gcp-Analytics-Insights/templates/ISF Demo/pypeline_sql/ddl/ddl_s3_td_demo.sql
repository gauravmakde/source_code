/*==============
 Teradata Semantic Layer Naming Standards 
https://gitlab.nordstrom.com/data-standards/data-standards/blob/main/docs/teradata-sl-naming-standard.md

Apply Teradata Basics 
https://confluence.nordstrom.com/display/DAS1/Teradata+Basics+Training?src=contextnavpagetreemode

Include:
- Approved abbreviation standard
- ETL Audit Columns
=================*/
-- comment the below line off if creating table for the first time! 
-- DROP TABLE T2DL_DAS_BIE_DEV.FUNNEL_TEST_TBL; 
CREATE MULTISET TABLE {t2_schema}.FUNNEL_TEST_TBL,
FALLBACK,
NO BEFORE JOURNAL,
NO AFTER JOURNAL,
CHECKSUM = DEFAULT,
DEFAULT MERGEBLOCKRATIO,
MAP = TD_MAP1
(
order_number varchar(32) compress,
user_id_type varchar(24) compress,
user_id varchar(36),
shopper_id varchar(36) compress,
country CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('US','CA'),
channel VARCHAR(24) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('FULL_LINE','RACK', 'UNKNOWN'),
platform CHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS('IOS','WEB','MOW'),
pst_date DATE FORMAT 'YYYY-MM-DD'
)
PRIMARY INDEX (user_id);


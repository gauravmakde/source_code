-------- DDL table creation ------- Run only for table creation/recreation
/*==============
 Teradata Semantic Layer Naming Standards 
https://gitlab.nordstrom.com/data-standards/data-standards/blob/main/docs/teradata-sl-naming-standard.md

Apply Teradata Basics 
https://confluence.nordstrom.com/display/DAS1/Teradata+Basics+Training?src=contextnavpagetreemode

Include:
- Approved abbreviation standard
- ETL Audit Columns
=================*/

--- drop table T2DL_DAS_BIE_DEV.test_return_isf_stg2; --- remove this if creating staging table for the first time

CREATE MULTISET TABLE T2DL_DAS_BIE_DEV.test_return_isf_stg2,
FALLBACK,
NO BEFORE JOURNAL,
NO AFTER JOURNAL,
CHECKSUM = DEFAULT,
DEFAULT MERGEBLOCKRATIO,
MAP = TD_MAP1
(
acp_id VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC,
channel VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
shipped_sales DECIMAL(38,2),
shipped_qty INTEGER,
return_amt DECIMAL(38,2),
return_qty INTEGER,
s3_year INTEGER,
s3_month INTEGER,
s3_day INTEGER
)
PRIMARY INDEX (acp_id);



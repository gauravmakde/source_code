-------- DDL SQL ------- 
/*==============
 Teradata Semantic Layer Naming Standards 
https://gitlab.nordstrom.com/data-standards/data-standards/blob/main/docs/teradata-sl-naming-standard.md

Apply Teradata Basics 
https://confluence.nordstrom.com/display/DAS1/Teradata+Basics+Training?src=contextnavpagetreemode

Include:
- Approved abbreviation standard
- ETL Audit Columns
=================*/

--- drop table T2DL_DAS_BIE_DEV.test_return_isf_stg; --- Run only for table's recreation

--- Note: Command out the table creation part for pipeline re-run since table may have created or already exists
--- Table creation part
create multiset table T2DL_DAS_BIE_DEV.test_return_isf_stg, fallback, --- staging table 
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
        acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
        channel VARCHAR(9) CHARACTER SET UNICODE NOT CASESPECIFIC,
        shipped_sales DECIMAL(38,2),
        shipped_qty INTEGER,
        return_amt DECIMAL(38,2),
        return_qty INTEGER,
        update_timestamp TIMESTAMP(6) WITH TIME ZONE)
    PRIMARY INDEX (acp_id);

-------- end of table creation part



CREATE TEMPORARY VIEW s3_orc_view AS SELECT * FROM s3_orc_tbl;

INSERT overwrite s3_merged_data SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY loyaltynoteid ORDER BY objectmodelcreationtime DESC) AS rn FROM (
        SELECT * FROM s3_orc_view
            UNION ALL
        SELECT   * FROM {schema_name}.{resulting_lmo_table_name})
        AS uninon) 
    AS tbl
WHERE tbl.rn=1 and 1=1;


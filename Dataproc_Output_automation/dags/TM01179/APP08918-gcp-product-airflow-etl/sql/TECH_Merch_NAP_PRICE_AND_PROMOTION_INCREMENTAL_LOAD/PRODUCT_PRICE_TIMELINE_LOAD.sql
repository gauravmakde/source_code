DECLARE out1 STRING;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_PROC.PRODUCT_PRICE_TIMELINE_INCREMENTAL_LOAD('H',out1);
Declare out_return_msg_drvr string;


CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_PROC.MERCH_JWN_SALE_SKU_STORE_DAY_AGG_WRK_LOAD_DRIVER_SP(out_return_msg_drvr);


begin 
 declare out_return_msg_drvr string default 'out_return_msg_drvr';

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_PROC.MERCH_INVENTORY_SKU_STORE_DAY_AGG_WRK_LOAD_DRIVER_SP(out_return_msg_drvr);
end;



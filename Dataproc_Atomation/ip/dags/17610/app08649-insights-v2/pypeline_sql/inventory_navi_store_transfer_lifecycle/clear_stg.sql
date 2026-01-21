/***************************************************************************************************************************************
-- Deleting all records from INVENTORY_NAVI_STORE_TRANSFER_LDG table since Spark tasks don't override data in there becouse of 
-- parallel inserting into the same INVENTORY_NAVI_STORE_TRANSFER_LDG table different events (StoreTransferCreated, StoreTransferShipped)
***************************************************************************************************************************************/

DELETE FROM {db_env}_NAP_BASE_VWS.INVENTORY_NAVI_STORE_TRANSFER_LDG;

ET;
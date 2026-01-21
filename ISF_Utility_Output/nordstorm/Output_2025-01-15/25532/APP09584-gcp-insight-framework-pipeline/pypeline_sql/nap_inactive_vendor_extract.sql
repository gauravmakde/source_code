SELECT psd.prmy_supp_num, vomd.market_code
-- contains PO activity
FROM {db_env}_NAP_USR_VWS.PURCHASE_ORDER_ITEM_FACT poif
    -- needed to get the corresponding vendor order from id
    JOIN {db_env}_NAP_USR_VWS.PRODUCT_SKU_DIM psd ON poif.rms_sku_num = psd.rms_sku_num
    -- needed to filter out non NPG/already inactivated vendors
    JOIN {db_env}_NAP_USR_VWS.VENDOR_ORDERFROM_MARKET_DIM vomd ON psd.prmy_supp_num = vomd.vendor_num
    -- needed to estimate vendor creation time
    JOIN {db_env}_NAP_USR_VWS.VENDOR_DIM vm ON vomd.vendor_num = vm.vendor_num
-- only non NPG vendors
WHERE psd.npg_ind = 'N'
-- only currently active vendors
  AND vomd.market_status in ('A', 'O')
-- only vendors that were created over 13 months ago
  AND vm.dw_sys_load_tmstp < ADD_MONTHS( CURRENT_DATE (), -13)
-- ensures distinct prmy_supp_num, market_code pairs
GROUP BY psd.prmy_supp_num, vomd.market_code
-- most recent event happened over 13 months ago, use add_months to handle varying month lengths
HAVING MAX (poif.latest_event_tmstp_pacific) < ADD_MONTHS( CURRENT_DATE (), -13)
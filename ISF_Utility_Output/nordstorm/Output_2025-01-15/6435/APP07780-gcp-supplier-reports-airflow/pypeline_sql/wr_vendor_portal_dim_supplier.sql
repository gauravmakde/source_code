select
    vendor_num supp_idnt,
    vendor_name supp_name
from
    {db_env}_NAP_BASE_VWS.VENDOR_DIM
where order_from_num<>'';
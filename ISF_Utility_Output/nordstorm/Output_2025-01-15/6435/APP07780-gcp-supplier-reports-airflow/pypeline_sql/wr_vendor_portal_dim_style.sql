select distinct 
coalesce(prmy_supp_num ,'') as supp_idnt,
coalesce(supp_part_num,'') as supp_prt_nbr,
rms_style_num as rms_style_idnt,
oreplace(style_desc , chr(10), '') rms_styl_desc,
channel_country mrkt_ind
from {db_env}_nap_base_vws.product_style_rms_dim_vw;
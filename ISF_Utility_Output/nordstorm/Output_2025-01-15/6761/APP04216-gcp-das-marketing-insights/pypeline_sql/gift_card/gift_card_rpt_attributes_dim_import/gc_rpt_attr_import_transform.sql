create temporary view rpt_attr_tmp AS select * from gift_card_rpt_attr_src;

create temporary view gc_rpt_work as
select
    alt_merchant_desc,
    merchant_desc,
    class_name,
    promo_code_desc,
    rpt_business_unit_name,
    rpt_location,
    rpt_geog,
    rpt_channel,
    rpt_banner,
    rpt_distribution_partner_desc,
    cast((cast(cast(current_timestamp() as double) as DECIMAL(14, 3)) * 1000) as long) as current_tmstp
FROM rpt_attr_tmp;

insert overwrite gift_card_rpt_attributes
select
    alt_merchant_desc,
    merchant_desc,
    class_name,
    promo_code_desc,
    rpt_business_unit_name,
    rpt_location,
    rpt_geog,
    rpt_channel,
    rpt_banner,
    rpt_distribution_partner_desc,
    current_tmstp as event_tmstp,
    current_tmstp as sys_tmstp
from gc_rpt_work;

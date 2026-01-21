
create or replace temporary view por_orders using orc OPTIONS (path "s3://mim-nap/probability-of-return/prod/pyspark/build/data/return_probability_online/");
create or replace temporary view por_transactions using orc OPTIONS (path "s3://mim-nap/probability-of-return/prod/pyspark/build/data/return_probability_brick/");

create or replace temporary view por_crosstable (
            bu_type string
            , order_country string
            , min_price int
            , max_price int
            , average_por decimal(7,6)
            )
using csv OPTIONS (path "s3://marketing-analytics-nordstrom/mta4box/PoR/crosstables_jan2022/", header "true");

create or replace temporary view mmm_adj (
            arrived_channel string
            , bu_type string
            , mktg_channel string
            , mmm_ce decimal(10,9)
            , fiscal_quarter int
            , base_prop decimal(3,2)
            , coef_1 int
            )
using csv OPTIONS (path "s3://marketing-analytics-nordstrom/mta4box/20210819/01/", header "true");

-- Definine new Hive table for output
create table if not exists ace_etl.mta_acp_scoring 
(
    acp_id string,
	order_number string,
    order_channel string,
    order_channelcountry string,
    sku_id string,
    arrived_event_id string,
    arrived_date_pacific date,
    arrived_channel string,
    arrived_channelcountry string,
    arrived_platform string,
    mktg_type string,
    finance_rollup string,
    finance_detail string,
    marketing_type string,
    attributed_order double,
    attributed_units double,
    attributed_demand double,
    attributed_pred_net double,
    utm_channel string,
    utm_campaign string,
    sp_campaign string,
    utm_term string,
    utm_content string,
    utm_source string,
    sp_source string,
    gclid string,
    is_marketplace string,
    partner_relationship_id string,
    partner_relationship_type string,
    order_date_pacific date
)
using orc
location 's3://ace-etl/mta/mta_acp_scoring/'
partitioned by (order_date_pacific);

--msck repair runs a sync on the partitions so we can bring all data into the subsequent query
msck repair table ace_etl.mta_acp_scoring;


create or replace temporary view touch_base as (
select  mta_tb.acp_id,
        mta_tb.units,
        mta_tb.demand,
        mta_tb.order_number,
        mta_tb.order_date_pacific,
        mta_tb.order_channel,
        mta_tb.order_channelcountry,
        case
            when mta_tb.order_channel in ('FULL LINE', 'FULL LINE CANADA') then 'FL'
            when mta_tb.order_channel in ('N.COM', 'N.CA') then 'FC'
            when mta_tb.order_channel in ('RACK', 'RACK CANADA') then 'RK'
            when mta_tb.order_channel = 'RACK.COM'  then 'HL'
        end as bu_type,
        mta_tb.style_id,
        mta_tb.sku_id,
        mta_tb.arrived_event_id,
        mta_tb.arrived_date_pacific,
        mta_tb.arrived_channel,
        mta_tb.arrived_channelcountry,
        mta_tb.arrived_platform,
        mta_tb.mktg_type,
        mc_hier.finance_rollup,
        mc_hier.finance_detail,
        mc_hier.marketing_type,
        mta_tb.marketing_touch,
        case
            when mc_hier.finance_detail in ('SEO_SHOPPING', 'SEO_SEARCH', 'SEO_LOCAL') then 'SEO'
            else mc_hier.finance_detail
        end as mktg_channel,
        mta_tb.utm_channel,
        mta_tb.utm_campaign,
        mta_tb.sp_campaign,
        mta_tb.utm_term,
        mta_tb.utm_content,
        mta_tb.utm_source,
        mta_tb.sp_source,
        mta_tb.gclid,
        mta_tb.is_marketplace,
        mta_tb.partner_relationship_id,
        mta_tb.partner_relationship_type
from    ace_etl.mta_touches mta_tb
left join ace_etl.utm_channel_lookup utm_chann_lkup ON lower(mta_tb.utm_channel) = lower(utm_chann_lkup.utm_mkt_chnl)
left join ace_etl.marketing_channel_hierarchy mc_hier ON coalesce(utm_chann_lkup.bi_channel, mta_tb.mktg_type) = mc_hier.join_channel
where   mta_tb.order_date_pacific between date'2024-05-16' and date'2024-05-22'
);

create or replace temporary view touch_demand as (
select  acp_id,
        touch_weight,
        units,
        demand,
        order_number,
        order_date_pacific,
        order_channel,
        order_channelcountry,
        bu_type,
        sku_id,
        arrived_event_id,
        arrived_date_pacific,
        arrived_channel,
        arrived_channelcountry,
        arrived_platform,
        mktg_type,
        finance_rollup,
        finance_detail,
        marketing_type,
        utm_channel,
        utm_campaign,
        sp_campaign,
        utm_term,
        utm_content,
        utm_source,
        sp_source,
        gclid,
        is_marketplace,
        partner_relationship_id,
        partner_relationship_type,
        touch_weight / sum(touch_weight) over (partition by order_number, sku_id) as attributed_item,
        touch_weight /sum(touch_weight) over (partition by order_number) as attributed_order,
        demand * touch_weight / sum(touch_weight) over (partition by order_number, sku_id) as attributed_demand,
        units * touch_weight / sum(touch_weight) over (partition by order_number, sku_id) as attributed_units
from (  select  acp_id,
                units,
                demand,
                order_number,
                order_date_pacific,
                order_channel,
                order_channelcountry,
                bu_type,
                sku_id,
                arrived_event_id,
                arrived_date_pacific,
                arrived_channel,
                arrived_channelcountry,
                arrived_platform,
                mktg_type,
                finance_rollup,
                finance_detail,
                marketing_type,
                utm_channel,
                utm_campaign,
                sp_campaign,
                utm_term,
                utm_content,
                utm_source,
                sp_source,
                gclid,
                is_marketplace,
                partner_relationship_id,
                partner_relationship_type,
                p,
                b,
                case
                when acp_id is not null and marketing_touch = 'N'
                    then coalesce(b, 0.55) * p
                when acp_id is not null and marketing_touch = 'Y'
                    then coalesce(1.0-b, 0.45) * p
                when acp_id is null and marketing_touch = 'N' then 0.25 * p
                when acp_id is null and marketing_touch = 'Y' then 0.75 * p
                else 0
                end as touch_weight
        from (  select  tb.acp_id,
                        tb.units,
                        tb.demand,
                        tb.order_number,
                        tb.order_date_pacific,
                        tb.order_channel,
                        tb.order_channelcountry,
                        tb.bu_type,
                        tb.style_id,
                        tb.sku_id,
                        tb.arrived_event_id,
                        tb.arrived_date_pacific,
                        tb.arrived_channel,
                        tb.arrived_channelcountry,
                        tb.arrived_platform,
                        tb.mktg_type,
                        tb.finance_rollup,
                        tb.finance_detail,
                        tb.marketing_type,
                        tb.marketing_touch,
                        tb.utm_channel,
                        tb.utm_campaign,
                        tb.sp_campaign,
                        tb.utm_term,
                        tb.utm_content,
                        tb.utm_source,
                        tb.sp_source,
                        tb.gclid,
                        tb.is_marketplace,
                        tb.partner_relationship_id,
                        tb.partner_relationship_type,
                        coalesce(ma.mmm_ce, 1.0) / sum(coalesce(ma.mmm_ce, 1.0)) over (
                            partition by tb.order_number, tb.style_id, tb.sku_id, tb.marketing_touch
                            ) as p,
                        avg(ma.base_prop) over (
                            partition by tb.order_number, tb.style_id, tb.sku_id
                            ) as b
                from    touch_base as tb
                left join mmm_adj as ma on ma.fiscal_quarter = 2
                                        and tb.arrived_channel = ma.arrived_channel
                                        and tb.bu_type = ma.bu_type
                                        and tb.mktg_channel = ma.mktg_channel
            )
    )
);

-- need to add in date filters to the por temp views
create or replace temporary view return_probability as (
select  bu_type,
        order_number,
        rms_sku_id,
        min(order_date_pacific) as sale_date,
        avg(return_probability) as return_prob
from    por_orders
where order_date_utc between date'2024-05-16' and date'2024-05-22'+1
group by bu_type, order_number, rms_sku_id
union all
select  bu_type,
        global_tran_id as order_number,
        rms_sku_id,
        min(tran_date) as sale_date,
        avg(return_probability) as return_prob
from    por_transactions
where business_date between date'2024-05-16' and date'2024-05-22'
group by bu_type, global_tran_id, rms_sku_id
);

create or replace temporary view return_crosstable as (
select  bu_type,
        order_country,
        min_price,
        max_price,
        average_por as return_prob
from    por_crosstable
);

create or replace temporary view mta_demand as (
select  td.acp_id,
        td.order_number,
        td.order_channel,
        td.order_channelcountry,
        td.sku_id,
        td.arrived_event_id,
        td.arrived_date_pacific,
        td.arrived_channel,
        td.arrived_channelcountry,
        td.arrived_platform,
        td.mktg_type,
        td.finance_rollup,
        td.finance_detail,
        td.marketing_type,
        CAST(td.attributed_order as double) as attributed_order,
        CAST(td.attributed_units as double) as attributed_units,
        td.attributed_demand,
        td.attributed_demand * (1.0 - coalesce(rp.return_prob, ct.return_prob)) as attributed_pred_net,        
        td.utm_channel,
        td.utm_campaign,
        td.sp_campaign,
        td.utm_term,
        td.utm_content,
        td.utm_source,
        td.sp_source,
        td.gclid,
        td.is_marketplace,
        td.partner_relationship_id,
        td.partner_relationship_type,
        td.order_date_pacific
from    touch_demand as td
left join return_probability as rp on td.order_number = rp.order_number
                                    and td.sku_id = rp.rms_sku_id
left join return_crosstable as ct on td.order_channelcountry = ct.order_country
                                    and td.bu_type = ct.bu_type
                                    and td.demand >= ct.min_price
                                    and td.demand < ct.max_price
);


-- Writing output to Hive table
insert overwrite table ace_etl.mta_acp_scoring partition (order_date_pacific)
select /*+ REPARTITION(100) */ * from mta_demand
;

-- Writing output to teradata landing table.  
insert overwrite table mta_acp_scoring_fact_ldg_output
select * from mta_demand
;


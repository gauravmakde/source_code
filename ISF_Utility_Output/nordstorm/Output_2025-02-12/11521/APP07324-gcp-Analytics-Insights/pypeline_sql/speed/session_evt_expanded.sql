--What this is for - this pipeline is to meet the needs of the DO team to get the data to build business dashboard. 
--Owner - Jin.Liu2 
--Created - 10/14/2022 
--Last modified - 5/17/2023 
--Data Source and output will be hive but PSS and PSCV will read from Parquet files until hive tables ready to use in SparkSQL
create table if not exists {hive_schema}.session_evt_expanded_session_base_staging (
    event_time_utc timestamp,
    event_time_pst timestamp,
    channel string,
    experience string,
    channelcountry string,
    session_id string,
    event_id string,
    event_name string,
    feature string,
    cust_idtype string,
    cust_id string,
    acp_id string, 
    ent_cust_id string,
    experiment_id string,
    loyalty_id string,
    original_context_pagetype string,
    context_pagetype string,
    pageinstance_id string,
    context_id string,
    page_rank bigint,
    Ord_session_flag int,
    ATB_session_flag int,
    ATB_Ord_session_flag int,
    activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/session_evt_expanded_session_base_staging' partitioned by (activity_date_partition);

create
or replace temp view session_base as
select
    event_time_utc,
    event_time_pst,
    channel,
    experience,
    channelcountry,
    session_id,
    event_id,
    event_name,
    upper(feature) as feature,
    cust_idtype,
    cust_id,
    acp_id, 
    ent_cust_id,
    experiment_id,
    loyalty_id,
    context_pagetype as original_context_pagetype,
    case
        when event_name in (
            'com.nordstrom.event.customer.Engaged',
            'com.nordstrom.event.customer.Impressed',
            'com.nordstrom.event.customer.ProductSummarySelected',
            'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
        ) then coalesce(
            last_value(
                case
                    when context_pagetype in ('UNDETERMINED', 'UNKNOWN') then null
                    else context_pagetype
                end,
                true
            ) over (
                partition by session_id,
                pageinstance_id
                order by
                    page_type_count,
                    event_time_utc desc,
                    context_pagetype asc rows between unbounded preceding
                    and unbounded following
            ),
            'UNDETERMINED'
        )
        else null
    end as context_pagetype,
    pageinstance_id,
    context_id,
    case
        when pageinstance_id is not null then sum(staging) over (
            partition by (
                case
                    when pageinstance_id is not null then session_id
                end
            )
            order by
                rn
        )
    end as page_rank,
    Ord_session_flag,
    ATB_session_flag,
    ATB_Ord_session_flag,
    activity_date_partition
from
    (
        select
            *,
            count(context_pagetype) over (
                partition by session_id,
                pageinstance_id,
                context_pagetype
            ) page_type_count,
            case
                pageinstance_id is not null
                when rn = 1 then 1
                when pageinstance_id = prior_page then 0
                when pageinstance_id <> prior_page then 1
                else null
            end as staging
        from
            (
                select
                    *,
                    case
                        when pageinstance_id is not null then last_value(pageinstance_id, true) over (
                            partition by (
                                case
                                    when pageinstance_id is not null then session_id
                                end
                            )
                            order by
                                event_time_utc,
                                pageinstance_id,
                                event_id asc rows between unbounded preceding
                                and 1 preceding
                        )
                        else null
                    end as prior_page
                from
                    (
                        select
                            activity_date_partition,
                            event_time_utc,
                            event_time_pst,
                            channel,
                            experience,
                            channelcountry,
                            session_id,
                            event_id,
                            event_name,
                            feature,
                            cust_idtype,
                            cust_id,
                            context_pagetype,
                            pageinstance_id,
                            context_id,
                            case
                                when pageinstance_id is not null then ROW_NUMBER() OVER (
                                    PARTITION BY (
                                        case
                                            when pageinstance_id is not null then session_id
                                        end
                                    )
                                    ORDER BY
                                        event_time_utc,
                                        pageinstance_id,
                                        event_id
                                )
                            end AS rn,
                            Ord_session_flag,
                            ATB_session_flag,
                            case
                                when Ord_session_flag = 1
                                or ATB_session_flag = 1 then 1
                                else 0
                            end as ATB_Ord_session_flag
                        from
                            (
                                select
                                    activity_date_partition,
                                    cast(event_time_utc as timestamp) event_time_utc,
                                    from_utc_timestamp(event_time_utc, 'US/Pacific') event_time_pst,
                                    channel,
                                    experience,
                                    channelcountry,
                                    session_id,
                                    event_id,
                                    event_name,
                                    feature,
                                    cust_idtype,
                                    cust_id,
                                    nullif(context_pagetype, '') context_pagetype,
                                    nullif(pageinstance_id, '') pageinstance_id,
                                    nullif(context_id, '') context_id,
                                    case
                                        when event_name in (
                                            'com.nordstrom.event.customer.Engaged',
                                            'com.nordstrom.event.customer.Impressed',
                                            'com.nordstrom.event.customer.ProductSummarySelected',
                                            'com.nordstrom.event.customer.ProductSummaryCollectionViewed'
                                        )
                                        and nullif(pageinstance_id, '') is null then 0
                                        else 1
                                    end as event_flag,
                                    count(
                                        case
                                            when nullif(pageinstance_id, '') is not null then pageinstance_id
                                        end
                                    ) over (partition by session_id) as session_flag,
                                    max(
                                        case
                                            when event_name = 'com.nordstrom.customer.OrderSubmitted' then 1
                                            else 0
                                        end
                                    ) over (partition by session_id) as Ord_session_flag,
                                    max(
                                        case
                                            when event_name = 'com.nordstrom.customer.AddedToBag' then 1
                                            else 0
                                        end
                                    ) over (partition by session_id) as ATB_session_flag
                                from
                                    acp_vector.customer_digital_session_evt_fact e
                                where
                                    activity_date_partition between {start_date}
                                    and {end_date}
                                    and e.selling_channel = 'ONLINE'
                                    and channelcountry ='US'
                                    and experience in (
                                        'DESKTOP_WEB',
                                        'MOBILE_WEB',
                                        'IOS_APP',
                                        'ANDROID_APP'
                                    )
                                    and event_name in (
                                        'com.nordstrom.event.customer.Engaged',
                                        'com.nordstrom.event.customer.Impressed',
                                        'com.nordstrom.event.customer.ProductSummarySelected',
                                        'com.nordstrom.event.customer.ProductSummaryCollectionViewed',
                                        'com.nordstrom.customer.OrderSubmitted',
                                        'com.nordstrom.customer.AddedToBag'
                                    )
                            )
                        where
                            event_flag = 1
                            and session_flag >= 1
                    )
            )
    ) base
    left join 
    (
    select 
        session_id as xref_session_id,
        shopper_id,
        nullif(acp_id,'') as acp_id, 
        nullif(ent_cust_id,'') as ent_cust_id,
        nullif(experiment_id,'')  as experiment_id,
        nullif(loyalty_id,'') as loyalty_id
    from acp_vector.customer_session_xref
    where activity_date_partition between {start_date} and {end_date}
    ) x 
    on base.cust_id = x.shopper_id
    and base.session_id = x.xref_session_id;

insert
    overwrite table {hive_schema}.session_evt_expanded_session_base_staging partition (activity_date_partition)
select
    *
from
    session_base;

MSCK REPAIR table {hive_schema}.session_evt_expanded_session_base_staging;

create table if not exists {hive_schema}.session_evt_expanded_page_assignment_staging (
    event_time_utc timestamp,
    event_time_pst timestamp,
    channel string,
    experience string,
    channelcountry string,
    session_id string,
    event_id string,
    event_name string,
    feature string,
    cust_idtype string,
    cust_id string,
    acp_id string, 
    ent_cust_id string,
    experiment_id string,
    loyalty_id string,
    original_context_pagetype string,
    context_pagetype string,
    pageinstance_id string,
    entry_pageinstance_id string,
    entry_context_pagetype string,
    exit_pageinstance_id string,
    exit_context_pagetype string,
    page_rank bigint,
    context_id string,
    ATB_flag int,
    quantity int,
    bagsize bigint,
    atb_style_id string,
    atb_RMSSKU_id string,
    bagtype string,
    bagactiontype string,
    ATB_REGULAR_UNITS decimal(36, 6),
    ATB_current_UNITS decimal(36, 6),
    ATB_discount_UNITS decimal(36, 6),
    prior_pageinstance_id string,
    prior_context_pagetype string,
    prior_page_rank bigint,
    next_pageinstance_id string,
    next_context_pagetype string,
    next_page_rank bigint,
    activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/session_evt_expanded_page_assignment_staging' partitioned by (activity_date_partition);

create
or replace temp view engage as
select
    cast(headers ['Id'] AS string) as event_id,
    action as action,
    element.type as element_type,
    element.id as element_id,
    element.subid as element_subid,
    element.value as element_value,
    element.state as element_state,
    element.index as element_index,
    dc.idtype,
    dc.id,
    case
        when dc.idtype = 'STYLE' then dc.id
    end as eng_style_id,
    case
        when dc.idtype = 'RMSSKU' then dc.id
    end as eng_RMSSKU_id
from
    acp_event_view.customer_activity_engaged LATERAL VIEW OUTER explode(context.digitalcontents) as dc
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US'
    ;

create
or replace temp view atb_events as
select
    cast(headers ['Id'] AS string) as event_id,
    case
        when (
            bagtype = 'DEFAULT'
            and (
                (
                    bagactiontype = 'UNDETERMINED'
                    and SOURCE.feature = 'ShoppingBag'
                )
                or bagactiontype = 'ADD'
            )
        ) then 1
        else 0
    end as ATB_flag,
    quantity,
    bagsize,
    productStyle.id as atb_style_id,
    product.id as atb_RMSSKU_id,
    bagtype,
    bagactiontype,
    cast(
        price.regular.units + price.regular.nanos / 1000000000 as decimal(36, 6)
    ) as ATB_REGULAR_UNITS,
    cast(
        price.current.units + price.current.nanos / 1000000000 as decimal(36, 6)
    ) as ATB_current_UNITS,
    cast(
        price.discount.units + price.discount.nanos / 1000000000 as decimal(36, 6)
    ) as ATB_discount_UNITS
from
    acp_event_view.customer_activity_added_to_bag
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and SOURCE.platform IN ('WEB', 'MOW', 'IOS', 'ANDROID')
    and source.channelcountry ='US';

create
or replace temp view atb_ord_session_with_ord_page as
select
    session_id,
    ATB_session_flag,
    event_time_utc,
    a.event_id,
    event_name,
    coalesce(pageinstance_id, assigned_ord_pageid) as pageinstance_id,
    coalesce(page_rank, assigned_ord_page_rank) as page_rank,
    coalesce(context_pagetype, assigned_ord_pagetype) as context_pagetype,
    eng_style_id,
    eng_RMSSKU_id,
    prior_pageid,
    next_pageid,
    prior_context_pagetype,
    next_context_pagetype,
    prior_page_rank,
    next_page_rank
from
    (
        select
            *,
            case
                when prior_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then prior_pageid
                when next_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then next_pageid
                when event_name = 'com.nordstrom.customer.OrderSubmitted' then coalesce(prior_pageid, next_pageid)
                else null
            end as assigned_ord_pageid,
            case
                when prior_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then prior_context_pagetype
                when next_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then next_context_pagetype
                when event_name = 'com.nordstrom.customer.OrderSubmitted' then coalesce(prior_context_pagetype, next_context_pagetype)
                else null
            end as assigned_ord_pagetype,
            case
                when prior_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then prior_page_rank
                when next_context_pagetype = 'REVIEW_ORDER'
                and event_name = 'com.nordstrom.customer.OrderSubmitted' then next_page_rank
                when event_name = 'com.nordstrom.customer.OrderSubmitted' then coalesce(prior_page_rank, next_page_rank)
                else null
            end as assigned_ord_page_rank
        from
            (
                select
                    session_id,
                    ATB_session_flag,
                    event_time_utc,
                    a.event_id,
                    event_name,
                    pageinstance_id,
                    page_rank,
                    context_pagetype,
                    last_value(pageinstance_id, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between unbounded preceding
                            and 1 preceding
                    ) as prior_pageid,
                    first_value(pageinstance_id, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between 1 following
                            and unbounded following
                    ) as next_pageid,
                    last_value(context_pagetype, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between unbounded preceding
                            and 1 preceding
                    ) as prior_context_pagetype,
                    first_value(context_pagetype, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between 1 following
                            and unbounded following
                    ) as next_context_pagetype,
                    last_value(page_rank, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between unbounded preceding
                            and 1 preceding
                    ) as prior_page_rank,
                    first_value(page_rank, true) over (
                        partition by session_id
                        order by
                            event_time_utc,
                            pageinstance_id,
                            event_id rows between 1 following
                            and unbounded following
                    ) as next_page_rank
                from
                    {hive_schema}.session_evt_expanded_session_base_staging a
                where
                    ATB_Ord_session_flag = 1
                    and activity_date_partition between {start_date}
                    and {end_date}
            )
    ) a
    left join (
        select
            *
        from
            engage
        where
            engage.idtype in ('STYLE', 'RMSSKU')
    ) engage on a.event_id = engage.event_id;

create
or replace temp view atb_page as with atb as (
    select
        session_id,
        event_time_utc,
        a.event_id,
        event_name,
        ATB_flag,
        quantity,
        bagsize,
        atb_style_id,
        atb_RMSSKU_id,
        bagtype,
        bagactiontype,
        ATB_REGULAR_UNITS,
        ATB_current_UNITS,
        ATB_discount_UNITS
    from
        atb_ord_session_with_ord_page a
        left join atb_events b on a.event_id = b.event_id
    where
        event_name = 'com.nordstrom.customer.AddedToBag'
),
eng as (
    select
        session_id,
        event_time_utc,
        event_id,
        event_name,
        page_rank,
        eng_style_id,
        eng_RMSSKU_id,
        context_pagetype,
        pageinstance_id
    from
        atb_ord_session_with_ord_page
    where
        event_name in ('com.nordstrom.event.customer.Engaged')
        and ATB_session_flag = 1
),
join_table as (
    select
        ATB_flag,
        quantity,
        bagsize,
        bagtype,
        bagactiontype,
        ATB_REGULAR_UNITS,
        ATB_current_UNITS,
        ATB_discount_UNITS,
        a.session_id,
        a.event_id as atb_event_id,
        a.event_time_utc as atb_evt_time,
        b.event_id as eng_event_id,
        b.event_time_utc as eng_evt_time,
        abs(
            ROUND(
                (
                    CAST(a.event_time_utc AS DOUBLE) - CAST(b.event_time_utc AS DOUBLE)
                ) * 1000
            ) / 1000.000
        ) as time_diff_abs,
        ROUND(
            (
                CAST(a.event_time_utc AS DOUBLE) - CAST(b.event_time_utc AS DOUBLE)
            ) * 1000
        ) / 1000.000 as time_diff,
        a.atb_rmssku_id,
        b.eng_rmssku_id,
        case
            when b.eng_RMSSKU_id is not null then 1
            else 0
        end as sku_matched,
        a.atb_style_id,
        b.eng_style_id,
        case
            when b.eng_style_id is not null then 1
            else 0
        end as style_matched,
        b.context_pagetype,
        b.pageinstance_id,
        page_rank
    from
        atb a
        left join eng b on a.session_id = b.session_id
        and (
            a.atb_rmssku_id = b.eng_rmssku_id
            or a.atb_style_id = b.eng_style_id
        )
        and abs(
            unix_timestamp(a.event_time_utc) - unix_timestamp(b.event_time_utc)
        ) <= 10
)
select
    distinct session_id,
    atb_event_id,
    ATB_flag,
    quantity,
    bagsize,
    atb_style_id,
    atb_RMSSKU_id,
    bagtype,
    bagactiontype,
    ATB_REGULAR_UNITS,
    ATB_current_UNITS,
    ATB_discount_UNITS,
    pageinstance_id,
    page_rank,
    context_pagetype
from
    (
        select
            *,
            row_number() over(
                partition by session_id,
                atb_event_id
                order by
                    find_result_rk
            ) as final_rk
        from
            (
                select
                    *,
                    sku_matched * -1000000 + style_matched * (1 - sku_matched) * -100 + time_diff_abs + 0.0000001 * time_diff as find_result_rk
                from
                    join_table
            )
    )
where
    final_rk = 1;

create
or replace temp view atb_ord_session_with_ord_page_atb_page as
select
    a.session_id,
    event_id,
    event_name,
    ATB_flag,
    quantity,
    bagsize,
    atb_style_id,
    atb_RMSSKU_id,
    bagtype,
    bagactiontype,
    ATB_REGULAR_UNITS,
    ATB_current_UNITS,
    ATB_discount_UNITS,
    coalesce(
        a.pageinstance_id,
        atb.pageinstance_id,
        prior_pageid,
        next_pageid
    ) as pageinstance_id,
    coalesce(
        a.page_rank,
        atb.page_rank,
        prior_page_rank,
        next_page_rank
    ) page_rank,
    coalesce(
        a.context_pagetype,
        atb.context_pagetype,
        prior_context_pagetype,
        next_context_pagetype
    ) context_pagetype
from
    (
        select
            distinct session_id,
            event_id,
            event_name,
            pageinstance_id,
            page_rank,
            context_pagetype,
            prior_pageid,
            next_pageid,
            prior_context_pagetype,
            next_context_pagetype,
            prior_page_rank,
            next_page_rank
        from
            atb_ord_session_with_ord_page
        where
            event_name in (
                'com.nordstrom.customer.OrderSubmitted',
                'com.nordstrom.customer.AddedToBag'
            )
    ) a
    left join atb_page atb on a.event_id = atb.atb_event_id
    and a.session_id = atb.session_id;

create
or replace temp view user_action_events as
select
    *,
    max(
        case
            when page_rank = 1 then pageinstance_id
            else null
        end
    ) over (partition by session_id) as entry_pageinstance_id,
    max(
        case
            when page_rank = 1 then context_pagetype
            else null
        end
    ) over (partition by session_id) as entry_context_pagetype,
    max(
        case
            when page_rank = max_page_rank_per_session then pageinstance_id
            else null
        end
    ) over (partition by session_id) as exit_pageinstance_id,
    max(
        case
            when page_rank = max_page_rank_per_session then context_pagetype
            else null
        end
    ) over (partition by session_id) as exit_context_pagetype
from
    (
        select
            activity_date_partition,
            event_time_utc,
            event_time_pst,
            channel,
            experience,
            channelcountry,
            a.session_id,
            a.event_id,
            a.event_name,
            feature,
            cust_idtype,
            cust_id,
            acp_id , 
            ent_cust_id ,
            experiment_id ,
            loyalty_id ,
            context_id,
            ATB_flag,
            quantity,
            bagsize,
            atb_style_id,
            atb_RMSSKU_id,
            bagtype,
            bagactiontype,
            ATB_REGULAR_UNITS,
            ATB_current_UNITS,
            ATB_discount_UNITS,
            original_context_pagetype,
            coalesce(a.context_pagetype, b.context_pagetype) as context_pagetype,
            coalesce(a.pageinstance_id, b.pageinstance_id) as pageinstance_id,
            coalesce(a.page_rank, b.page_rank) as page_rank,
            max(coalesce(a.page_rank, b.page_rank)) over (partition by a.session_id) as max_page_rank_per_session
        from
            {hive_schema}.session_evt_expanded_session_base_staging a
            left join atb_ord_session_with_ord_page_atb_page b on a.event_id = b.event_id
            and a.session_id = b.session_id
        where
            activity_date_partition between {start_date}
            and {end_date}
    );

create
or replace temp view user_action_events_prior_next_pages as with pageinfo as (
    select
        session_id,
        pageinstance_id,
        page_rank,
        context_pagetype
    from
        user_action_events
    group by
        1,
        2,
        3,
        4
)
select
    a.event_time_utc,
    a.event_time_pst,
    a.channel,
    a.experience,
    a.channelcountry,
    a.session_id,
    a.event_id,
    a.event_name,
    a.feature,
    a.cust_idtype,
    a.cust_id,
    a.acp_id , 
    a.ent_cust_id ,
    a.experiment_id ,
    a.loyalty_id ,
    a.original_context_pagetype,
    a.context_pagetype,
    a.pageinstance_id,
    a.entry_pageinstance_id,
    a.entry_context_pagetype,
    a.exit_pageinstance_id,
    a.exit_context_pagetype,
    a.page_rank,
    a.context_id,
    a.ATB_flag,
    a.quantity,
    a.bagsize,
    a.atb_style_id,
    a.atb_RMSSKU_id,
    a.bagtype,
    a.bagactiontype,
    a.ATB_REGULAR_UNITS,
    a.ATB_current_UNITS,
    a.ATB_discount_UNITS,
    prior.pageinstance_id as prior_pageinstance_id,
    prior.context_pagetype as prior_context_pagetype,
    prior.page_rank as prior_page_rank,
    next.pageinstance_id as next_pageinstance_id,
    next.context_pagetype as next_context_pagetype,
    next.page_rank as next_page_rank,
    a.activity_date_partition
from
    user_action_events a
    left join pageinfo as prior on a.session_id = prior.session_id
    and a.page_rank -1 = prior.page_rank
    left join pageinfo as next on a.session_id = next.session_id
    and a.page_rank + 1 = next.page_rank;

insert
    overwrite table {hive_schema}.session_evt_expanded_page_assignment_staging partition (activity_date_partition)
select
    *
from
    user_action_events_prior_next_pages;

MSCK REPAIR table {hive_schema}.session_evt_expanded_page_assignment_staging;

create table if not exists {hive_schema}.session_evt_expanded (
    event_time_utc timestamp,
    event_time_pst timestamp,
    channel string,
    experience string,
    channelcountry string,
    session_page_rank_id string,
    session_id string,
    event_id string,
    event_name string,
    feature string,
    cust_idtype string,
    cust_id string,
    acp_id string, 
    ent_cust_id string,
    experiment_id string,
    loyalty_id string,
    original_context_pagetype string,
    context_pagetype string,
    pageinstance_id string,
    page_rank bigint,
    prior_pageinstance_id string,
    prior_context_pagetype string,
    next_pageinstance_id string,
    next_context_pagetype string,
    entry_pageinstance_id string,
    entry_context_pagetype string,
    exit_pageinstance_id string,
    exit_context_pagetype string,
    context_id string,
    action string,
    element_id string,
    element_index bigint,
    element_type string,
    element_value string,
    element_state string,
    element_subid string,
    element_subindex bigint,
    productstyle_type string,
    productstyle_id string,
    digitalcontents_type string,
    digitalcontents_id string,
    ordernumber string,
    order_style_id string,
    order_rmssku_id string,
    orderLineId string,
    orderLineNumber string,
    ORDER_LINE_REGULAR_UNITS decimal(36, 6),
    ORDER_LINE_current_UNITS decimal(36, 6),
    ORDER_LINE_discount_UNITS decimal(36, 6),
    atb_flag int,
    bagtype string,
    bagactiontype string,
    atb_style_id string,
    atb_RMSSKU_id string,
    ATB_REGULAR_UNITS decimal(36, 6),
    ATB_current_UNITS decimal(36, 6),
    quantity int,
    bagsize bigint,
    activity_date_partition date
) using ORC location 's3://{s3_bucket_root_var}/session_evt_expanded' partitioned by (activity_date_partition);

create
or replace temp view engage as
select
    cast(headers ['Id'] AS string) as event_id,
    action as action,
    element.type as element_type,
    element.id as element_id,
    element.subid as element_subid,
    element.value as element_value,
    element.state as element_state,
    element.index as element_index,
    dc.idtype,
    dc.id,
    case
        when dc.idtype = 'STYLE' then dc.id
    end as eng_style_id,
    case
        when dc.idtype = 'RMSSKU' then dc.id
    end as eng_RMSSKU_id
from
    acp_event_view.customer_activity_engaged LATERAL VIEW OUTER explode(context.digitalcontents) as dc
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US';

create
or replace temp view impress as
select
    cast(headers ['Id'] AS string) as event_id,
    elements,
    context.digitalContents
from
    acp_event_view.customer_activity_impressed
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US';

create
or replace temp view pscv as
select
    cast(headers ['Id'] AS string) as event_id,
    productsummarycollection AS psc_array,
    context.digitalContents
from
     acp_event_view.customer_activity_product_summary_collection_viewed
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US';

create
or replace temp view pss as
select
    cast(headers ['Id'] AS string) as event_id,
    productsummary.element.id as element_id,
    productsummary.element.subid as element_subid,
    productsummary.element.index as element_index,
    productsummary.element.subindex as element_subindex,
    productsummary.productstyle.idtype as productstyle_type,
    productsummary.productstyle.id as productstyle_id,
    context.digitalContents
from
    acp_event_view.customer_activity_product_summary_selected
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US';

CREATE
OR REPLACE TEMPORARY VIEW orders as
select
    distinct cast(headers ['Id'] AS string) as event_id,
    invoice.ordernumber as ordernumber,
    i.orderLineId,
    i.orderLineNumber,
    i.product.id AS order_RMSSKU_id,
    i.productStyle.id AS order_style_id,
    i.price.regular.currencyCode as ORDER_CURRENCY_CODE,
    cast(
        i.price.regular.units + i.price.regular.nanos / 1000000000 as decimal(36, 6)
    ) as ORDER_LINE_REGULAR_UNITS,
    cast(
        i.price.current.units + i.price.current.nanos / 1000000000 as decimal(36, 6)
    ) as ORDER_LINE_current_UNITS,
    cast(
        i.price.discount.units + i.price.discount.nanos / 1000000000 as decimal(36, 6)
    ) as ORDER_LINE_discount_UNITS
from
    acp_event_view.customer_activity_order_submitted o LATERAL VIEW OUTER explode (items) AS i
where
    to_date(
        concat(
            year,
            '-',
            LPAD(month, 2, '0'),
            '-',
            LPAD(day, 2, '0')
        ),
        'yyyy-MM-dd'
    ) between {start_date} - 1
    and {end_date} + 1
    and source.channelcountry ='US';

create
or replace temp view user_action_events_exp_meta as
select
    event_time_utc,
    event_time_pst,
    channel,
    experience,
    channelcountry,
    session_id,
    a.event_id,
    event_name,
    feature,
    cust_idtype,
    cust_id,
    acp_id , 
    ent_cust_id ,
    experiment_id ,
    loyalty_id ,
    action,
    element_type,
    element_value,
    element_state,
    pageinstance_id,
    original_context_pagetype,
    context_pagetype,
    page_rank,
    prior_pageinstance_id,
    next_pageinstance_id,
    prior_context_pagetype,
    next_context_pagetype,
    entry_pageinstance_id,
    entry_context_pagetype,
    exit_pageinstance_id,
    exit_context_pagetype,
    context_id,
    atb_style_id,
    atb_RMSSKU_id,
    ordernumber,
    order_RMSSKU_id,
    order_style_id,
    orderLineId,
    orderLineNumber,
    ORDER_LINE_REGULAR_UNITS,
    ORDER_LINE_current_UNITS,
    ORDER_LINE_discount_UNITS,
    atb_flag,
    quantity,
    bagsize,
    bagtype,
    bagactiontype,
    ATB_REGULAR_UNITS,
    ATB_current_UNITS,
    activity_date_partition,
    impress.elements as impress_elements,
    pscv.psc_array,
    pss.element_subindex,
    pss.productstyle_type,
    pss.productstyle_id,
    coalesce(pss.element_id, engage.element_id) as element_id,
    coalesce(pss.element_subid, engage.element_subid) as element_subid,
    coalesce(pss.element_index, engage.element_index) as element_index,
    engage.idtype as digitalcontents_type,
    engage.id as digitalcontents_id,
    coalesce(
        impress.digitalcontents,
        pscv.digitalcontents,
        pss.digitalcontents
    ) as digitalcontents
from
    {hive_schema}.session_evt_expanded_page_assignment_staging a
    left join impress on a.event_id = impress.event_id
    left join pscv on a.event_id = pscv.event_id
    left join pss on a.event_id = pss.event_id
    left join engage on a.event_id = engage.event_id
    left join orders on a.event_id = orders.event_id
where
    activity_date_partition between {start_date}
    and {end_date};

create
or replace temp view user_action_events_exp_joined as
select
    event_time_utc,
    event_time_pst,
    channel,
    experience,
    channelcountry,
    concat(session_id,page_rank)session_page_rank_id,
    session_id,
    event_id,
    event_name,
    feature,
    cust_idtype,
    cust_id,
    acp_id , 
    ent_cust_id ,
    experiment_id ,
    loyalty_id ,
    original_context_pagetype,
    context_pagetype,
    pageinstance_id,
    page_rank,
    prior_pageinstance_id,
    next_pageinstance_id,
    prior_context_pagetype,
    next_context_pagetype,
    context_id,
    entry_pageinstance_id,
    entry_context_pagetype,
    exit_pageinstance_id,
    exit_context_pagetype,
    action,
    coalesce(elements.id, psc.element.id, a.element_id) as element_id,
    coalesce(
        elements.index,
        psc.element.index,
        a.element_index
    ) as element_index,
    coalesce(elements.type, a.element_type) as element_type,
    coalesce(elements.value, a.element_value) as element_value,
    coalesce(elements.state, a.element_state) as element_state,
    coalesce(
        elements.subid,
        psc.element.subid,
        a.element_subid
    ) as element_subid,
    coalesce(psc.element.subindex, a.element_subindex) as element_subindex,
    coalesce(psc.productstyle.idtype, a.productstyle_type) as productstyle_type,
    coalesce(psc.productstyle.id, a.productstyle_id) as productstyle_id,
    coalesce(a.digitalcontents_type, dc.idtype) as digitalcontents_type,
    coalesce(a.digitalcontents_id, dc.id) as digitalcontents_id,
    ordernumber,
    order_style_id,
    order_rmssku_id,
    orderLineId,
    orderLineNumber,
    ORDER_LINE_REGULAR_UNITS,
    ORDER_LINE_current_UNITS,
    ORDER_LINE_discount_UNITS,
    atb_flag,
    bagtype,
    bagactiontype,
    atb_style_id,
    atb_RMSSKU_id,
    ATB_REGULAR_UNITS,
    ATB_current_UNITS,
    quantity,
    bagsize,
    activity_date_partition
from
    user_action_events_exp_meta a LATERAL VIEW OUTER explode(impress_elements) as elements LATERAL VIEW OUTER explode(digitalcontents) as dc LATERAL VIEW OUTER explode(psc_array) as psc;

insert
    overwrite table {hive_schema}.session_evt_expanded partition (activity_date_partition)
select
    event_time_utc,
    event_time_pst,
    channel,
    experience,
    channelcountry,
    session_page_rank_id,
    session_id,
    event_id,
    event_name,
    feature,
    cust_idtype,
    cust_id,
    acp_id , 
    ent_cust_id ,
    experiment_id ,
    loyalty_id ,
    original_context_pagetype,
    context_pagetype,
    pageinstance_id,
    page_rank,
    prior_pageinstance_id,
    prior_context_pagetype,
    next_pageinstance_id,
    next_context_pagetype,
    entry_pageinstance_id,
    entry_context_pagetype,
    exit_pageinstance_id,
    exit_context_pagetype,
    context_id,
    action,
    element_id,
    element_index,
    element_type,
    element_value,
    element_state,
    element_subid,
    element_subindex,
    productstyle_type,
    productstyle_id,
    digitalcontents_type,
    digitalcontents_id,
    ordernumber,
    order_style_id,
    order_rmssku_id,
    orderLineId,
    orderLineNumber,
    ORDER_LINE_REGULAR_UNITS,
    ORDER_LINE_current_UNITS,
    ORDER_LINE_discount_UNITS,
    atb_flag,
    bagtype,
    bagactiontype,
    atb_style_id,
    atb_RMSSKU_id,
    ATB_REGULAR_UNITS,
    ATB_current_UNITS,
    quantity, 
    bagsize,
    activity_date_partition
from
    user_action_events_exp_joined;

MSCK REPAIR table {hive_schema}.session_evt_expanded; 
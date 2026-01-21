--FILE CREATION DATE - 04/10/2023
--CREATED BY - Jin Liu
--DataSource - Hive
--Cadence - Daily (incremental)
--Last modified: 9/19/2023



create
or replace temp view browse_analytical_base as
SELECT
    activity_date_partition,
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    event_id,
    element_id,
    element_value,
    element_type,
    element_index,
    browse_context_id as context_id,
    pageinstance_id,
    page_rank,
    rank() over (
        partition by session_id
        order by
            page_rank asc
    ) as browse_session_page_rank,
    arrival_rank,
    max(
        case
            when arrival_rank >= 1 then 1
            else 0
        end
    ) over (
        partition by session_id,
        pageinstance_id
    ) as external_signal,
    prior_context_pagetype,
    referrer,
    event_time_utc,
    event_name,
    productstyle_id,
    feature,
    context_pagetype,
    page_indicator,
    browse_value,
    last_value(browse_value) over (
        partition by session_id
        order by
            page_rank,
            event_time_utc,
            event_id rows between unbounded preceding
            and 1 preceding
    ) as prior_browse_value,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    row_number() over (
        partition by session_id
        order by
            page_rank,
            event_time_utc,
            event_id
    ) as rn
FROM
    acp_event_intermediate.session_evt_expanded_attributes_parquet se
    left join (
        select
            session_id as browse_session_id,
            key_page_instance_id,
            context_id as browse_context_id
        from
            acp_event_intermediate.browse_event_attributes_parquet
        where
            activity_date_partition between {start_date}
            and {end_date}
    ) browse on se.session_id = browse.browse_session_id
    and se.pageinstance_id = browse.key_page_instance_id
where
    activity_date_partition between {start_date}
    and {end_date}
    and channelcountry = 'US'
    and page_indicator = 'BROWSE';

create
or replace temp view last_events as
select
    current.session_id,
    current.pageinstance_id,
    rank() over (
        partition by current.session_id,
        current.pageinstance_id
        order by
            current.page_rank
    ) as page_internal_rank,
    prior.context_pagetype as prior_last_context_pagetype,
    prior.last_click_feature as prior_last_feature,
    prior.last_click_element_id as PRIOR_LAST_element,
    prior.last_click_element_type as prior_last_element_type
from
    acp_event_intermediate.session_page_attributes_parquet current
    left join (
        select
            distinct session_id,
            page_rank,
            context_pagetype,
            last_value(
                case
                    when event_name in ('com.nordstrom.event.customer.Engaged')
                    and lower(action) = 'click' then feature
                end,
                true
            ) over (
                partition by session_page_rank_id
                order by
                    event_time_pst,
                    event_id rows between unbounded preceding
                    and unbounded following
            ) as last_click_feature,
            last_value(
                case
                    when event_name in ('com.nordstrom.event.customer.Engaged')
                    and lower(action) = 'click' then element_id
                end,
                true
            ) over (
                partition by session_page_rank_id
                order by
                    event_time_pst,
                    event_id rows between unbounded preceding
                    and unbounded following
            ) as last_click_element_id,
            last_value(
                case
                    when event_name in ('com.nordstrom.event.customer.Engaged')
                    and lower(action) = 'click' then element_type
                end,
                true
            ) over (
                partition by session_page_rank_id
                order by
                    event_time_pst,
                    event_id rows between unbounded preceding
                    and unbounded following
            ) as last_click_element_type
        from
            acp_event_intermediate.session_evt_expanded_attributes_parquet
        where
            activity_date_partition between {start_date}
            and {end_date}
    ) prior on current.session_id = prior.session_id
    and current.page_rank -1 = prior.page_rank
where
    current.activity_date_partition between {start_date}
    and {end_date}
    and page_indicator = 'BROWSE';

create
or replace temp view base_ref_origin as
select
    base.activity_date_partition,
    base.session_id,
    base.session_page_rank_id,
    base.channel,
    base.experience,
    base.channelcountry,
    base.event_id,
    base.element_id,
    base.element_value,
    base.element_type,
    base.element_index,
    base.context_id,
    base.pageinstance_id,
    base.page_rank,
    base.arrival_rank,
    base.prior_context_pagetype,
    base.referrer,
    base.event_time_utc,
    base.event_name,
    base.productstyle_id,
    base.feature,
    base.context_pagetype,
    base.page_indicator,
    base.browse_value,
    base.prior_browse_value,
    base.browse_filters_key,
    base.browse_sortorder,
    base.browse_totalresultcount,
    base.rn,
    PRIOR_LAST_element,
    prior_last_context_pagetype,
    prior_last_feature,
    prior_last_element_type,
    case
        when external_signal = 0
        and PRIOR_LAST_element = 'Content/Component'
        and prior_last_feature = 'CONTENT' then 'Content Navigation'
        when external_signal = 0
        and PRIOR_LAST_element = 'ProductResults/Navigation/Link'
        and prior_last_feature = 'SEARCH_BROWSE' then 'Category Navigation'
        when external_signal = 0
        and prior_last_element = 'Global/Navigation/Section/Link' then 'Global Navigation'
        when external_signal = 0
        and prior_last_element = 'StoreDetail/ShopMyStore/Department' then 'Shop My Store Engagement'
        when external_signal = 0
        and prior_last_element in (
            'ProductDetail/BrandTitleLink',
            'MiniPDP/BrandTitleLink',
            'ProductDetail/ShopTheBrand',
            'ProductDetail/BrandTitle'
        ) then 'Shop the Brand'
        when external_signal = 0
        and PRIOR_LAST_element in (
            'ProductResults/Filters/Section/Filter/Select',
            'ProductResults/Filters/Section/Filter/Deselect',
            'ProductResults/Filters/ActiveFilters/Filter/Deselect',
            'ProductResults/Filters/ActiveFilters/ClearFilters',
            'ProductResults/FilterTab',
            'ProductResults/Filters/Section/CurrentLocation',
            'ProductResults/Filters/ViewResults'
        )
        and prior_last_feature = 'SEARCH_BROWSE' then 'Filter Engagement'
        when external_signal = 0
        and PRIOR_LAST_element in ('ProductResults/ProductGallery/SortSelect')
        and prior_last_feature = 'SEARCH_BROWSE' then 'Sort Engagement'
        when external_signal = 0
        and PRIOR_LAST_element is null
        and experience in ('DESKTOP_WEB')
        and prior_context_pagetype in (
            'BRAND_RESULTS',
            'CATEGORY_RESULTS',
            'FLASH_EVENT_RESULTS',
            'FLASH_EVENTS',
            'SEARCH_RESULTS',
            'UNDETERMINED'
        ) then 'Path Engagement'
        when external_signal = 0
        and (
            lower(PRIOR_LAST_element) like '%tabbar%'
            and experience <> 'ANDROID_APP'
        )
        or (
            lower(PRIOR_LAST_element) in ('accounttab', 'hometab', 'bagtab', 'wishlisttab')
            and experience = 'ANDROID_APP'
        ) then 'TabBar Engagement'
        when external_signal = 0
        and PRIOR_LAST_element is null
        and browse_session_page_rank = 1 then 'Returning Session'
        when external_signal = 0
        and PRIOR_LAST_element is null
        and experience not in ('DESKTOP_WEB', 'MOBILE_WEB')
        and browse_session_page_rank > 1 then 'Pagination'
        when external_signal = 0
        and (
            PRIOR_LAST_element = 'ProductResults/ProductGallery/Pagination'
            and experience in ('DESKTOP_WEB', 'MOBILE_WEB')
        )
        and browse_session_page_rank > 1 then 'Pagination'
        when external_signal = 0
        and PRIOR_LAST_element is not null
        and browse_session_page_rank = 1 then 'Other'
        else 'Other'
    end as browse_int_origin,
    external_signal
from
    browse_analytical_base base
    left join last_events le on base.session_id = le.session_id
    and base.pageinstance_id = le.pageinstance_id
    and page_internal_rank = 1;

create
or replace temp view parent_path as
select
    activity_date_partition,
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    event_id,
    element_id,
    element_value,
    element_type,
    element_index,
    context_id,
    pageinstance_id,
    page_rank,
    arrival_rank,
    prior_context_pagetype,
    referrer,
    event_time_utc,
    event_name,
    productstyle_id,
    feature,
    context_pagetype,
    page_indicator,
    case
        when browse_value like '/browse/%' then REPLACE(browse_value, '/browse/', '')
        when browse_value like '/clearance%' then REPLACE(
            browse_value,
            '/clearance',
            'clearance'
        )
        else browse_value
    end as browse_value,
    case
        when prior_browse_value like '/browse/%' then REPLACE(prior_browse_value, '/browse/', '')
        when prior_browse_value like '/clearance%' then REPLACE(
            prior_browse_value,
            '/clearance',
            'clearance'
        )
        else prior_browse_value
    end as prior_browse_value,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    first_value(ext_int_browse_origin, true) over (
        partition by session_id,
        pageinstance_id
        order by
            rn rows between unbounded preceding
            and unbounded following
    ) as browse_page_origin,
    first_value(ext_int_browse_origin, true) over (
        partition by session_id,
        context_id
        order by
            rn rows between unbounded preceding
            and unbounded following
    ) as browse_context_origin,
    PRIOR_LAST_element,
    prior_last_context_pagetype,
    prior_last_feature,
    prior_last_element_type,
    rn
from
    (
        select
            activity_date_partition,
            session_page_rank_id,
            session_id,
            channel,
            experience,
            channelcountry,
            event_id,
            element_id,
            element_value,
            element_type,
            element_index,
            context_id,
            pageinstance_id,
            page_rank,
            arrival_rank,
            prior_context_pagetype,
            referrer,
            event_time_utc,
            event_name,
            productstyle_id,
            feature,
            context_pagetype,
            page_indicator,
            browse_value,
            prior_browse_value,
            browse_filters_key,
            browse_sortorder,
            browse_totalresultcount,
            PRIOR_LAST_element,
            prior_last_context_pagetype,
            prior_last_feature,
            prior_last_element_type,
            browse_int_origin,
            rn,
            coalesce (
                case
                    when external_signal = 1 then referrer
                end,
                browse_int_origin
            ) as ext_int_browse_origin
        from
            base_ref_origin
    );

create
or replace temp view parent_nav_rank as
select
    *,
    sum(staging) over (
        partition by session_id
        order by
            rn
    ) as original_browse_parent_rank,
    path_count - prior_path_count as var_path_count,
    case
        when path1 = prior_path1 then 9999
        else 1
    end as var_path1,
    case
        when path2 = prior_path2 then 9999
        else 2
    end as var_path2,
    case
        when path3 = prior_path3 then 9999
        else 3
    end as var_path3,
    case
        when path4 = prior_path4 then 9999
        else 4
    end as var_path4,
    case
        when path5 = prior_path5 then 9999
        else 5
    end as var_path5,
    case
        when path6 = prior_path6 then 9999
        else 6
    end as var_path6
from
    (
        select
            activity_date_partition,
            session_page_rank_id,
            session_id,
            channel,
            experience,
            channelcountry,
            event_id,
            element_id,
            element_value,
            element_type,
            element_index,
            context_id,
            pageinstance_id,
            page_rank,
            arrival_rank,
            prior_context_pagetype,
            referrer,
            event_time_utc,
            event_name,
            productstyle_id,
            feature,
            context_pagetype,
            page_indicator,
            browse_value,
            browse_filters_key,
            browse_sortorder,
            browse_totalresultcount,
            prior_browse_value,
            PRIOR_LAST_element,
            prior_last_context_pagetype,
            prior_last_feature,
            prior_last_element_type,
            browse_page_origin,
            rn,
            browse_context_origin,
            length(browse_value) - length(
                REPLACE(browse_value, '/', '')
            ) + 1 as path_count,
            split(browse_value, '/') [0] as path1,
            split(browse_value, '/') [1] as path2,
            split(browse_value, '/') [2] as path3,
            split(browse_value, '/') [3] as path4,
            split(browse_value, '/') [4] as path5,
            split(browse_value, '/') [5] as path6,
            case
                browse_value is not null
                when rn = 1 then 1
                when browse_value = prior_browse_value then 0
                when browse_value <> prior_browse_value then 1
                else null
            end as staging,
            coalesce (
                length(prior_browse_value) - length(
                    REPLACE(prior_browse_value, '/', '')
                ) + 1,
                0
            ) as prior_path_count,
            split(prior_browse_value, '/') [0] as prior_path1,
            split(prior_browse_value, '/') [1] as prior_path2,
            split(prior_browse_value, '/') [2] as prior_path3,
            split(prior_browse_value, '/') [3] as prior_path4,
            split(prior_browse_value, '/') [4] as prior_path5,
            split(prior_browse_value, '/') [5] as prior_path6
        from
            parent_path
    );

create
or replace temp view parent_original_rank as
select
    activity_date_partition,
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    event_id,
    element_id,
    element_value,
    element_type,
    element_index,
    context_id,
    pageinstance_id,
    page_rank,
    arrival_rank,
    prior_context_pagetype,
    referrer,
    event_time_utc,
    event_name,
    productstyle_id,
    feature,
    context_pagetype,
    page_indicator,
    browse_value,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    browse_context_origin,
    PRIOR_LAST_element,
    prior_last_context_pagetype,
    prior_last_feature,
    prior_last_element_type,
    browse_page_origin,
    rn,
    path_count,
    path1,
    path2,
    path3,
    path4,
    path5,
    path6,
    original_browse_parent_rank,
    sum(id_change_check) over (
        partition by (session_id)
        order by
            rn
    ) as original_browse_navigation_rank
from
    (
        select
            *,
            case
                when rn = 1 then 1
                when (
                    rn = min_rn
                    and (
                        arrival_rank is not null
                        or browse_page_origin in (
                            'Global Navigation',
                            'Shop My Store Engagement',
                            'Shop the Brand'
                        )
                    )
                )
                and incremental_check = 0
                and min_var < 10 then 1
                when rn = min_rn
                and incremental_check = 1 then 1
                else 0
            end as id_change_check
        from
            (
                select
                    *,
                    min(rn) over (
                        partition by session_id,
                        pageinstance_id
                        order by
                            rn
                    ) as min_rn,
                    least(
                        var_path1,
                        var_path2,
                        var_path3,
                        var_path4,
                        var_path5,
                        var_path6
                    ) as min_var,
                    case
                        when (
                            var_path_count >= 0
                            and least(
                                var_path1,
                                var_path2,
                                var_path3,
                                var_path4,
                                var_path5,
                                var_path6
                            ) >= prior_path_count
                        )
                        or (
                            var_path_count < 0
                            and least(
                                var_path1,
                                var_path2,
                                var_path3,
                                var_path4,
                                var_path5,
                                var_path6
                            ) > path_count
                        )
                        or least(
                            var_path1,
                            var_path2,
                            var_path3,
                            var_path4,
                            var_path5,
                            var_path6
                        ) = 9999 then 0
                        else 1
                    end as incremental_check
                from
                    parent_nav_rank
            )
    );

create
or replace temp view final as
select
    activity_date_partition,
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    event_id,
    element_id,
    element_value,
    element_type,
    element_index,
    context_id,
    pageinstance_id,
    page_rank,
    arrival_rank,
    prior_context_pagetype,
    referrer,
    event_time_utc,
    event_name,
    productstyle_id,
    feature,
    context_pagetype,
    page_indicator,
    browse_value,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    browse_page_origin,
    browse_context_origin,
    PRIOR_LAST_element,
    prior_last_context_pagetype,
    prior_last_feature,
    prior_last_element_type,
    path_count,
    path1,
    path2,
    path3,
    path4,
    path5,
    path6,
    original_browse_parent_rank,
    dense_rank () over (
        partition by session_id
        order by
            browse_parent_rank
    ) as browse_parent_rank,
    original_browse_navigation_rank,
    dense_rank () over (
        partition by session_id
        order by
            browse_navigation_rank
    ) as browse_navigation_rank
from
    (
        select
            activity_date_partition,
            session_page_rank_id,
            session_id,
            channel,
            experience,
            channelcountry,
            event_id,
            element_id,
            element_value,
            element_type,
            element_index,
            context_id,
            pageinstance_id,
            page_rank,
            arrival_rank,
            prior_context_pagetype,
            referrer,
            event_time_utc,
            event_name,
            productstyle_id,
            feature,
            context_pagetype,
            page_indicator,
            browse_value,
            browse_filters_key,
            browse_sortorder,
            browse_totalresultcount,
            browse_page_origin,
            browse_context_origin,
            rn,
            PRIOR_LAST_element,
            prior_last_context_pagetype,
            prior_last_feature,
            prior_last_element_type,
            path_count,
            path1,
            path2,
            path3,
            path4,
            path5,
            path6,
            original_browse_parent_rank,
            first_value(original_browse_parent_rank) over (
                partition by session_id,
                context_id
                order by
                    rn rows between unbounded preceding
                    and unbounded following
            ) as browse_parent_rank,
            original_browse_navigation_rank,
            first_value(original_browse_navigation_rank) over (
                partition by session_id,
                context_id
                order by
                    rn rows between unbounded preceding
                    and unbounded following
            ) as browse_navigation_rank
        from
            parent_original_rank
    ) ranked;

create
or replace temp view output as
select
    activity_date_partition,
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    event_id,
    element_id,
    element_value,
    element_type,
    element_index,
    context_id,
    pageinstance_id,
    page_rank,
    arrival_rank,
    prior_context_pagetype,
    referrer,
    from_utc_timestamp(event_time_utc, 'US/Pacific') as event_time_pst,
    event_name,
    feature,
    context_pagetype,
    page_indicator,
    browse_value,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    browse_page_origin,
    browse_context_origin,
    PRIOR_LAST_element,
    prior_last_context_pagetype,
    prior_last_feature,
    prior_last_element_type,
    path_count,
    path1,
    path2,
    path3,
    path4,
    path5,
    path6,
    original_browse_parent_rank,
    browse_parent_rank,
    concat(session_id, cast(browse_parent_rank as string)) as browse_parent_id,
    original_browse_navigation_rank,
    browse_navigation_rank,
    concat(
        session_id,
        cast(browse_navigation_rank as string)
    ) as browse_navigation_id,
    productstyle_id,
    atb_ct,
    atb_units,
    order_ct,
    order_units
from
    final
    left join (
        select
            session_id as atb_session_id,
            atb_style_id,
            count(distinct event_id) as atb_ct,
            sum(atb_current_units) as atb_units
        from
            acp_event_intermediate.session_evt_expanded_attributes_parquet
        where
            event_name = 'com.nordstrom.customer.AddedToBag'
            and atb_flag = 1
            and activity_date_partition between {start_date}
            and {end_date}
        group by
            1,
            2
    ) atb on final.productstyle_id = atb.atb_style_id
    and final.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
    and final.session_id = atb.atb_session_id
    left join (
        select
            session_id as order_session_id,
            order_style_id,
            count(distinct orderlineid) as order_ct,
            sum(order_line_current_units) as order_units
        from
            acp_event_intermediate.session_evt_expanded_attributes_parquet
        where
            event_name = 'com.nordstrom.customer.OrderSubmitted'
            and activity_date_partition between {start_date}
            and {end_date}
        group by
            1,
            2
    ) ord on final.productstyle_id = ord.order_style_id
    and final.event_name = 'com.nordstrom.event.customer.ProductSummarySelected'
    and final.session_id = ord.order_session_id;

CREATE TABLE if not exists {hive_schema}.browse_session_analytical (
    session_page_rank_id string,
    session_id string,
    channel string,
    experience string,
    channelcountry string,
    context_id string,
    event_id string,
    event_time_pst timestamp,
    event_name string,
    feature string,
    arrival_rank bigint,
    pageinstance_id string,
    context_pagetype string,
    page_rank bigint,
    page_indicator string,
    prior_context_pagetype string,
    element_id string,
    element_value string,
    element_type string,
    element_index bigint,
    referrer string,
    browse_filters_key string,
    browse_sortorder string,
    browse_totalresultcount bigint,
    browse_value string,
    browse_page_origin string,
    browse_context_origin string,
    path_count bigint,
    path1 string,
    path2 string,
    path3 string,
    path4 string,
    path5 string,
    path6 string,
    original_browse_parent_rank bigint,
    browse_parent_rank bigint,
    browse_parent_id string,
    original_browse_navigation_rank bigint,
    browse_navigation_rank bigint,
    browse_navigation_id string,
    productstyle_id string,
    atb_ct bigint,
    atb_units float,
    order_ct bigint,
    order_units float,
    activity_date_partition date
) using ORC location "s3://{s3_bucket_root_var}/browse_session_analytical" partitioned BY (activity_date_partition);

msck repair table {hive_schema}.browse_session_analytical;

insert
    OVERWRITE TABLE {hive_schema}.browse_session_analytical PARTITION (activity_date_partition)
select
    session_page_rank_id,
    session_id,
    channel,
    experience,
    channelcountry,
    context_id,
    event_id,
    event_time_pst,
    event_name,
    feature,
    arrival_rank,
    pageinstance_id,
    context_pagetype,
    page_rank,
    page_indicator,
    prior_context_pagetype,
    element_id,
    element_value,
    element_type,
    element_index,
    referrer,
    browse_filters_key,
    browse_sortorder,
    browse_totalresultcount,
    browse_value,
    browse_page_origin,
    browse_context_origin,
    path_count,
    path1,
    path2,
    path3,
    path4,
    path5,
    path6,
    original_browse_parent_rank,
    browse_parent_rank,
    browse_parent_id,
    original_browse_navigation_rank,
    browse_navigation_rank,
    browse_navigation_id,
    productstyle_id,
    atb_ct,
    atb_units,
    order_ct,
    order_units,
    activity_date_partition
from
    output;
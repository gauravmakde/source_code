/******************************************************************************
Name: Scaled Events - CYBER View Definitions
APPID-Name: APP08204 - Scaled Events Reporting
Purpose: Store all view definitions for Cyber Reporting (both prep and tableau view code)
Variable(s):    {environment_schema} - T2DL_DAS_SCALED_EVENTS
DAG: merch_se_all_views
TABLE NAME: Multiple Views
Author(s): Manuela Hurtado Gonzalez, Alli Moore
Date Last Updated: September 2023
******************************************************************************/



/******************************************************** START - PREP VIEWS ********************************************************/

/******************************************************************************
** FILE: ma_pipeline_prod_main_se_cyb_prep_sales_plan.sql
** TABLE: {environment_schema}.cyber_sales_plan_23
**
** DESCRIPTION:
**      - View that contains the financial sales plans. This data is provided by business and is static based upon a T3 Data upload table
**      - PARAMS:
**          - environment_schema = Schema data will reside in
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS
**
******************************************************************************/


/******************************************************** START - TABLEAU VIEWS ********************************************************/

/******************************************************************************
** FILE: cyber_item_daily_vw.sql
** TABLE: T2DL_DAS_SCALED_EVENTS.cyber_item_daily_vw
**
** DESCRIPTION:
**      - Source of Cyber Item Daily Tableau data source - TY Only, and UDIG-captured items only
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS
**
** AUTHOR(s):
**      - Manuela Hurtado Gonzalez
**      - Alli Moore
******************************************************************************/

REPLACE VIEW {environment_schema}.cyber_item_daily_vw
AS
LOCK ROW FOR ACCESS
SELECT
    day_idnt
    , day_dt
    , event_day
    , month_idnt
    , month_label
    , ty_ly_ind
    , price_type
    , channel
    , banner
    , country
    , style_num
    , style_desc
    , vpn
    , supp_color
    , style_group_idnt
    , division
    , subdivision
    , department
    , "class"
    , subclass
    , supplier
    , brand
    , holiday_theme
    , stocking_stuffer
    , quantrix_category
    , assortment_grouping
    , ccs_category
    , ccs_subcategory
    , nord_role_desc
    , rack_role_desc
    , promo_grouping
    , merch_theme
    , price_band_one                        AS price_band_1
    , price_band_two                        AS price_band_2
    , MAX(rp_ind                         )  AS rp_ind
    , MAX(npg_ind                        )  AS npg_ind
    , MAX(dropship_ind                   )  AS dropship_ind
    , MAX(price_match_ind                )  AS price_match_ind
    , MAX(udig                           )  AS udig_main
    , MAX(gift_ind                       )  AS gift_ind
    , MAX(bipoc_ind                      )  AS bipoc_ind
    , MAX(anchor_brands                  )  AS anchor_brands_ind
    , SUM(sales_units                    )  AS sales_units
    , SUM(sales_dollars                  )  AS sales_dollars
    , SUM(sales_cost                     )  AS sales_cost
    , SUM(return_units                   )  AS return_units
    , SUM(return_dollars                 )  AS return_dollars
    , SUM(return_cost                    )  AS return_cost
    , SUM(ntn                            )  AS ntn
    , SUM(demand_units                   )  AS demand_units
    , SUM(demand_dollars                 )  AS demand_dollars
    , SUM(demand_cancel_units         )  AS demand_cancel_units
    , SUM(demand_cancel_dollars       )  AS demand_cancel_dollars
    , SUM(shipped_units                  )  AS shipped_units
    , SUM(shipped_dollars                )  AS shipped_dollars
    , SUM(shipped_cost                   )  AS shipped_cost
    , SUM(eoh_units                      )  AS eoh_units
    , SUM(eoh_dollars                    )  AS eoh_dollars
    , SUM(eoh_cost                       )  AS eoh_cost
    , SUM(boh_units                      )  AS boh_units
    , SUM(boh_dollars                    )  AS boh_dollars
    , SUM(boh_cost                       )  AS boh_cost
    , SUM(nonsellable_units              )  AS nonsellable_units
    , SUM(receipt_units                  )  AS receipt_units
    , SUM(receipt_dollars                )  AS receipt_dollars
    , SUM(receipt_cost                   )  AS receipt_cost
    , SUM(store_fulfill_units            )  AS store_fulfill_units
    , SUM(store_fulfill_dollars          )  AS store_fulfill_dollars
    , SUM(store_fulfill_cost             )  AS store_fulfill_cost
    , SUM(dropship_units                 )  AS dropship_units
    , SUM(dropship_dollars               )  AS dropship_dollars
    , SUM(dropship_cost                  )  AS dropship_cost
    , SUM(demand_dropship_units          )  AS demand_dropship_units
    , SUM(demand_dropship_dollars        )  AS demand_dropship_dollars
    , SUM(receipt_dropship_units         )  AS receipt_dropship_units
    , SUM(receipt_dropship_dollars       )  AS receipt_dropship_dollars
    , SUM(receipt_dropship_cost          )  AS receipt_dropship_cost
    , SUM(on_order_units                 )  AS on_order_units
    , SUM(on_order_retail_dollars        )  AS on_order_retail_dollars
    , SUM(on_order_cost_dollars          )  AS on_order_cost_dollars
--    , SUM(on_order_4wk_units             )  AS on_order_4wk_units
--    , SUM(on_order_4wk_retail_dollars    )  AS on_order_4wk_retail_dollars
    , SUM(product_views                  )  AS product_views
    , SUM(cart_adds                      )  AS cart_adds
    , SUM(order_units                    )  AS order_units
    , SUM(instock_views                  )  AS instock_views
    , SUM(wishlist_adds               )  AS wishlist_adds
    , SUM(cogs                        )  AS cogs
    , SUM(sales_pm                    )  AS sales_pm
    , SUM(scored_views                )  AS scored_views
FROM {environment_schema}.scaled_event_sku_clk_rms
WHERE date_event_type = 'Cyber'
    AND ty_ly_ind = 'TY'
    AND udig IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34;
GRANT SELECT ON {environment_schema}.cyber_item_daily_vw TO PUBLIC WITH GRANT OPTION;

/******************************************************************************
** FILE: cyber_supplier_class_daily_vw.sql
** TABLE: T2DL_DAS_SCALED_EVENTS.cyber_supplier_class_daily_vw
**
** DESCRIPTION:
**      - Source of Cyber Supplier Class Daily Tableau data source
**
** TO DO:
**      - Productionalize in T2DL_DAS_SCALED_EVENTS
**
** AUTHOR(s):
**      - Manuela Hurtado Gonzalez
**      - Alli Moore
******************************************************************************/

REPLACE VIEW {environment_schema}.cyber_supplier_class_daily_vw -- UPDATE TO REPLACE AFTER DEV
AS
LOCK ROW FOR ACCESS
SELECT
    bs.day_idnt
    , bs.day_dt
    , bs.event_day
    , bs.day_dt_aligned
    , bs.month_idnt
    , bs.month_label
    , bs.ty_ly_ind
    , bs.banner
    , bs.price_type
    , bs.channel
    , bs.country
    , bs.division
    , bs.subdivision
    , bs.department
    , bs."class"
    , bs.subclass
    , bs.brand
    , bs.supplier
    , bs.holiday_theme
    , bs.stocking_stuffer
    , bs.quantrix_category
    , bs.assortment_grouping
    , bs.climate_cluster
    --, bs.NR_price_cluster
    , bs.ccs_category
    , bs.ccs_subcategory
    , bs.nord_role_desc
    , bs.rack_role_desc
    , bs.promo_grouping
    , bs.merch_theme
    , bs.rp_ind
    , bs.npg_ind
    , bs.dropship_ind
    , bs.gift_ind
    , bs.bipoc_ind
    , bs.anchor_brands
    , bs.price_band_1
    , bs.price_band_2
    , bs.price_match_ind
    , bs.udig_main
    , ZEROIFNULL(
    CASE
        WHEN bs.ty_ly_ind = 'TY' THEN pl.TY_SALES_PLAN
        WHEN bs.ty_ly_ind = 'LY' THEN pl.LY_SALES_PLAN
        ELSE NULL
    END) AS sales_plan
    , ntn
    , sales_units
    , sales_dollars
    , sales_cost
    , return_units
    , return_dollars
    , return_cost
    , demand_units
    , demand_dollars
    , demand_cancel_units
    , demand_cancel_dollars
    , receipt_units
    , receipt_dollars
    , receipt_cost
    , shipped_units
    , shipped_dollars
    , shipped_cost
    , store_fulfill_units
    , store_fulfill_dollars
    , store_fulfill_cost
    , dropship_units
    , dropship_dollars
    , dropship_cost
    , demand_dropship_units
    , demand_dropship_dollars
    , receipt_dropship_units
    , receipt_dropship_dollars
    , receipt_dropship_cost
    , on_order_units
    , on_order_retail_dollars
    , on_order_cost_dollars
--    , on_order_4wk_units
--    , on_order_4wk_retail_dollars
    , product_views
    , cart_adds
    , order_units
    , instock_views
    , boh_units
    , boh_dollars
    , boh_cost
    , eoh_units
    , eoh_dollars
    , eoh_cost
    , nonsellable_units
    , wishlist_adds
    , cogs
    , sales_pm
    , scored_views
FROM (
    SELECT
        bs.day_idnt
        , bs.day_dt
        , bs.event_day
        , bs.day_dt_aligned
        , bs.month_idnt
        , bs.month_label
        , bs.ty_ly_ind
        , bs.banner
        , bs.price_type
        , bs.channel
        , bs.country
        , bs.division
        , bs.subdivision
        , bs.department
        , bs."class"
        , bs.subclass
        , bs.brand
        , bs.supplier
        , bs.holiday_theme
        , bs.stocking_stuffer
        , bs.quantrix_category
        , bs.assortment_grouping
        , bs.climate_cluster
        --, bs.NR_price_cluster
        , bs.ccs_category
        , bs.ccs_subcategory
        , bs.nord_role_desc
        , bs.rack_role_desc
        , bs.promo_grouping
        , bs.merch_theme
        , bs.rp_ind
        , bs.npg_ind
        , bs.dropship_ind
        , bs.gift_ind
        , bs.bipoc_ind
        , bs.anchor_brands
        , bs.price_band_one as price_band_1
        , bs.price_band_two as price_band_2
        , bs.price_match_ind
        , bs.udig AS udig_main
        , SUM(bs.ntn                            ) AS ntn
        , SUM(bs.sales_units                    ) AS sales_units
        , SUM(bs.sales_dollars                  ) AS sales_dollars
        , SUM(bs.sales_cost                     ) AS sales_cost
        , SUM(bs.return_units                   ) AS return_units
        , SUM(bs.return_dollars                 ) AS return_dollars
        , SUM(bs.return_cost                    ) AS return_cost
        , SUM(bs.demand_units                   ) AS demand_units
        , SUM(bs.demand_dollars                 ) AS demand_dollars
        , SUM(bs.demand_cancel_units            ) AS demand_cancel_units
        , SUM(bs.demand_cancel_dollars          ) AS demand_cancel_dollars
        , SUM(bs.receipt_units                  ) AS receipt_units
        , SUM(bs.receipt_dollars                ) AS receipt_dollars
        , SUM(bs.receipt_cost                   ) AS receipt_cost
        , SUM(bs.shipped_units                  ) AS shipped_units
        , SUM(bs.shipped_dollars                ) AS shipped_dollars
        , SUM(bs.shipped_cost                   ) AS shipped_cost
        , SUM(bs.store_fulfill_units            ) AS store_fulfill_units
        , SUM(bs.store_fulfill_dollars          ) AS store_fulfill_dollars
        , SUM(bs.store_fulfill_cost             ) AS store_fulfill_cost
        , SUM(bs.dropship_units                 ) AS dropship_units
        , SUM(bs.dropship_dollars               ) AS dropship_dollars
        , SUM(bs.dropship_cost                  ) AS dropship_cost
        , SUM(bs.demand_dropship_units          ) AS demand_dropship_units
        , SUM(bs.demand_dropship_dollars        ) AS demand_dropship_dollars
        , SUM(bs.receipt_dropship_units         ) AS receipt_dropship_units
        , SUM(bs.receipt_dropship_dollars       ) AS receipt_dropship_dollars
        , SUM(bs.receipt_dropship_cost          ) AS receipt_dropship_cost
        , SUM(bs.on_order_units                 ) AS on_order_units
        , SUM(bs.on_order_retail_dollars        ) AS on_order_retail_dollars
        , SUM(bs.on_order_cost_dollars          ) AS on_order_cost_dollars
--        , SUM(bs.on_order_4wk_units             ) AS on_order_4wk_units
--        , SUM(bs.on_order_4wk_retail_dollars    ) AS on_order_4wk_retail_dollars
        , SUM(bs.product_views                  ) AS product_views
        , SUM(bs.cart_adds                      ) AS cart_adds
        , SUM(bs.order_units                    ) AS order_units
        , SUM(bs.instock_views                  ) AS instock_views
        , SUM(bs.boh_units                      ) AS boh_units
        , SUM(bs.boh_dollars                    ) AS boh_dollars
        , SUM(bs.boh_cost                       ) AS boh_cost
        , SUM(bs.eoh_units                      ) AS eoh_units
        , SUM(bs.eoh_dollars                    ) AS eoh_dollars
        , SUM(bs.eoh_cost                       ) AS eoh_cost
        , SUM(bs.nonsellable_units              ) AS nonsellable_units
        , SUM(bs.wishlist_adds                  ) AS wishlist_adds
        , SUM(bs.cogs                           ) AS cogs
        , SUM(bs.sales_pm                       ) AS sales_pm
        , SUM(bs.scored_views                   ) AS scored_views
    FROM
        (SELECT *
         FROM {environment_schema}.scaled_event_sku_clk_rms bs
         WHERE date_event_type = 'Cyber'
            AND day_dt_aligned <= (select max(day_dt) from {environment_schema}.scaled_event_sku_clk_rms)) bs
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
) bs
-- sales plan
LEFT JOIN (
    SELECT
      cast(CHANNEL as varchar(3)) as channel,
      DEPT_IDNT AS DEPT_NUM,
      SUM(CASE WHEN PLAN_YEAR = 2024 THEN PLAN_NET_SLS_R END) AS TY_SALES_PLAN,
      SUM(CASE WHEN PLAN_YEAR = 2023 THEN PLAN_NET_SLS_R END) AS LY_SALES_PLAN
    FROM T2DL_DAS_SCALED_EVENTS.CYBER_COST_CAT_PLANS
    WHERE plan_type = 'CYBER'
    GROUP BY 1,2 ) pl
ON bs.channel = pl.channel
AND TRIM(STRTOK(bs.department,', ', 1)) = pl.DEPT_NUM;

GRANT SELECT ON {environment_schema}.cyber_supplier_class_daily_vw TO PUBLIC WITH GRANT OPTION;

/******************************************************** END - TABLEAU VIEWS ********************************************************/

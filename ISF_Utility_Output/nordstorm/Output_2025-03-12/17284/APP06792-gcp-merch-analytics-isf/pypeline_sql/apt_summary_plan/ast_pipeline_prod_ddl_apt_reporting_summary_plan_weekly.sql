/*
APT Summary Report DDL
Author: Jihyun Yu
Date Created: 2/6/23

Datalab: t2dl_das_apt_cost_reporting

Tables:
    {environment_schema}.plan_summary_category_weekly{env_suffix}
    {environment_schema}.plan_summary_suppliergroup_weekly{env_suffix}
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'plan_summary_category_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.plan_summary_category_weekly{env_suffix}, FALLBACK,
    NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
    fiscal_year_num                 INTEGER NOT NULL
    , quarter_label                 VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC
    , month_idnt                    INTEGER NOT NULL
    , month_label                   VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    , month_454_label               VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , week_idnt                     INTEGER NOT NULL
    , week_label                    VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC
    , week_454_label                VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC
    , week_start_day_date           DATE FORMAT 'YYYY-MM-DD'
    , week_end_day_date             DATE FORMAT 'YYYY-MM-DD'
    , country                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US', 'CA')
    , currency_code                 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    , banner                        VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    , chnl_idnt                     INTEGER
    , fulfill_type_num              INTEGER NOT NULL
    , category                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , price_band                    VARCHAR(40) CHARACTER SET UNICODE NOT CASESPECIFIC
    , division_label                VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subdivision_label             VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dept_idnt                     INTEGER
    , dept_label                    VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , category_planner_1            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('OUTDOOR', 'BATHROOM', 'OTHER', 'KITCHEN', 'CASUAL', 'BEDROOM', 'PET', 'CRIB', 'FINE BIG BET', 'GIRS', 'DRESS', 'MENS BIG BET', 'DINING ROOM', 'BOYS', 'TOYS & GEAR', 'LIVING ROOM', 'HOME ORG & CARE', 'HANDBAGS BIG BET', 'HANGING')
    , category_planner_2            VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('SANDALS', 'HATS', 'OUTERWEAR', 'BOTTOMS', 'OTHER', 'DRESSES', 'JACKETS', 'CASUAL', 'SPORT COATS', 'INFANT', 'DRESS', 'TOPS', 'FLATS', 'BIG', 'SNEAKERS', 'BOOTS', 'SWIM', 'PUMPS', 'SUITS', 'LITTLE', 'MENS CATEGORIES')
    , category_group                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , seasonal_designation          VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('COLD', 'WARM')
    , rack_merch_zone               VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('MENS', 'WOMENS SHOES', 'LUGGAGE', 'MENS SHOES', 'MENS APPAREL', 'HOME', 'QUEUE', 'HANDBAGS', 'MENS ACC', 'SOFT ACC', 'JEWELRY/WATCHES', 'LINGERIE/SLEEP', 'KIDS SHOES', 'HOISERY', 'WOMENS APPAREL', 'BEAUTY', 'KIDS', 'SUN', 'DESIGNER')
    , is_activewear                 VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('ACTIVE')
    , Nordstrom_CCS_Roles           VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Focused Essential', 'Destination', 'Traffic Driver', 'Signature Statement')
    , Rack_CCS_Roles                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('Focused Essential', 'Destination', 'Traffic Driver', 'Signature Statement')
    , channel_name                  VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC
    , demand_units                  DECIMAL(38,8)
    , demand_r_dollars              DECIMAL(38,13)
    , gross_sls_units               DECIMAL(38,8)
    , gross_sls_r_dollars           DECIMAL(38,13)
    , return_units                  DECIMAL(38,8)
    , return_r_dollars              DECIMAL(38,13)
    , net_sls_units                 DECIMAL(38,8)
    , net_sls_r_dollars             DECIMAL(38,13)
    , net_sls_c_dollars             DECIMAL(38,13)
    , avg_inv_ttl_c                 DECIMAL(38,13)
    , avg_inv_ttl_u                 DECIMAL(38,8)
    , rcpt_need_r                   DECIMAL(38,4)
    , rcpt_need_c                   DECIMAL(38,4)
    , rcpt_need_u                   INTEGER
    , rcpt_need_lr_r                DECIMAL(38,4)
    , rcpt_need_lr_c                DECIMAL(38,4)
    , rcpt_need_lr_u                INTEGER
    , plan_bop_c$                   DECIMAL(38,2)
    , plan_bop_u                    INTEGER
    , beginofmonth_bop_c            DECIMAL(38,2)
    , beginofmonth_bop_u            INTEGER
    , endofmonth_bop_c              DECIMAL(38,2)
    , endofmonth_bop_u              INTEGER
    , ly_sales_c$                   DECIMAL(38,4)
    , ly_sales_r$                   DECIMAL(38,4)
    , ly_sales_u                    INTEGER
    , ly_gross_r$                   DECIMAL(38,4)
    , ly_gross_u                    INTEGER
    , ly_demand_r$                  DECIMAL(38,6)
    , ly_demand_u                   INTEGER
    , ly_bop_c$                     DECIMAL(38,2)
    , ly_bop_u                      INTEGER
    , ly_bom_bop_c$                 DECIMAL(38,2)
    , ly_bom_bop_u                  INTEGER
    , ly_rcpt_need_c$               DECIMAL(38,4)
    , ly_rcpt_need_u                INTEGER
    , cp_bop_ttl_c_dollars          DECIMAL(38,4)
    , cp_bop_ttl_u                  DECIMAL(38,4)
    , cp_beginofmonth_bop_c         DECIMAL(38,4)
    , cp_beginofmonth_bop_u         DECIMAL(38,4)
    , cp_eop_ttl_c_dollars          DECIMAL(38,4)
    , cp_eop_ttl_u                  DECIMAL(38,4)
    , cp_eop_eom_c_dollars          DECIMAL(38,4)
    , cp_eop_eom_u                  DECIMAL(38,4)
    , cp_rcpt_need_c_dollars        DECIMAL(38,4)
    , cp_rcpt_need_u                DECIMAL(38,4)
    , cp_rcpt_need_lr_c_dollars     DECIMAL(38,4)
    , cp_rcpt_need_lr_u             DECIMAL(38,4)
    , cp_demand_r_dollars           DECIMAL(38,4)
    , cp_demand_u                   DECIMAL(38,4)
    , cp_gross_sls_r_dollars        DECIMAL(38,4)
    , cp_gross_sls_u                DECIMAL(38,4)
    , cp_return_r_dollars           DECIMAL(38,4)
    , cp_return_u                   DECIMAL(38,4)
    , cp_net_sls_r_dollars          DECIMAL(38,4)
    , cp_net_sls_c_dollars          DECIMAL(38,4)
    , cp_net_sls_u                  DECIMAL(38,4)
    , update_timestamp              TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, chnl_idnt, dept_idnt, category, fulfill_type_num, price_band)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '7' DAY , NO RANGE)
;

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'plan_summary_suppliergroup_weekly{env_suffix}', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.plan_summary_suppliergroup_weekly{env_suffix}, FALLBACK,
    NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
    fiscal_year_num                 INTEGER NOT NULL
    , quarter_label                 VARCHAR(7) CHARACTER SET UNICODE NOT CASESPECIFIC
    , month_idnt                    INTEGER NOT NULL
    , month_label                   VARCHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC
    , month_454_label               VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    , month_start_day_date          DATE FORMAT 'YYYY-MM-DD'
    , month_end_day_date            DATE FORMAT 'YYYY-MM-DD'
    , week_idnt                     INTEGER NOT NULL
    , week_label                    VARCHAR(17) CHARACTER SET UNICODE NOT CASESPECIFIC
    , week_454_label                VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC
    , week_start_day_date           DATE FORMAT 'YYYY-MM-DD'
    , week_end_day_date             DATE FORMAT 'YYYY-MM-DD'
    , country                       VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('US', 'CA')
    , currency_code                 VARCHAR(3) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('USD','CAD')
    , banner                        VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('NORDSTROM', 'NORDSTROM_RACK')
    , chnl_idnt                     INTEGER
    , fulfill_type_num              INTEGER NOT NULL
    , alternate_inventory_model     VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('DROPSHIP', 'OWN')
    , category                      VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , division_label                VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , subdivision_label             VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , dept_idnt                     INTEGER
    , dept_label                    VARCHAR(163) CHARACTER SET UNICODE NOT CASESPECIFIC
    , supplier_group                VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    , buy_planner                   VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , preferred_partner_desc        VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    , areas_of_responsibility       VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC
    , is_npg                        VARCHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('N', 'Y')
    , diversity_group               VARCHAR(15) CHARACTER SET UNICODE NOT CASESPECIFIC COMPRESS ('LATIN X BLACK', 'DIB BRAND', 'LATIN X', 'BLACK', 'ASIAN', 'DIB')
    , nord_to_rack_transfer_rate    VARCHAR(25) CHARACTER SET UNICODE NOT CASESPECIFIC
    , demand_units                  DECIMAL(38,8)
    , demand_r_dollars              DECIMAL(38,13)
    , gross_sls_units               DECIMAL(38,8)
    , gross_sls_r_dollars           DECIMAL(38,13)
    , return_units                  DECIMAL(38,8)
    , return_r_dollars              DECIMAL(38,13)
    , net_sls_units                 DECIMAL(38,8)
    , net_sls_r_dollars             DECIMAL(38,13)
    , net_sls_c_dollars             DECIMAL(38,13)
    , avg_inv_ttl_c                 DECIMAL(38,13)
    , avg_inv_ttl_u                 DECIMAL(38,8)
    , plan_bop_c_dollars            DECIMAL(38,4)
    , plan_bop_c_units              DECIMAL(38,4)
    , plan_eop_c_dollars            DECIMAL(38,4)
    , plan_eop_c_units              DECIMAL(38,4)
    , rcpt_need_r                   DECIMAL(38,4)
    , rcpt_need_c                   DECIMAL(38,4)
    , rcpt_need_u                   INTEGER
    , rcpt_need_lr_r                DECIMAL(38,4)
    , rcpt_need_lr_c                DECIMAL(38,4)
    , rcpt_need_lr_u                INTEGER
    , beginofmonth_bop_c            DECIMAL(38,2)
    , beginofmonth_bop_u            INTEGER
    , ly_sales_c$                   DECIMAL(38,4)
    , ly_sales_r$                   DECIMAL(38,4)
    , ly_sales_u                    INTEGER
    , ly_gross_r$                   DECIMAL(38,4)
    , ly_gross_u                    INTEGER
    , ly_demand_r$                  DECIMAL(38,6)
    , ly_demand_u                   INTEGER
    , ly_bop_c$                     DECIMAL(38,2)
    , ly_bop_u                      INTEGER
    , ly_bom_bop_c$                 DECIMAL(38,2)
    , ly_bom_bop_u                  INTEGER
    , ly_rcpt_need_c$               DECIMAL(38,4)
    , ly_rcpt_need_u                INTEGER
    , update_timestamp              TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, chnl_idnt, dept_idnt, category, fulfill_type_num, supplier_group)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '7' DAY , NO RANGE)
;

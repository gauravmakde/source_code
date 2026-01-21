CREATE OR REPLACE TEMPORARY VIEW nmn_head_brands
(
head_brands string,
brand_id string
 )
USING CSV
OPTIONS (
    path "s3://mim-production/NMN/Head_brands.csv",
    sep ",",
    header "true"
)
;

INSERT OVERWRITE TABLE nmn_head_brands_ldg_output 

SELECT
    head_brands,
    brand_id
FROM nmn_head_brands
WHERE 1=1
;

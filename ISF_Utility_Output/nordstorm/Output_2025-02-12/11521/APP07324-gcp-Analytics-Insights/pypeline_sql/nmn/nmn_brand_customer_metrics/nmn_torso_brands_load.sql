CREATE OR REPLACE TEMPORARY VIEW nmn_torso_brands
(
torso_brands string,
brand_id string
 )
USING CSV
OPTIONS (
    path "s3://mim-production/NMN/Torso_Brands.csv",
    sep ",",
    header "true"
)
;

INSERT OVERWRITE TABLE nmn_torso_brands_ldg_output 

SELECT
    torso_brands,
    brand_id
FROM nmn_torso_brands
WHERE 1=1
;

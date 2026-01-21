CREATE TEMPORARY TABLE IF NOT EXISTS sku_dim_prep
CLUSTER BY sku_id
AS
SELECT DISTINCT sk.rms_sku_num AS sku_id,
 sk.epm_style_num,
 sk.sku_desc,
 sk.style_desc,
 sk.brand_name,
 sk.div_desc,
 sk.dept_desc,
 sk.class_desc,
 sk.sbclass_desc,
  CASE
  WHEN REGEXP_INSTR(sk.div_desc, 'WOMEN|WEAR|SPEC|MN/KID') > 0
  THEN 'APPAREL'
  WHEN REGEXP_INSTR(sk.div_desc, 'SHOE') > 0
  THEN 'SHOES'
  WHEN REGEXP_INSTR(sk.div_desc, 'ACC') > 0
  THEN 'ACCESSORIES'
  WHEN sk.div_desc IS NULL
  THEN 'OTHER'
  ELSE REPLACE(sk.div_desc, 'INACT-|INACT -', '')
  END AS merch_division,
 REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(sk
              .dept_desc, 'OTHER'), 'YNG ', 'YOUNG '), 'WMS |WMNS |WM ', 'WOMENS '), 'BTR |BET ', 'BETTER '),
          ' BTR| BET', ' BETTER '), 'DSNR', 'DESIGNER'), 'SPTWR', 'SPORTSWEAR '), 'HDBGS|HDBG', 'HANDBAGS '), 'MID CRR'
      ,'MID CAREER'), 'ACT ', 'ACTIVE '), ' APP', ' APPAREL'), '/LUG', '/LUGGAGE'), 'BET ', 'BETTER ') AS
 merch_department,
 REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CASE
                              WHEN LOWER(sk.class_desc) IN (LOWER('DRESSES'), LOWER('SNEAKERS'), LOWER('DENIM PANTS'),
                                LOWER('KNIT TOPS'), LOWER('BOOTS SHORT'), LOWER('SNEAKERS'), LOWER('GIFT SETS'), LOWER('SANDALS'
                                 ), LOWER('FRAGRANCE'), LOWER('CROSSBODY'), LOWER('WM NONPZD SUNGL'), LOWER('CROSSBODY'
                                 ), LOWER('TOTE'), LOWER('THROWS'), LOWER('HOME DECOR'), LOWER('SHEETS'), LOWER('HANDHELD'
                                 ), LOWER('EARRINGS'), LOWER('HOME DECOR'), LOWER('TABLETOP'), LOWER('TOTE'), LOWER('CASUAL PANTS'
                                 ), LOWER('DENIM'), LOWER('S/S WOVENS'), LOWER('SNEAKERS'), LOWER('KNIT TOPS'))
                              THEN sk.class_desc
                              ELSE 'OTHER'
                              END, 'ACTIV ', 'ACTIVE'), 'ACCESSORY', 'ACCESSORIES'), ' ACC ', ' ACCESSORIES '), 'BOOT '
                          ,'BOOTS '), 'BOY ', 'BOYS '), 'DRSS', 'DRESSES'), 'ELECTRNC', 'ELECTRONICS'), ' JWL ',
                      ' JEWELRY '), 'GIRL |GRL ', 'GIRLS '), ' GRL', ' GIRLS'), 'ORGANIZATN', 'ORGANIZATION'), 'INF ',
                  'INFANTS '), 'SANDAL |SNDL ', 'SANDALS '), 'SNKR', 'SNEAKERS'), ' LIT | LITTL ', ' LITTLE '), 'TDLR',
              'TODDLER'), 'YNG ', 'YOUNG '), 'WMS |WMNS |WM ', 'WOMENS '), 'BTR |BET ', 'BETTER '), ' BTR| BET',
          ' BETTER '), 'DSNR', 'DESIGNER'), 'SPTWR', 'SPORTSWEAR '), 'HDBGS|HDBG', 'HANDBAGS '), 'MID CRR', 'MID CAREER'
      ), 'ACT ', 'ACTIVE '), ' APP', ' APPAREL'), '/LUG', '/LUGGAGE'), 'BET ', 'BETTER ') AS merch_class,
  CASE
  WHEN LOWER(sk.brand_name) IN (LOWER('UGG'), LOWER('NIKE'), LOWER('SAINT LAURENT'), LOWER('GUCCI'), LOWER('ZELLA'),
    LOWER('FREE PEOPLE'), LOWER('TORY BURCH'), LOWER('ADIDAS'), LOWER('NORDSTROM'), LOWER('TORY BURCH'), LOWER('MAC DUGGAL'
     ), LOWER('MADEWELL'), LOWER('NIKE'), LOWER('CHANEL'), LOWER('VINCE'), LOWER('TED BAKER LONDON'), LOWER('LA FEMME')
    , LOWER('THE NORTH FACE'), LOWER('RAG AND BONE'), LOWER('AG'), LOWER('FRAME'), LOWER('DAVID YURMAN'), LOWER('PAIGE'
     ), LOWER('NATORI'), LOWER('VINCE CAMUTO'), LOWER('SPANX'), LOWER('GUCCI'), LOWER('DYSON'), LOWER('CANADA GOOSE'),
    LOWER('LANCOME'), LOWER('ESTEE LAUDER'), LOWER('BONY LEVY'), LOWER('BAREFOOT DREAMS'), LOWER('MARC JACOBS'), LOWER('NORDSTROM'
     ), LOWER('DYSON'), LOWER('DOLCE AND GABBANA'), LOWER('ORREFORS'), LOWER('OLD TREND'), LOWER('CECE'), LOWER('BELLA BELLE'
     ), LOWER('STONE ISLAND'), LOWER('DSQUARED2'), LOWER('THAMES AND KOSMOS'), LOWER('ECCO'))
  THEN sk.brand_name
  ELSE 'OTHER'
  END AS merch_brand,
 COALESCE(CASE
   WHEN mpg.mpg_category IS NULL
   THEN 'undefined'
   ELSE mpg.mpg_category
   END, 'undefined') AS merch_category_mpg,
 COALESCE(CASE
   WHEN dmr.merch_role_nord IS NULL
   THEN 'undefined'
   ELSE dmr.merch_role_nord
   END, 'undefined') AS merch_role_nord,
 COALESCE(CASE
   WHEN dmr.merch_role_rack IS NULL
   THEN 'undefined'
   ELSE dmr.merch_role_rack
   END, 'undefined') AS merch_role_rack,
 sk.color_desc AS merch_color,
 sk.size_1_desc AS merch_size,
 sk.sku_type_desc AS merch_skutype,
 pisrd.live_date,
 sk.msrp_amt AS merch_price_msrp,
   sty.style_group_num || '_' || sk.color_num AS merch_stylecolor_id,
 sty.style_group_num,
  CASE
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'women') > 0
  THEN 'womens'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'men') > 0
  THEN 'mens'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'kids') > 0
  THEN 'kids'
  ELSE cap.merch_persona
  END AS merch_persona,
  CASE
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'career') > 0
  THEN 'career'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'intimates|sleep') > 0
  THEN 'sleep'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'outerwear|active|swim') > 0
  THEN 'out and active'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'fragrance|designer|fine_jewelry|handbags|beauty|mens_dress') > 0
  THEN 'dress to impress'
  WHEN REGEXP_INSTR(COALESCE(CASE
      WHEN mpg.mpg_category IS NULL
      THEN 'undefined'
      ELSE mpg.mpg_category
      END, 'undefined'), 'good|better|best') > 0
  THEN REGEXP_EXTRACT_ALL(mpg.mpg_category, '[^_]+')[SAFE_OFFSET(3)]
  ELSE 'other'
  END AS merch_occasion,
 cap.merch_type,
 cap.inventory_capacity_category AS smart_capacity_id,
 CONCAT(CASE
   WHEN REGEXP_INSTR(sk.div_desc, 'WOMEN|WEAR|SPEC|MN/KID') > 0
   THEN 'APPAREL'
   WHEN REGEXP_INSTR(sk.div_desc, 'SHOE') > 0
   THEN 'SHOES'
   WHEN REGEXP_INSTR(sk.div_desc, 'ACC') > 0
   THEN 'ACCESSORIES'
   WHEN sk.div_desc IS NULL
   THEN 'OTHER'
   ELSE REPLACE(sk.div_desc, 'INACT-|INACT -', '')
   END, '|', REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(sk
               .dept_desc, 'OTHER'), 'YNG ', 'YOUNG '), 'WMS |WMNS |WM ', 'WOMENS '), 'BTR |BET ', 'BETTER '),
           ' BTR| BET', ' BETTER '), 'DSNR', 'DESIGNER'), 'SPTWR', 'SPORTSWEAR '), 'HDBGS|HDBG', 'HANDBAGS '), 'MID CRR'
       ,'MID CAREER'), 'ACT ', 'ACTIVE '), ' APP', ' APPAREL'), '/LUG', '/LUGGAGE'), 'BET ', 'BETTER '), '|',
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CASE
                               WHEN LOWER(sk.class_desc) IN (LOWER('DRESSES'), LOWER('SNEAKERS'), LOWER('DENIM PANTS'),
                                 LOWER('KNIT TOPS'), LOWER('BOOTS SHORT'), LOWER('SNEAKERS'), LOWER('GIFT SETS'), LOWER('SANDALS'
                                  ), LOWER('FRAGRANCE'), LOWER('CROSSBODY'), LOWER('WM NONPZD SUNGL'), LOWER('CROSSBODY'
                                  ), LOWER('TOTE'), LOWER('THROWS'), LOWER('HOME DECOR'), LOWER('SHEETS'), LOWER('HANDHELD'
                                  ), LOWER('EARRINGS'), LOWER('HOME DECOR'), LOWER('TABLETOP'), LOWER('TOTE'), LOWER('CASUAL PANTS'
                                  ), LOWER('DENIM'), LOWER('S/S WOVENS'), LOWER('SNEAKERS'), LOWER('KNIT TOPS'))
                               THEN sk.class_desc
                               ELSE 'OTHER'
                               END, 'ACTIV ', 'ACTIVE'), 'ACCESSORY', 'ACCESSORIES'), ' ACC ', ' ACCESSORIES '), 'BOOT '
                           ,'BOOTS '), 'BOY ', 'BOYS '), 'DRSS', 'DRESSES'), 'ELECTRNC', 'ELECTRONICS'), ' JWL ',
                       ' JEWELRY '), 'GIRL |GRL ', 'GIRLS '), ' GRL', ' GIRLS'), 'ORGANIZATN', 'ORGANIZATION'), 'INF ',
                   'INFANTS '), 'SANDAL |SNDL ', 'SANDALS '), 'SNKR', 'SNEAKERS'), ' LIT | LITTL ', ' LITTLE '), 'TDLR'
               ,'TODDLER'), 'YNG ', 'YOUNG '), 'WMS |WMNS |WM ', 'WOMENS '), 'BTR |BET ', 'BETTER '), ' BTR| BET',
           ' BETTER '), 'DSNR', 'DESIGNER'), 'SPTWR', 'SPORTSWEAR '), 'HDBGS|HDBG', 'HANDBAGS '), 'MID CRR',
       'MID CAREER'), 'ACT ', 'ACTIVE '), ' APP', ' APPAREL'), '/LUG', '/LUGGAGE'), 'BET ', 'BETTER '), '|', CASE
   WHEN LOWER(sk.brand_name) IN (LOWER('UGG'), LOWER('NIKE'), LOWER('SAINT LAURENT'), LOWER('GUCCI'), LOWER('ZELLA'),
     LOWER('FREE PEOPLE'), LOWER('TORY BURCH'), LOWER('ADIDAS'), LOWER('NORDSTROM'), LOWER('TORY BURCH'), LOWER('MAC DUGGAL'
      ), LOWER('MADEWELL'), LOWER('NIKE'), LOWER('CHANEL'), LOWER('VINCE'), LOWER('TED BAKER LONDON'), LOWER('LA FEMME'
      ), LOWER('THE NORTH FACE'), LOWER('RAG AND BONE'), LOWER('AG'), LOWER('FRAME'), LOWER('DAVID YURMAN'), LOWER('PAIGE'
      ), LOWER('NATORI'), LOWER('VINCE CAMUTO'), LOWER('SPANX'), LOWER('GUCCI'), LOWER('DYSON'), LOWER('CANADA GOOSE'),
     LOWER('LANCOME'), LOWER('ESTEE LAUDER'), LOWER('BONY LEVY'), LOWER('BAREFOOT DREAMS'), LOWER('MARC JACOBS'), LOWER('NORDSTROM'
      ), LOWER('DYSON'), LOWER('DOLCE AND GABBANA'), LOWER('ORREFORS'), LOWER('OLD TREND'), LOWER('CECE'), LOWER('BELLA BELLE'
      ), LOWER('STONE ISLAND'), LOWER('DSQUARED2'), LOWER('THAMES AND KOSMOS'), LOWER('ECCO'))
   THEN sk.brand_name
   ELSE 'OTHER'
   END, '|', COALESCE(dmr.merch_role_nord, 'undefined'), '|', COALESCE(dmr.merch_role_rack, 'undefined'), '|', COALESCE(COALESCE(CASE
     WHEN mpg.mpg_category IS NULL
     THEN 'undefined'
     ELSE mpg.mpg_category
     END, 'undefined'), 'undefined')) AS sku_segment_id,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim AS sk
 INNER JOIN (SELECT rms_sku_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.order_line_detail_fact AS oldf
  WHERE order_date_pacific BETWEEN {{params.start_date}} AND {{params.end_date}}
  GROUP BY rms_sku_num) AS active_skus ON LOWER(sk.rms_sku_num) = LOWER(active_skus.rms_sku_num)
 LEFT JOIN (SELECT rms_sku_num,
   MAX(CAST(eff_begin_tmstp AS DATE)) AS live_date
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_item_selling_rights_dim
  WHERE LOWER(channel_country) = LOWER('US')
   AND LOWER(is_sellable_ind) = LOWER('Y')
   AND LOWER(selling_status_code) = LOWER('UNBLOCKED')
   AND LOWER(selling_channel) = LOWER('ONLINE')
  GROUP BY rms_sku_num) AS pisrd ON LOWER(sk.rms_sku_num) = LOWER(pisrd.rms_sku_num)
 LEFT JOIN (SELECT epm_sku_num,
   epm_style_num,
   rms_style_num,
   div_desc
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim
  WHERE LOWER(channel_country) = LOWER('US')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_sku_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS psd
 ON sk.epm_sku_num = psd.epm_sku_num
 LEFT JOIN (SELECT epm_style_num,
   class_num,
   dept_num,
   style_group_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_style_dim
  WHERE LOWER(channel_country) = LOWER('US')
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY epm_style_num ORDER BY channel_country DESC, dw_batch_date DESC)) = 1) AS sty
 ON psd.epm_style_num = sty.epm_style_num
 LEFT JOIN `{{params.gcp_project_id}}`.t2dl_das_cal.customer_merch_mpg_dept_categories AS mpg ON sk.dept_num = mpg.dept_num
 LEFT JOIN (SELECT div_desc,
   dept_desc,
   class_desc,
   sbclass_desc,
   COALESCE(MAX(nord_role_desc), 'undefined') AS merch_role_nord,
   COALESCE(MAX(rack_role_desc), 'undefined') AS merch_role_rack
  FROM `{{params.gcp_project_id}}`.t2dl_das_po_visibility.ccs_merch_themes
  GROUP BY div_desc,
   dept_desc,
   class_desc,
   sbclass_desc) AS dmr ON LOWER(dmr.div_desc) = LOWER(sk.div_desc) AND LOWER(dmr.dept_desc) = LOWER(sk.dept_desc) AND
    LOWER(dmr.class_desc) = LOWER(sk.class_desc) AND LOWER(dmr.sbclass_desc) = LOWER(sk.sbclass_desc)
 LEFT JOIN (SELECT dept_num,
   subdivision_name,
    CASE
    WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
    THEN 'womens'
    WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
    THEN 'mens'
    WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
    THEN 'kids'
    ELSE 'other'
    END AS merch_persona,
    CASE
    WHEN REGEXP_INSTR(subdivision_name,
      'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
    THEN 'apparel'
    WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
    THEN 'shoes'
    WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
    THEN 'accessories'
    WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
    THEN 'home'
    ELSE 'other'
    END AS merch_type,
    CASE
    WHEN LOWER(CASE
       WHEN REGEXP_INSTR(subdivision_name,
         'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
       THEN 'apparel'
       WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
       THEN 'shoes'
       WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
       THEN 'accessories'
       WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
       THEN 'home'
       ELSE 'other'
       END) = LOWER('home')
    THEN 'other home'
    WHEN REGEXP_INSTR(subdivision_name, 'KIDS APPAREL|KIDS DESIGNER') > 0 OR LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
        THEN 'womens'
        WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
        THEN 'mens'
        WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
        THEN 'kids'
        ELSE 'other'
        END) = LOWER('kids')
    THEN 'kids apparel and accessories'
    WHEN LOWER(subdivision_name) IN (LOWER('ACCESSORIES'), LOWER('MAKEUP&SKINCARE'))
    THEN 'womens accessories'
    WHEN REGEXP_INSTR(subdivision_name, 'WOMENS DESIGNER|LINGERIE|WOMENS SPECIAL') > 0
    THEN 'womens apparel'
    WHEN REGEXP_INSTR(subdivision_name, 'MENS DESIGNER|MENS SPECIALIZED|AM DESIGNER') > 0
    THEN 'mens apparel'
    WHEN LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
        THEN 'womens'
        WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
        THEN 'mens'
        WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
        THEN 'kids'
        ELSE 'other'
        END) = LOWER('womens') AND LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name,
          'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
        THEN 'apparel'
        WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
        THEN 'shoes'
        WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
        THEN 'accessories'
        WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
        THEN 'home'
        ELSE 'other'
        END) = LOWER('other')
    THEN 'womens apparel'
    WHEN LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
        THEN 'womens'
        WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
        THEN 'mens'
        WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
        THEN 'kids'
        ELSE 'other'
        END) = LOWER('other') AND LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name,
          'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
        THEN 'apparel'
        WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
        THEN 'shoes'
        WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
        THEN 'accessories'
        WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
        THEN 'home'
        ELSE 'other'
        END) = LOWER('apparel')
    THEN 'womens apparel'
    WHEN LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
        THEN 'womens'
        WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
        THEN 'mens'
        WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
        THEN 'kids'
        ELSE 'other'
        END) = LOWER('mens') AND LOWER(CASE
        WHEN REGEXP_INSTR(subdivision_name,
          'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
        THEN 'apparel'
        WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
        THEN 'shoes'
        WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
        THEN 'accessories'
        WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
        THEN 'home'
        ELSE 'other'
        END) = LOWER('other')
    THEN 'mens apparel'
    ELSE CONCAT(CASE
      WHEN REGEXP_INSTR(subdivision_name, 'WOMENS|WMN|LINGERIE|BEAUTY|PETITES|MAKEUP|SKINCARE|BRIDAL') > 0
      THEN 'womens'
      WHEN REGEXP_INSTR(subdivision_name, 'MENS|MN|MEN') > 0
      THEN 'mens'
      WHEN REGEXP_INSTR(subdivision_name, 'KIDS|TODDLER|INFANT|CHILD|YC |BABIE') > 0
      THEN 'kids'
      ELSE 'other'
      END, ' ', CASE
      WHEN REGEXP_INSTR(subdivision_name,
        'APPAREL|SWIM|WEAR|DESIGN|DRESSES|PETITES|LINGERIE|TOPSHOP|COATS|SPECIAL|SPRTSWR|CLOTH') > 0
      THEN 'apparel'
      WHEN REGEXP_INSTR(subdivision_name, 'SHOE') > 0
      THEN 'shoes'
      WHEN REGEXP_INSTR(subdivision_name, 'COSMETICS|BEAUTY|MAKEUP|SKINCARE|ACC|JEWELRY|FRAG') > 0
      THEN 'accessories'
      WHEN REGEXP_INSTR(subdivision_name, 'HOME|FURNISH') > 0
      THEN 'home'
      ELSE 'other'
      END)
    END AS inventory_capacity_category,
   COUNT(*) AS departments
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dd
  GROUP BY dept_num,
   subdivision_name,
   merch_persona,
   merch_type,
   inventory_capacity_category) AS cap ON sk.dept_num = cap.dept_num
WHERE LOWER(sk.channel_country) = LOWER('US');


INSERT INTO `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sku_dim
SELECT DISTINCT *
 FROM sku_dim_prep
 WHERE sku_id NOT IN (SELECT DISTINCT sku_id
    FROM `{{params.gcp_project_id}}`.{{params.usl_t2_schema}}.sku_dim);
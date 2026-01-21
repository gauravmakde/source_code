/*
Name: Strategic Brands Lookup Table and Supplier View
APPID-Name: APP08706 - General In-Season Reporting
Purpose: Creates a static Strategic Brands Lookup table (T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.STRAT_BRANDS_LKP) for 
    2023 Nordstrom and Nordstrom Rack strategic brands
    
    Also creates a dynamic Supplier/Supplier Group lookup view based upon current supplier + supplier group mappings 
    (T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.STRAT_BRANDS_SUPP_CURR_VW)

Variable(s):    {environment_schema} - T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING
DAG: merch_pra_strategic_brands
Author(s): Alli Moore
Date Created: 03-27-2023
Date Last Updated: 03-27-2023
*/


/*********************************** STRATEGIC BRANDS LOOKUP TABLE *************************************/

-- DDL
CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'STRAT_BRANDS_LKP', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.STRAT_BRANDS_LKP
    , FALLBACK
    , NO BEFORE JOURNAL
    , NO AFTER JOURNAL
    , CHECKSUM = DEFAULT
    , DEFAULT MERGEBLOCKRATIO
    , MAP = TD_MAP1
(
	banner					VARCHAR(15) CHARACTER SET UNICODE NOT NULL
	, vendor_brand_code		INTEGER
	, vendor_brand_name		VARCHAR(60) CHARACTER SET UNICODE NOT NULL
	, vendor_brand_desc		VARCHAR(300) CHARACTER SET UNICODE
	, load_tmstp			TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX (vendor_brand_name, banner);
GRANT SELECT ON {environment_schema}.STRAT_BRANDS_LKP TO PUBLIC;


-- HARDCODED 2023 STRATEGIC BRANDS  
INSERT INTO {environment_schema}.STRAT_BRANDS_LKP
    -- 2023 Rack SB
    SELECT DISTINCT
        'NORDSTROM_RACK' AS banner
        , vendor_brand_code
        , vendor_brand_name
        , vendor_brand_desc 
        , CURRENT_TIMESTAMP AS load_tmstp
    FROM PRD_NAP_USR_VWS.VENDOR_BRAND_DIM
    WHERE UPPER(vendor_brand_name) IN 
    (
        '7 FOR ALL MANKIND'
        ,'ADIDAS'
        ,'ADRIANNA PAPELL'
        ,'AG'
        ,'ALLSAINTS'
        ,'ANNE KLEIN'
        ,'ASICS'
        ,'ATELIER LUXE'
        ,'BAREFOOT DREAMS'
        ,'BCBG'
        ,'BIRKENSTOCK'
        ,'BORN'
        ,'BP'
        ,'BROOKS BROTHERS'
        ,'CALVIN KLEIN'
        ,'CHARLOTTE TILBURY'
        ,'COACH'
        ,'COLE HAAN'
        ,'DEMOCRACY'
        ,'DONNA KARAN'
        ,'DOLCE VITA'
        ,'DR MARTENS'
        ,'ECCO USA' 
        ,'EILEEN FISHER'
        ,'FENDI'
        ,'FLORSHEIM'
        ,'FRAME'
        ,'FRANCO SARTO'
        ,'FREE PEOPLE'
        ,'FRENCH CONNECTION'
        ,'GOOD AMERICAN'
        ,'GUCCI'
        ,'GUESS'
        ,'HALOGEN'
        ,'HUDSON'
        ,'HUGO BOSS' 
        ,'JEFFREY CAMPBELL'
        ,'JOES JEANS' 
        ,'KATE SPADE' 
        ,'KARL LAGERFELD' 
        ,'KENNETH COLE'
        ,'KURT GEIGER'
        ,'LEVI STRAUSS AND CO.'
        ,'LONGCHAMP'
        ,'LUCKY BRAND'
        ,'MAC'
        ,'MADEWELL'
        ,'MAGNANNI'
        ,'MARC FISHER'
        ,'MARC JACOBS'
        ,'MAX STUDIO'
        ,'MIA'
        ,'MICHAEL KORS'
        ,'MICHELE'
        ,'NATORI'
        ,'NATURALIZER'
        ,'NAUTICA'
        ,'NEST NEW YORK'
        ,'NEUTROGENA'
        ,'NEW BALANCE'
        ,'NIKE'
        ,'NINE WEST'
        ,'NORDSTROM'
        ,'NORDSTROM RACK' -- DNE
        ,'NYDJ'
        ,'ORIGINAL PENGUIN'
        ,'ON RUNNING'
        ,'PAIGE'
        ,'PUMA'
        ,'RAG AND BONE'
        ,'RALPH LAUREN'
        ,'RAY BAN'
        ,'REBECCA MINKOFF'
        ,'ROCKPORT'
        ,'RODD AND GUNN'
        ,'SAM EDELMAN'
        ,'SKECHERS'
        ,'SOREL'
        ,'SPANX'
        ,'SPERRY TOP-SIDER'
        ,'STEVE MADDEN'
        ,'STUART WEITZMAN'
        ,'TAHARI'
        ,'TED BAKER' 
        ,'THE NORTH FACE'
        ,'THEORY'
        ,'TIMBERLAND'
        ,'TOMMY BAHAMA'
        ,'TOMMY HILFIGER'
        ,'TOPSHOP'
        ,'TORY BURCH'
        ,'TRAVIS MATHEW'
        ,'TUMI'
        ,'UGG'
        ,'VANS'
        ,'VERONICA BEARD'
        ,'VINCE'
        ,'VINCE CAMUTO'
        ,'WIT  AND  WISDOM' 
        ,'ZELLA'
    )
    AND vendor_brand_status = 'A'

    UNION ALL 

    -- 2023 Nordstrom SB
    SELECT DISTINCT
        'NORDSTROM' AS banner
        , vendor_brand_code
        , vendor_brand_name
        , vendor_brand_desc 
        , CURRENT_TIMESTAMP AS load_tmstp
    FROM PRD_NAP_USR_VWS.VENDOR_BRAND_DIM
    WHERE UPPER(vendor_brand_name) IN 
    (
        'NATORI' 
        ,'SKARLETT BLUE'
        ,'VINCE'
        ,'ROBERT BARAKETT'
        ,'JACK VICTOR'
        ,'TED BAKER'
        ,'TRAVIS MATHEW'
        ,'HUGO BOSS'
        ,'MOTHER'
        ,'DAVID DONAHUE'
        ,'REISS'
        ,'RAILS'
        ,'CINQ A SEPT'
        ,'LIKELY'
        ,'TOMMY BAHAMA'
        ,'PETER MILLAR'
        ,'BERNARDO'
        ,'FRAME' 
        ,'ALICE + OLIVIA'
        ,'GOOD AMERICAN'
        ,'THEORY'
        ,'VERONICA BEARD'
        ,'CANALI'
        ,'RALPH LAUREN'
        ,'SAM EDELMAN'
        ,'VIA SPIGA'
        ,'34 HERITAGE'
        ,'MAVI' 
        ,'EMPORIO ARMANI' 
        --,'EMPORIO ARMANI'-- DUPLICATED
        ,'ANDREW MARC'
        ,'COLE HAAN'
        ,'LEVI STRAUSS AND CO.' 
        ,'MARC JOSEPH NEW YORK' 
        ,'KARL LAGERFELD' 
        ,'ELIZA J'
        ,'HARPER ROSE'
        ,'VINCE CAMUTO'
        ,'BURBERRY'
        ,'CHANEL'
        ,'VERSACE' 
        ,'OAKLEY'
        ,'PRADA'
        ,'RAY BAN'
        ,'TORY BURCH'
        ,'VALENTINO'
        ,'TOM FORD'
        ,'CELINE'
        ,'CHRISTIAN DIOR'
        ,'FENDI'
        ,'DRESS THE POPULATION'
        ,'FREE PEOPLE'
        ,'MADEWELL'
        ,'AG'
        ,'STEVE MADDEN'
        ,'SUPERGA'
        ,'DOLCE VITA'
        ,'DV FOOTWEAR'-- DNE -- DOLCE VITA?
        ,'ALLSAINTS'
        ,'SPANX'
        ,'EILEEN FISHER'
        ,'ETON'
        ,'FAHERTY BRAND'
        ,'WACOAL'
        ,'SWEATY BETTY'
        ,'CORSO COMO'
        ,'LUCKY BRAND'
        ,'BRAX'
        ,'SPERRY TOP-SIDER'
        ,'MERRELL'
        ,'SAUCONY'
        ,'BAREFOOT DREAMS'
        ,'THE NORTH FACE'
        ,'JEFFREY CAMPBELL'
        ,'ECCO USA' 
        ,'SOREL'
        ,'BEYOND YOGA'
        ,'ALO'
        ,'CALVIN KLEIN'
        ,'VANS'
        ,'MARC JACOBS'
        ,'TRUE AND CO'
        ,'NIKE'
        ,'CARRERA EYEWEAR'
        ,'GIVENCHY'
        ,'KATE SPADE'
        ,'RAG AND BONE' 
        ,'ON RUNNING' 
        ,'DR MARTENS'
        ,'STUART WEITZMAN'
        ,'MAGNANNI'
        ,'CONVERSE'
        ,'PAUL GREEN'
        ,'ADIDAS'
        ,'TO BOOT NEW YORK'
        ,'BORN'
        ,'KORK-EASE'
        ,'AGL' 
        ,'UGG'
        ,'ALLEN EDMONDS'
        ,'27 EDIT'
        ,'DR SCHOLLS'
        ,'FRANCO SARTO'
        ,'NATURALIZER'
        ,'VIONIC'
        ,'ZODIAC'
        ,'BIRKENSTOCK'
        ,'LONGINES' 
        ,'TISSOT' 
        ,'MOVADO'
        ,'MVMT'
        ,'BECCA COSMETICS' 
        ,'SOLUNA'
        ,'LA BLANCA'
        ,'BOND-EYE AUSTRALIA' 
        ,'SEA LEVEL'
        ,'ELAN'
        ,'ROBIN PICCONE'
        ,'HANKY PANKY'
        ,'RODD AND GUNN' 
        ,'BARBOUR'
        ,'MICHAEL KORS'
        ,'SCHUTZ'
        ,'ALEXANDRE BIRMAN' 
        ,'HOKA' 
        ,'BLONDO'
        ,'DONNA KARAN'
        ,'FRENCH CONNECTION'
        ,'LOEWE'
        ,'BETSEY JOHNSON'
        ,'ANNE KLEIN'
        ,'KEDS'
        ,'JIMMY CHOO'
        ,'ISABEL MARANT'
        ,'HAMILTON' 
        ,'RADO' 
        ,'MIDO'
        ,'COACH'
        ,'LACOSTE'
        ,'REBECCA MINKOFF'
        ,'OLIVIA BURTON' 
        ,'FOSSIL'
        ,'MICHELE' 
        ,'COLUMBIA SPORTSWEAR COMPANY' 
        ,'BZEES'
        ,'RYKA'
        ,'LIFESTRIDE'
        ,'GUCCI'
        ,'SAINT LAURENT'
        ,'L.L.BEAN'
        ,'SKIMS'
        ,'BONY LEVY'
        ,'PAIGE'
        ,'CANADA GOOSE'
        ,'MARC FISHER'
        ,'ALEXANDER MCQUEEN'
        ,'BOTTEGA VENETA'
        ,'BALENCIAGA'
        ,'CARTIER'
        ,'CHLOE'
    )
    AND vendor_brand_status = 'A';

COLLECT STATS
	 PRIMARY INDEX (banner, vendor_brand_name)
		ON {environment_schema}.STRAT_BRANDS_LKP;

/***********************************************************************************************************/





/********************************* SB SUPPLIER/SUPPLIER GROUP LOOKUP VIEW *********************************/

-- Supplier/Supplier Group Lookup View	
    -- NOTE: A Supplier Num can map to MULTIPLE Supplier Groups with the same Eff_Begin_Tmstp but different Dept mappings
CREATE VIEW {environment_schema}.STRAT_BRANDS_SUPP_CURR_VW
AS
LOCK ROW FOR ACCESS
WITH vendor_lkp AS (	
	SELECT DISTINCT 
		sb.banner AS sb_banner
		, CASE
			WHEN sb.banner = 'NORDSTROM_RACK' THEN 'OP'
			ELSE 'FP'
		END AS lkp_banner
		, sb.vendor_brand_code
		, sb.vendor_brand_name
		, CAST(vendor_num AS varchar(100)) AS vendor_num
		
	FROM {environment_schema}.STRAT_BRANDS_LKP sb
	
	LEFT JOIN PRD_NAP_USR_VWS.VENDOR_BRAND_XREF vbx 
		ON vbx.vendor_brand_code = sb.vendor_brand_code
		AND vbx.association_status = 'A'
)
SELECT DISTINCT
	sb_banner
	, vendor_brand_code
	, vendor_brand_name
	, supplier_num
    , dept_num
	, supplier_group
    , case
	  	when vendor_brand_name = 'COLUMBIA SPORTSWEAR COMPANY' and lkp_banner = 'FP' then 'COLUMBIA'
	  	when vendor_brand_name = 'ALICE + OLIVIA' and lkp_banner = 'FP' then 'ALICE AND OLIVIA'
	 	when vendor_brand_name = 'ECCO USA' and lkp_banner = 'FP' then 'ECCO'
	 	when vendor_brand_name = 'TED BAKER' and lkp_banner = 'FP' then 'TED BAKER LONDON'
	  	when vendor_brand_name like 'KARL LAGERFELD%' and lkp_banner = 'FP' then 'KARL LAGERFELD'
	  	when vendor_brand_name = 'TRAVIS MATHEW' and lkp_banner = 'FP' then 'TRAVISMATHEW'
	  	when vendor_brand_name = 'DR MARTENS' and lkp_banner = 'FP' then 'DR. MARTENS'
	  	when vendor_brand_name = 'RAY BAN' and lkp_banner = 'FP' then 'RAY-BAN'
	  	when vendor_brand_name = 'HUGO BOSS' and lkp_banner = 'FP' then 'boss hugo boss'
	  	when vendor_brand_name = 'LEVI STRAUSS AND CO.' and lkp_banner = 'FP' then 'LEVIS'
	  	when vendor_brand_name = 'KATE SPADE' and lkp_banner = 'FP' then 'KATE SPADE NEW YORK'
	  	when vendor_brand_name like '%JACK VICTOR%' and lkp_banner = 'FP' then 'JACK VICTOR'
	  	else vendor_brand_name
      end as vendor_brand_name_adj    
	
FROM vendor_lkp v
INNER JOIN PRD_NAP_USR_VWS.SUPP_DEPT_MAP_DIM supp 
	ON supp.supplier_num = v.vendor_num
		AND supp.banner = v.lkp_banner
		AND CURRENT_DATE BETWEEN CAST(eff_begin_tmstp AS DATE FORMAT 'YYYY-MM-DD') AND CAST(eff_end_tmstp AS DATE FORMAT 'YYYY-MM-DD')
QUALIFY(ROW_NUMBER() OVER (PARTITION BY supplier_num, supplier_group, banner, dept_num ORDER BY eff_begin_tmstp DESC)) = 1 
;

GRANT SELECT ON {environment_schema}.STRAT_BRANDS_SUPP_CURR_VW TO PUBLIC;

/***********************************************************************************************************/

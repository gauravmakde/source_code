SET QUERY_BAND = 'App_ID=APP08907; 
     DAG_ID=ddl_fashion_map_choice_11521_ACE_ENG;
     Task_Name=ddl_fashion_map_choice;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_DIGENG.FASHION_MAP_CHOICE
Team/Owner: AI Digital/Soren Stime, Robert Legg
Date Created/Modified: 09/18/2024,TBD

Note:
Choice level data for fashion map users to join embeddings to so they know what product they are looking at. 
Used by other data science teams.
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'FASHION_MAP_CHOICE', OUT_RETURN_MSG);
 
create multiset table {deg_t2_schema}.FASHION_MAP_CHOICE
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    web_style_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    rms_style_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    color_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    channel_country VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    channel_brand VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,  
    selling_channel VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    selling_status_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    selling_channel_eligibility_code VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    is_online_purchasable VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    selling_retail_price_amt DECIMAL(18,2),
    regular_price_amt DECIMAL(18,2),
    vendor_brand_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    type_level_1_desc VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    type_level_2_desc VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    genders_desc VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    age_groups_desc VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    title_desc VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    features_desc VARCHAR(3200) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    details_and_care_desc VARCHAR(3200) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    ingredients_desc VARCHAR(3200) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    dw_sys_load_date DATE format 'YYYY-MM-DD',
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(web_style_num)
PARTITION BY COLUMN(type_level_1_desc)
;

-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE IS 'Customer choice table for fashion map users to reference their embeddings.';
-- Column comments (OPTIONAL)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.web_style_num IS 'Unique ID maintained in Web for a Style';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.rms_style_num IS 'Unique ID maintained for a rms style number';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.color_num IS 'Represent code maintained for a ite(m(s) color within Nordstrom';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.channel_country IS 'Channel Country = US,  and this will represent banner and channels';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.channel_brand IS '	Represents the brand of the customer channel for Nordstrom family of businesses Eg: NORDSTROM,NORDSTROM_RACK';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.selling_channel IS '	Represents the Selling Channels for Nordstrom Business If a new channel is added, the producer must version this schema.ONLINE:eCommerce Platforms like Web, Mobile.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.selling_status_code IS 'The selling status for the current item.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.selling_channel_eligibility_code IS '';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.is_online_purchasable IS 'Indicates the item is available, meets right to see criteria, has price greater than 0.99 and is published';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.selling_retail_price_amt IS 'Selling retail price value.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.regular_price_amt IS 'Regular price amount value.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.vendor_brand_name IS 'Brand name of the vendor';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.type_level_1_desc IS 'Describes the Product Type levels.Eg:SHOES,BOTTOMS,SWIMWEAR,TOPS,GLOVES/MITTENS,OUTERWEAR etc.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.type_level_2_desc IS 'Describes the Product Type levels.Eg:ATHLETIC,PANT,SWIM TOP,SPORTSHIRT,GLOVES,ANORAK/PARKA etc';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.genders_desc IS 'Denotes description for the gender code,Eg:Unisex,Male,Female,Unclassified';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.age_groups_desc IS 'Denotes description for the age_groups_code ADT-Adult,INF-infant';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.title_desc IS 'Item Title, its shown as "Title" of the item on the website';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.features_desc IS 'Copy Features, list of copy features and its shown as bulleted points under "Details and Care" on the website';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.details_and_care_desc IS 'Copy Body, its shown as "Details and Care" on the website';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.ingredients_desc IS 'Ingredients, its shown as "Ingredients" on the website';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.dw_sys_load_date IS 'Date that the record was added to the table';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (web_style_num), --  primary index
                    COLUMN (type_level_1_desc)  --  partition
on {deg_t2_schema}.FASHION_MAP_CHOICE;

SET QUERY_BAND = NONE FOR SESSION; 
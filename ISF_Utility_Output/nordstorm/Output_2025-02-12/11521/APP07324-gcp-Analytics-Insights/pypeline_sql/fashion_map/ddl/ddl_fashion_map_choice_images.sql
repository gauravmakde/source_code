SET QUERY_BAND = 'App_ID=APP08907; 
     DAG_ID=ddl_fashion_map_choice_images_11521_ACE_ENG;
     Task_Name=ddl_fashion_map_choice_images;'
     FOR SESSION VOLATILE;


/*
T2/Table Name: T2DL_DAS_DIGENG.FASHION_MAP_CHOICE_IMAGES
Team/Owner: AI Digital/Soren Stime, Robert Legg
Date Created/Modified: 09/18/2024,TBD

Note:
Choice level image data for fashion map users to join embeddings to so they know what product they are looking at. 
Used by other data science teams.
*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'FASHION_MAP_CHOICE_IMAGES', OUT_RETURN_MSG);
 
create multiset table {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    web_style_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    color_num VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    channel_country VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    channel_brand VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    image_index INTEGER, 
    shot_name VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    is_hero_image CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    asset_id VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC, 
    image_url VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC,
    dw_sys_load_date DATE format 'YYYY-MM-DD',
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(web_style_num)
PARTITION BY COLUMN(image_index)
;


-- Table Comment (STANDARD)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES IS 'Customer choice table for fashion map users to reference their embeddings.';
-- Column comments (OPTIONAL)
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.web_style_num IS 'Unique ID maintained in Web for a Style';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.color_num IS 'Represent code maintained for a ite(m(s) color within Nordstrom';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.channel_country IS 'Channel Country = US,  and this will represent banner and channels';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.channel_brand IS '	Represents the brand of the customer channel for Nordstrom family of businesses Eg: NORDSTROM,NORDSTROM_RACK';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.image_index IS 'Represents the order in which the images are displayed in the product detials page,starting with 0';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.shot_name IS 'Name of the type of shot for the image (e.g. side:onFigure)';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.is_hero_image IS 'Indicates the particular asset id is the hero image, ie the first image displayed, then this is marked as Y';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.asset_id IS 'Unique ID assigned for asset';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.image_url IS 'Indicates full URL for the image. The Asset ID is the last portion of the URL.';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.dw_sys_load_date IS 'Date that the record was added to the table';
COMMENT ON  {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES.dw_sys_load_tmstp IS 'Timestamp that the record was added to the table';


COLLECT STATISTICS  COLUMN (PARTITION),
                    COLUMN (web_style_num), --  primary index
                    COLUMN (image_index)  --  partition
on {deg_t2_schema}.FASHION_MAP_CHOICE_IMAGES;

SET QUERY_BAND = NONE FOR SESSION;
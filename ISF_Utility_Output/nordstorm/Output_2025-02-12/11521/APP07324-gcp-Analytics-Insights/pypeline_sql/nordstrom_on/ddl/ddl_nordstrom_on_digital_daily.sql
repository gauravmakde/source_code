SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_nordstrom_on_digital_styling_11521_ACE_ENG;
     Task_Name=ddl_nordstrom_on_digital_daily;'
     FOR SESSION VOLATILE;


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{remote_selling_t2_schema}', 'NORDSTROM_ON_DIGITAL_DAILY', OUT_RETURN_MSG); --for testing

--- Table Definition for T2DL_DAS_REMOTE_SELLING.NORDSTROM_ON_DIGITAL_DAILY
CREATE MULTISET TABLE {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY,
    FALLBACK ,
    NO BEFORE JOURNAL,
    NO AFTER JOURNAL,
    CHECKSUM = DEFAULT,
    DEFAULT MERGEBLOCKRATIO,
    MAP = TD_MAP1
    (
    board_id							varchar(100) character set unicode not casespecific,
    board_curator_id					varchar(100) character set unicode not casespecific,
    employee_name						varchar(200) character set unicode not casespecific,
    payroll_store						char(5) character set unicode not casespecific,
    payroll_store_description			varchar(200) character set unicode not casespecific,
    payroll_department_description  	varchar(200) character set unicode not casespecific,
    employee_payroll_store_region_num   integer compress,
    employee_payroll_region_desc		varchar(150) character set unicode not casespecific,
    board_created_date					date,
    board_sent_date						date,
    board_order_date					date,
    order_tmstp_pst				    	timestamp,
    platform                			varchar(30) character set unicode not casespecific,
    board_type							varchar(30) character set unicode not casespecific,
    board_id_sent						varchar(100) character set unicode not casespecific,
    attributed_item_ind					smallint compress(0,1),
    order_number						varchar(32) character set unicode not casespecific,
    order_line_id						varchar(100) character set unicode not casespecific, --had to change to varchar bad characters
    rms_sku_num							varchar(32) character set unicode not casespecific,
    padded_upc							varchar(32) character set unicode not casespecific,
    upc_seq_num							integer compress,
    source_channel_country_code			varchar(2) character set unicode not casespecific,
    demand								decimal(20,2) default 0.00 compress 0.00,
    units								decimal(12,0) default 0. compress (0.,1.,2.,3.,4.,5.,-1.),
    order_pickup_ind					smallint compress (0,1),
    ship_to_store_ind					smallint compress(0,1),
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL

    )
    primary index (board_id) --unique not allowed
    partition by range_n(board_created_date between date '2018-02-04' and '2025-12-31' each interval '1' day, unknown)
    ;

-- table comment
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY IS 'Nordstrom ON digital data';
--Column comments
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_id IS 'the unique identifier of a given styleboard or style link';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_curator_id IS 'the employee id of the sylist who created the styleboard or stylelink';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.employee_name IS 'the legal name of the board curator';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.payroll_store IS 'home store employee id is tied to, but employee may not physically be located at store';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.payroll_store_description IS 'home store description of employee id, but employee may not physically be located at store';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.payroll_department_description IS 'home store department description of employee id';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.employee_payroll_store_region_num IS 'employee payroll store region number';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.employee_payroll_region_desc IS 'employee payroll store region description';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_created_date IS 'the PST date that the stylist created the board or stylelink';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_sent_date IS 'the PST date the stylist sent the given styleboard or stylelink to a customer';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_order_date IS 'the PST date that the customer made an order with at least 1 item from a styleboard or stylelink';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.order_tmstp_pst IS 'timestamp pacific standard time when a Nordstrom ON order was made';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.platform IS 'platform in which styleboard was ordered';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_type IS 'differentiates between a styleboard and a stylelink';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.board_id_sent IS 'the board id sent to a customer';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.attributed_item_ind IS 'indicates item in the order was part of a styleboard or a stylelink';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.order_number IS 'the unique identifier of a digital customer order';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.order_line_id IS 'the unique identifier of a single order line within a digital order';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.rms_sku_num IS 'the sku number for a given order line';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.padded_upc IS 'a 15 character string to uniquely identify an item that has been padded with 0 to the length of 15 characters for correct joining across tables';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.upc_seq_num IS 'a pseudo key used to resolve many to many relationship between tables when a customer orders more than 1 of the same item';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.source_channel_country_code IS 'the country where an online order was placed';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.demand IS 'online sales, excluding fraud, that have been ordered, but the customer has not necessarily been charged';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.units IS 'online units demanded, excluding fraud';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.order_pickup_ind IS 'traditional picks filled by store nodes. either filled in store or via Nordstrom overnight vans';
COMMENT ON  {remote_selling_t2_schema}.NORDSTROM_ON_DIGITAL_DAILY.ship_to_store_ind IS 'standard or expedited shipping of product to a store. includes s2s next day pickup filled from FC';
SET QUERY_BAND = NONE FOR SESSION;

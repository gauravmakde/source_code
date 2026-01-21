SET QUERY_BAND = 'App_ID=APP08737;
DAG_ID=customer_sandbox_fact_daily_11521_ACE_ENG;
Task_Name=customer_store_distance_buckets_build;'
FOR SESSION VOLATILE;


/************************************************************************************/
/************************************************************************************/
/************************************************************************************
 *
 * Build Customer Store Distance Fact table to support store distance look ups for sandbox.
 *
 ************************************************************************************/
/************************************************************************************/
/************************************************************************************/

/************************************************************************************
 * PART A-4) Get distances to stores
 ************************************************************************************/

--Find all customers with a KNOWN zip code, along with their Nordstrom & Rack Stores of Loyalty
--Create lookup table of relevant stores & their lat/long

/*** PART A-4-i) Get the lat & long for all Zips ***/
create multiset volatile table zip_latlong_driver as (
select country_code
     , zip_code
     , avg(latitude) latitude
     , avg(longitude) longitude
from prd_nap_usr_vws.zip_codes_dim
group by 1,2
) with data primary index(zip_code) on commit preserve rows;


/*** PART A-4-ii) Get the lat & long for all Stores ***/
create multiset volatile table store_latlong_driver as (
select distinct store_num
     , case when business_unit_desc in ('FULL LINE','FULL LINE CANADA','N.CA','N.COM','TRUNK CLUB')
              then 'NORD'
            when business_unit_desc in ('RACK','RACK CANADA','OFFPRICE ONLINE')
              then 'RACK'
            else null end
         as store_banner
     , store_postal_code
     , store_location_longitude
     , store_location_latitude
from prd_nap_usr_vws.store_dim
where store_close_date is null -- store is still open
  and store_open_date <= current_date
  and selling_store_ind = 'S'
  and store_type_code in ('FL','NL','RK')
  and business_unit_desc in ('FULL LINE','FULL LINE CANADA','RACK','RACK CANADA')
  and store_num <> 828
  and store_name not like '%BULK%'
  and store_name not like '%TRAINING%'
  and store_name not like '%EMPLOYEE%'
  and store_location_latitude is not null
  and store_location_longitude is not null
) with data primary index(store_num) on commit preserve rows;


/*** PART A-4-iii) Find the store closest to each zip ***/
create multiset volatile table zip_store_distance_closest as (
select a.zip_code
     , b.store_num
     , b.store_banner
     , cast('POINT(' || cast(a.longitude as number)||' '|| cast(a.latitude as number) || ')' as ST_GEOMETRY) as zip_location
     , cast('POINT(' || cast(b.store_location_longitude as number)
          ||' '|| cast(b.store_location_latitude as number) || ')' as ST_GEOMETRY) as store_location
     , zip_location.ST_SPHERICALDISTANCE(store_location) / 1000 * 0.62137 as zip_store_distance
from zip_latlong_driver a 
cross join store_latlong_driver b
qualify row_number() over (partition by zip_code order by zip_store_distance) = 1
) with data primary index(zip_code,store_num) on commit preserve rows;


/*** PART A-4-iv) Get the following for every customer:
 * Nordstrom store-of-loyalty
 * Rack store-of-loyalty
 * Zip code
 * ***/
create multiset volatile table customer_latlong_driver as (
select distinct a.acp_id
     , a.billing_postal_code
     , b.longitude as cust_longitude
     , b.latitude as cust_latitude
     , a.fls_loyalty_store_num
     , a.rack_loyalty_store_num
from PRD_NAP_USR_VWS.ANALYTICAL_CUSTOMER a
join zip_latlong_driver b on a.billing_postal_code = b.zip_code
--join year_specific_activity c on a.acp_id=c.acp_id -- understand why the join to the table
where a.billing_postal_code is not null
) with data primary index(acp_id) on commit preserve rows;


/*** PART A-4-v) For every customer, get:
 * Closest store (& Banner thereof)
 * Distance to Closest Store
 * Distance to Nordstrom store-of-loyalty
 * Distance to Rack store-of-loyalty
 * ***/
create multiset volatile table customer_store_distance as (
select a.acp_id
     , a.billing_postal_code
     , zt.store_num closest_store
     , zt.store_banner closest_store_banner
     , zt.zip_store_distance closest_store_distance
     , a.fls_loyalty_store_num
     , cast('POINT(' || cast(a.cust_longitude as number)
          ||' '|| cast(a.cust_latitude as number) || ')' as ST_GEOMETRY) as custlocation
     , cast('POINT(' || cast(nsol.store_location_longitude as number)
          ||' '|| cast(nsol.store_location_latitude as number) || ')' as ST_GEOMETRY) as nord_store_loc
     , custlocation.ST_SPHERICALDISTANCE(nord_store_loc) / 1000 * 0.62137 as nord_sol_distance
     , a.rack_loyalty_store_num
     , cast('POINT(' || cast(rsol.store_location_longitude as number)
          ||' '|| cast(rsol.store_location_latitude as number) || ')' as ST_GEOMETRY) as rack_store_loc
     , custlocation.ST_SPHERICALDISTANCE(rack_store_loc) / 1000 * 0.62137 as rack_sol_distance
from customer_latlong_driver a 
left join store_latlong_driver nsol on a.fls_loyalty_store_num = nsol.store_num
left join store_latlong_driver rsol on a.rack_loyalty_store_num = rsol.store_num
left join zip_store_distance_closest zt on a.billing_postal_code = zt.zip_code
) with data primary index(acp_id) on commit preserve rows;


/*** PART A-4-vi) For every customer, bucket the 3 distance columns:
 * Distance to Closest Store
 * Distance to Nordstrom store-of-loyalty
 * Distance to Rack store-of-loyalty
 * ***/
delete from {str_t2_schema}.customer_store_distance_buckets;

insert into {str_t2_schema}.customer_store_distance_buckets
select a.acp_id
     , a.billing_postal_code
     , a.closest_store
     , a.closest_store_banner
     , a.closest_store_distance
     , case when closest_store_distance is null then 'missing'
            when closest_store_distance <9 then '0'||cast(cast(ceiling(closest_store_distance) as int) as varchar(2))||' miles'
            when closest_store_distance <50 then cast(cast(ceiling(closest_store_distance) as int) as varchar(2))||' miles'
            else '51+ miles' end closest_store_dist_bucket
     , a.fls_loyalty_store_num nord_loyalty_store
     , a.nord_sol_distance
     , case when nord_sol_distance is null then 'missing'
            when nord_sol_distance <9 then '0'||cast(cast(ceiling(nord_sol_distance) as int) as varchar(2))||' miles'
            when nord_sol_distance <50 then cast(cast(ceiling(nord_sol_distance) as int) as varchar(2))||' miles'
            else '51+ miles' end nord_sol_dist_bucket
     , a.rack_loyalty_store_num rack_loyalty_store
     , a.rack_sol_distance
     , case when rack_sol_distance is null then 'missing'
            when rack_sol_distance <9 then '0'||cast(cast(ceiling(rack_sol_distance) as int) as varchar(2))||' miles'
            when rack_sol_distance <50 then cast(cast(ceiling(rack_sol_distance) as int) as varchar(2))||' miles'
            else '51+ miles' end rack_sol_dist_bucket
     , current_timestamp(6) as dw_sys_load_tmstp
from customer_store_distance a;


COLLECT STATISTICS COLUMN (acp_id) ON {str_t2_schema}.customer_store_distance_buckets;
COLLECT STATISTICS COLUMN (closest_store_dist_bucket) ON {str_t2_schema}.customer_store_distance_buckets;
COLLECT STATISTICS COLUMN (nord_sol_dist_bucket) ON {str_t2_schema}.customer_store_distance_buckets;
COLLECT STATISTICS COLUMN (rack_sol_dist_bucket) ON {str_t2_schema}.customer_store_distance_buckets;


SET QUERY_BAND = NONE FOR SESSION;

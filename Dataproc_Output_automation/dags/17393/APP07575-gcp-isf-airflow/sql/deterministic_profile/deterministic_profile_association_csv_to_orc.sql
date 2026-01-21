-- Read s3 csv source data
create temporary view csv_source_stg as
select
    _c0 as deterministic_profile_id,
    _c1 as unique_source_id,
    _c2 as business_day_date,
    _c3 as profile_event_tmstp
from csv_source;

-- Sink data to s3 path
insert overwrite table deterministic_customer_profile_association
select
    deterministic_profile_id,
    unique_source_id,
    business_day_date as businessdate,
    profile_event_tmstp
from csv_source_stg;

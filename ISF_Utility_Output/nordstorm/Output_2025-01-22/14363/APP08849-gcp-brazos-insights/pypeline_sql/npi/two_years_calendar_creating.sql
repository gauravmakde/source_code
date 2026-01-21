create temporary view two_years_saturdays_calendar as
select
    concat('FY', c.fiscal_year_num, '_',
        case length(cast(c.fiscal_month_num as varchar(10))) when 1 then concat('0', c.fiscal_month_num) else c.fiscal_month_num end,
        '-', c.month_abrv) as npi_month,
    date_format(to_date(c.day_date, 'MM/dd/yyyy'), 'yyyy-MM-dd') as day_date,
    c.week_num_of_fiscal_month as week
from OBJECT_MODEL.DAY_CAL_454_DIM c
where
    c.fiscal_year_num
        between
            (year(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')) - 1)
        and
            (year(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')) + 2)
    and
    c.day_num_of_fiscal_week = 1
    and
    to_date(c.day_date, 'MM/dd/yyyy') < date_add(from_utc_timestamp(current_timestamp(), "America/Los_Angeles"), 2 * 365)
order by c.day_date;


insert overwrite table calendar
select * from two_years_saturdays_calendar ;

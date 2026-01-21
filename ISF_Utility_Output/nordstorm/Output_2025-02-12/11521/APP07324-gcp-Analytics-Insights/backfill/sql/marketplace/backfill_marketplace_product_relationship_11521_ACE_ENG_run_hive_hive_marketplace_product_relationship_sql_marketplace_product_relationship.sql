create table if not exists ace_etl.marketplace_product_relationship (
    webstyleid string, 
    marketcode string,
    parentepmstyleid string, 
    rmsstylegroupid string, 
    brandlabelid string, 
    brandlabeldisplayname string,
    partnerrelationshipid string, 
    partnerrelationshiptype string,
    empstyleid integer, 
    departmentdescription string, 
    departmentnumber string, 
    subclassdescription string, 
    subclassnumber string, 
    classdescription string, 
    classnumber string,
    typelevel1description string, 
    typelevel2description string, 
    gender string, 
    agegroup string,
    webstyleid_sourcepublishtimestamp timestamp,
    epmstyleid_sourcepublishtimestamp timestamp,
    join_date date
)
using PARQUET
location "s3://ace-etl/marketplace_product_relationship"
partitioned by (join_date);

MSCK REPAIR table ace_etl.marketplace_product_relationship;

create or replace temporary view find_single_epmstyleid as
select webstyleid, marketcode, parentepmstyleid, rmsstylegroupid, brandlabelid, brandlabeldisplayname, sourcepublishtimestamp, partnerrelationshipid, partnerrelationshiptype, date'2024-05-31' as join_date
from
    (select *,
        row_number() over(partition by webstyleid, marketcode order by sourcepublishtimestamp desc, parentepmstyleid, rmsstylegroupid, brandlabelid, brandlabeldisplayname, partnerrelationshipid, partnerrelationshiptype) rk 
    from
    	(select distinct webstyleid, marketcode, rmsstylegroupid, parentepmstyleid, brandlabelid, brandlabeldisplayname, partnerrelationshipid, partnerrelationshiptype,
    	        from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
    	from nap_product.sku_digital_webstyleid_bucketed
		where webstyleid is not null
        and marketcode = 'US'
			and cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date) <= date'2024-05-31'
		)
    )
where rk = 1
;

create or replace temporary view find_single_attribute as
select marketcode, empstyleid, departmentdescription, departmentnumber, subclassdescription, subclassnumber, classdescription, classnumber, typelevel1description, typelevel2description, gender, agegroup, sourcepublishtimestamp, date'2024-05-31' as join_date
from 
	(select *,
		row_number() over(partition by empstyleid, marketcode order by sourcepublishtimestamp desc, departmentnumber, classnumber, typelevel1description, typelevel2description, gender, agegroup) rk
	from
		(select distinct marketcode, empstyleid, 
				genderdescription as gender, 
				agedescription as agegroup, 
				departmentdescription, departmentnumber, subclassdescription, subclassnumber, classdescription, classnumber, typelevel1description, typelevel2description, 
				from_utc_timestamp(sourcepublishtimestamp,'PST') as sourcepublishtimestamp
		from nap_product.style_digital_epmstyleid_bucketed
		where empstyleid is not null
        and marketcode = 'US'
			and cast(from_utc_timestamp(sourcepublishtimestamp,'PST') as date)  <= date'2024-05-31'
		)
	)
where rk = 1
;

create or replace temporary view output as
select 
    a.webstyleid, 
    a.marketcode,
    a.parentepmstyleid, 
    a.rmsstylegroupid, 
    a.brandlabelid, 
    a.brandlabeldisplayname,
    a.partnerrelationshipid, 
    a.partnerrelationshiptype,
    cast(b.empstyleid as integer) as empstyleid, 
    b.departmentdescription, 
    b.departmentnumber, 
    b.subclassdescription, 
    b.subclassnumber, 
    b.classdescription, 
    b.classnumber,
    b.typelevel1description, 
    b.typelevel2description, 
    b.gender, 
    b.agegroup,
    a.sourcepublishtimestamp as webstyleid_sourcepublishtimestamp,
    b.sourcepublishtimestamp as epmstyleid_sourcepublishtimestamp,
    a.join_date
from find_single_epmstyleid a 
left join find_single_attribute b
on a.parentepmstyleid = b.empstyleid 
and a.marketcode = b.marketcode
and a.join_date = b.join_date
;

insert
    OVERWRITE TABLE ace_etl.marketplace_product_relationship PARTITION (join_date)
select
    webstyleid, 
    marketcode,
    parentepmstyleid, 
    rmsstylegroupid, 
    brandlabelid, 
    brandlabeldisplayname,
    partnerrelationshipid, 
    partnerrelationshiptype,
    empstyleid, 
    departmentdescription, 
    departmentnumber, 
    subclassdescription, 
    subclassnumber, 
    classdescription, 
    classnumber,
    typelevel1description, 
    typelevel2description, 
    gender, 
    agegroup,
    webstyleid_sourcepublishtimestamp,
    epmstyleid_sourcepublishtimestamp,
    join_date
from
    output;
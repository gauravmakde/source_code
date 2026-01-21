SET QUERY_BAND = '
App_ID=APP08849;
DAG_ID=nap_to_allocation_extracts;
Task_Name=location_extracts;'
FOR SESSION VOLATILE;

-- generating the extract Data 
create temporary view location_extracts as 
select 
     '1', 
	 storenumber,
     shorternames.medname,
     postaladdress.stdzdcityname,
	 postaladdress.origstatename,
     adminhier.subgroupnumber,
     adminhier.subgroupdesc,
     adminhier.groupnumber,
     adminhier.groupmeddesc,
     substr(element_at(functiontypes,4)['startdate'],1,4)||substr(element_at(functiontypes,4)['startdate'],6,2)||substr(element_at(functiontypes,4)['startdate'],9,2) as startdate,
     CASE WHEN locationaddlinfo.grosssquarefootage is not null 
		THEN locationaddlinfo.grosssquarefootage
     	ELSE 0
     	END as grosssquarefootage,
     CASE WHEN locationtypecode ='W'
        THEN 'Y' 
        ELSE 'N'
        END AS whse_flag,
	 compstatusdesc,
     adminhier.budesc,
     postaladdress.origcountrycode AS MARKETS,
     '' AS Z_OPEN_07,
     '' AS Z_OPEN_08,
     '' AS Z_OPEN_09,
     '' AS Z_OPEN_10,
     '' AS Z_OPEN_11,
     '' AS Z_OPEN_12,
     '' AS Z_OPEN_13,
     '' AS Z_OPEN_14,
     '' AS Z_OPEN_15,
     '' AS Z_OPEN_16,
     '' AS Z_OPEN_17,
     '' AS Z_OPEN_18,
     '' AS Z_OPEN_19,
     '' AS Z_OPEN_20,
     '' AS Z_OPEN_21,
     '' AS Z_OPEN_22,
     '' AS Z_OPEN_23,
     '' AS Z_OPEN_24,
     '' AS Z_OPEN_25,
     '' AS Z_OPEN_26,
     '' AS Z_OPEN_27,
     '' AS Z_OPEN_28,
     '' AS Z_OPEN_29,
     '' AS Z_OPEN_30,
     '' AS Z_OPEN_31,
     '' AS Z_OPEN_32,
     '' AS Z_OPEN_33,
     '' AS Z_OPEN_34, 
     '' AS Z_OPEN_35,
     '' AS Z_OPEN_36,
     '' AS Z_OPEN_37,
     '' AS Z_OPEN_38,
     '' AS Z_OPEN_39,
     '' AS Z_OPEN_40,
     postaladdress.stdzdline1text,
	 CASE WHEN locationtypecode <>'W'
          THEN 'Y'
          ELSE 'N'
      END AS DISPLAY_WS,
     '' AS GLOBAL_COORD_LONGITUDE,
     '' AS GLOBAL_COORD_LATITUDE,
     adminhier.bunumber
from org.store
where adminhier.bunumber in ('1000','5000','2000')
and adminhier.regionnumber NOT IN ('1020','1030','1040','1045','1070') 
and (element_at(functiontypes,4)['enddate'] is NULL
 or date(cast(regexp_replace(element_at(functiontypes,4)['enddate'], 'T', ' ') as timestamp)) >= current_date
    );

--- Writing Data to s3 on a single partition
insert overwrite table location_tab
select * from  location_extracts; 

insert overwrite table location_audit select concat(date_format(from_utc_timestamp(current_timestamp,'PST'),'yyyyMMddHHmmss'),' ',format_number(count(1),'000000000000')) from location_extracts;

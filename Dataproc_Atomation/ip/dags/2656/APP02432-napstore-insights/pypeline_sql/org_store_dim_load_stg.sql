SET QUERY_BAND = '
App_ID=app02432;
DAG_ID=org_load_2656_napstore_insights;
Task_Name=org_store_dim_load_0_stg_table;'
FOR SESSION VOLATILE;

-- Reading from kafka:
create
temporary view org_store_dim_input AS
select *
from kafka_org_store_dim_input;

-- Writing Kafka to Semantic Layer:
insert
overwrite table org_store_dim_stg_table
select
       currentData.adminHier.buDesc as currentdata_adminhier_budesc,
       currentData.adminHier.buNumber as  currentdata_adminhier_bunumber,
       currentData.adminHier.buMedDesc as currentdata_adminhier_bumeddesc,
       currentData.adminHier.entDesc as currentdata_adminhier_entdesc,
       currentData.adminHier.entNumber as currentdata_adminhier_entnumber,
       currentData.adminHier.entMedDesc as currentdata_adminhier_entmeddesc,
       currentData.adminHier.groupDesc as currentdata_adminhier_groupdesc,
       currentData.adminHier.groupNumber as currentdata_adminhier_groupnumber,
       currentData.adminHier.groupMedDesc as currentdata_adminhier_groupmeddesc,
       currentData.adminHier.regionDesc as currentdata_adminhier_regiondesc,
       currentData.adminHier.regionNumber as currentdata_adminhier_regionnumber,
       currentData.adminHier.regionMedDesc as currentdata_adminhier_regionmeddesc,
       currentData.adminHier.regionShortDesc as currentdata_adminhier_regionshortdesc,
       currentData.adminHier.subGroupDesc as currentdata_adminhier_subgroupdesc,
       currentData.adminHier.subGroupNumber as currentdata_adminhier_subgroupnumber,
       currentData.adminHier.subGroupMedDesc as currentdata_adminhier_subgroupmeddesc,
       currentData.adminHier.subGroupShortDesc as currentdata_adminhier_subgroupshortdesc,
       currentData.compStatusCode as currentdata_compstatuscode,
       currentData.compStatusDesc as currentdata_compstatusdesc,
       currentData.dc.name as currentdata_dc_name,
       currentData.dc.storeNumber as currentdata_dc_storenumber,
       filter(currentData.functionTypes, x -> x.functiontypecode='PUBL')[0].startdate as store_open_date,
       filter(currentData.functionTypes, x -> x.functiontypecode='PUBL')[0].enddate as store_close_date,
       cast(currentData.isStoreOperationalToPublic as string) as currentdata_isstoreoperationaltopublic,
       currentData.locationTypeCode as currentdata_locationtypecode,
       currentData.locationTypeDesc as currentdata_locationtypedesc,
       currentData.name as currentdata_name,
       cast(currentData.postalAddress.isDaylightSavingsTimeParticipant as CHAR(10)) as currentdata_postaladdress_isdaylightsavingstimeparticipant,
       currentData.postalAddress.latitude as currentdata_postaladdress_latitude,
       currentData.postalAddress.longitude as currentdata_postaladdress_longitude,
       currentData.postalAddress.origCountryCode as currentdata_postaladdress_origcountrycode,
       currentData.postalAddress.origCountryName as currentdata_postaladdress_origcountryname,
       currentData.postalAddress.origStateCode as currentdata_postaladdress_origstatecode,
       currentData.postalAddress.origStateName as currentdata_postaladdress_origstatename,
       currentData.postalAddress.standardTimeZoneAbbrev as currentdata_postaladdress_standardtimezoneabbrev,
       currentData.postalAddress.standardTimeZoneUTCOffset as currentdata_postaladdress_standardtimezoneutcoffset,
       currentData.postalAddress.stdzdCityName as currentdata_postaladdress_stdzdcityname,
       currentData.postalAddress.stdzdCountyName as currentdata_postaladdress_stdzdcountyname,
       currentData.postalAddress.stdzdLine1Text as currentdata_postaladdress_stdzdline1text,
       currentData.postalAddress.stdzdPostalCode as currentdata_postaladdress_stdzdpostalcode,
       currentData.shorterNames.medName as currentdata_shorternames_medname,
       currentData.shorterNames.shortName as currentdata_shorternames_shortname,
       currentData.storeNumber as currentdata_storenumber,
       currentData.storePlanningChannel.planningChannelCode as currentdata_storeplanningchannel_planningchannelcode,
       currentData.storePlanningChannel.planningChannelDesc as currentdata_storeplanningchannel_planningchanneldesc,
       currentData.timeZoneName as currentdata_timezonename,
       currentData.typeCode as currentdata_typecode,
       currentData.typeDesc as currentdata_typedesc,
       cast(currentData.typeIsInventoryStore as string) as currentdata_typeisinventorystore,
       cast(currentData.typeIsSellingStore as string) as currentdata_typeissellingstore,
       currentData.locationAddlInfo.grossSquareFootage as currentdata_locationaddlinfo_grosssquarefootage,
       currentData.dma as currentdata_dma,
       currentDataUpdatedTimeStamp,
       cast(currentData.eligibilityTypes as string) as currentdata_eligibilityTypes,
       currentData.receivingLocation.locationName as currentdata_receivingLocation_locationName,
       currentData.receivingLocation.locationNumber as currentdata_receivingLocation_locationNumber,
       currentData.receivingLocation.postalAddress.latitude as currentdata_receivingLocation_postalAddress_latitude,
       currentData.receivingLocation.postalAddress.longitude as currentdata_receivingLocation_postalAddress_longitude,
       currentData.receivingLocation.postalAddress.origCountryCode as currentdata_receivingLocation_postalAddress_origCountryCode,
       currentData.receivingLocation.postalAddress.origCountryIsoCode as currentdata_receivingLocation_postalAddress_origCountryIsoCode,
       currentData.receivingLocation.postalAddress.origCountryName as currentdata_receivingLocation_postalAddress_origCountryName,
       currentData.receivingLocation.postalAddress.origCountryTelephoneCode as currentdata_receivingLocation_postalAddress_origCountryTelephoneCode,
       currentData.receivingLocation.postalAddress.origStateCode as currentdata_receivingLocation_postalAddress_origStateCode,
       currentData.receivingLocation.postalAddress.origStateName as currentdata_receivingLocation_postalAddress_origStateName,
       currentData.receivingLocation.postalAddress.origRegionCode as currentdata_receivingLocation_postalAddress_origRegionCode,
       currentData.receivingLocation.postalAddress.origRegionName as currentdata_receivingLocation_postalAddress_origRegionName,
       currentData.receivingLocation.postalAddress.standardTimeZoneAbbrev as currentdata_receivingLocation_postalAddress_standardTimeZoneAbbrev,
       currentData.receivingLocation.postalAddress.standardTimeZoneUTCOffset as currentdata_receivingLocation_postalAddress_standardTimeZoneUTCOffset,
       currentData.receivingLocation.postalAddress.stdzdCityName as currentdata_receivingLocation_postalAddress_stdzdCityName,
       currentData.receivingLocation.postalAddress.stdzdCountyName as currentdata_receivingLocation_postalAddress_stdzdCountyName,
       currentData.receivingLocation.postalAddress.stdzdLine1Text as currentdata_receivingLocation_postalAddress_stdzdLine1Text,
       currentData.receivingLocation.postalAddress.stdzdPostalCode as currentdata_receivingLocation_postalAddress_stdzdPostalCode
from org_store_dim_input;

create
temporary view org_store_peer_group_dim_input AS
select
       currentData.name,
       explode_outer(currentData.peerGroups) peerGroups,
       currentData.storeNumber,
       currentDataUpdatedTimeStamp
from org_store_dim_input;

insert
overwrite table org_store_peer_group_dim_stg_table
select
       name as currentdata_name,
       peerGroups.peerGroupDesc as currentdata_peergroups_peergroupdesc,
       peerGroups.peerGroupNumber as currentdata_peergroups_peergroupnumber,
       peerGroups.peerGroupShortDesc as currentdata_peergroups_peergroupshortdesc,
       peerGroups.peerGroupTypeCode as currentdata_peergroups_peergrouptypecode,
       peerGroups.peerGroupTypeDesc as currentdata_peergroups_peergrouptypedesc,
       storeNumber as currentdata_storenumber,
       currentDataUpdatedTimeStamp
from org_store_peer_group_dim_input
where peerGroups.peerGroupNumber is not null;
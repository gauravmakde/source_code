
  -- Gets Latest State for a Store
  create temporary view temp_store_delta_v1 AS 
  select distinct *,  RANK() over (partition by storeNumber ORDER BY currentDataUpdatedTimeStamp DESC) as rank_column 
  FROM nsp.org_store_ldg ;

  create temporary view temp_store_delta_v2 AS 
  SELECT 
    currentData,
    currentdataupdatedtimestamp,
    currentdataupdatedtimestampms,
    storeNumber,
    rootlevelchanges
    FROM temp_store_delta_v1 where rank_column = 1;

-- Creates Store Dim Table
CREATE TABLE IF NOT EXISTS nsp.org_store_dim_delta (
  currentData STRUCT<
    adminHier: STRUCT<
      buDesc: STRING,
      buNumber: STRING,
      buMedDesc: STRING,
      entDesc: STRING,
      entNumber: STRING,
      entMedDesc: STRING,
      groupDesc: STRING,
      groupNumber: STRING,
      groupMedDesc: STRING,
      regionDesc: STRING,
      regionNumber: STRING,
      regionMedDesc: STRING,
      regionShortDesc: STRING,
      subGroupDesc: STRING,
      subGroupNumber: STRING,
      subGroupMedDesc: STRING,
      subGroupShortDesc: STRING
    >,
    compStatusCode: STRING,
    compStatusDesc: STRING,
    dc: STRUCT<name: STRING, storeNumber: STRING>,
    detailedStoreHours: STRUCT<
      periodOverrides: ARRAY<STRUCT<name: STRING,isPlanned: BOOLEAN,periodOverrideDays: ARRAY<STRUCT<closeDateTime: STRING,openDateTime: STRING>>>>,
      regularWeeklyScheduleDays0Sun6Sat: ARRAY<STRUCT<openAbsoluteTime: STRING,closeAbsoluteTime: STRING,dayOfWeekStart0Sun6Sat: INT,dayOfWeekEnd0Sun6Sat: INT>>
    >,
    displayAddress: STRUCT<cityName: STRING,displayLatitude: DOUBLE,displayLongitude: DOUBLE,line1Text: STRING,postalCode: STRING,stateCode: STRING>,
    displayName: STRING,
    eligibilityTypes: ARRAY<STRUCT<code: STRING, desc: STRING>>,
    floorNOWs: ARRAY<STRUCT<
      code: STRING,
      desc: STRING,
      nows: ARRAY<STRUCT<nowDisplayName: string,storeNowDisplayName: string,nowNumber: string,nowTypeCode: string,nowTypeDesc: string>>,
      type: STRING>>,
    functionTypes: ARRAY<STRUCT<desc: STRING,functionTypeCode: STRING,startDate: STRING,endDate: STRING>>,
    isStoreOperationalToPublic: BOOLEAN,
    isStoreOpenRightNow: BOOLEAN,
    locationTypeCode: STRING,
    locationTypeDesc: STRING,
    name: STRING,
    ownershipTypeCode: STRING,
    ownershipTypeDesc: STRING,
    peerGroups: ARRAY<STRUCT<peerGroupDesc: STRING,peerGroupNumber: STRING,peerGroupShortDesc: STRING,peerGroupTypeCode: STRING,peerGroupTypeDesc: STRING>>,
    postalAddress: STRUCT<
      isDaylightSavingsTimeParticipant: BOOLEAN,
      latitude: DOUBLE,
      longitude: DOUBLE,
      origCountryCode: STRING,
      origCountryIsoCode: STRING,
      origCountryName: STRING,
      origCountryTelephoneCode: STRING,
      origStateCode: STRING,
      origStateName: STRING,
      origRegionCode: STRING,
      origRegionName: STRING,
      standardTimeZoneAbbrev: STRING,
      standardTimeZoneUTCOffset: STRING,
      stdzdCityName: STRING,
      stdzdCountyName: STRING,
      stdzdLine1Text: STRING,
      stdzdPostalCode: STRING
    >,
    receivingLocation: STRUCT<
      locationName: STRING,
      locationNumber: STRING,
      postalAddress: STRUCT<
        latitude: DOUBLE,
        longitude: DOUBLE,
        origCountryCode: STRING,
        origCountryIsoCode: STRING,
        origCountryName: STRING,
        origCountryTelephoneCode: STRING,
        origStateCode: STRING,
        origStateName: STRING,
        origRegionCode: STRING,
        origRegionName: STRING,
        standardTimeZoneAbbrev: STRING,
        standardTimeZoneUTCOffset: STRING,
        stdzdCityName: STRING,
        stdzdCountyName: STRING,
        stdzdLine1Text: STRING,
        stdzdPostalCode: STRING
      >
    >,
    restaurantAmenities: ARRAY<STRUCT<
      code: STRING,
      desc: STRING,
      detailedHours: STRUCT<
        regularWeeklyScheduleDays0Sun6Sat: ARRAY<STRUCT<openAbsoluteTime: STRING,closeAbsoluteTime: STRING,dayOfWeekStart0Sun6Sat: INT,dayOfWeekEnd0Sun6Sat: INT>>
      >,
      nowDisplayName: STRING,
      nowNumber: STRING,
      nowTypeCode: STRING,
      nowTypeDesc: STRING,
      storeNowDisplayName: STRING,
      storeSubNowDisplayName: STRING,
      subLoc1Code: STRING,
      subLoc1Desc: STRING,
      subLoc1Type: STRING,
      subLoc2Code: STRING,
      subLoc2Desc: STRING,
      subLoc2Type: STRING,
      telephone: STRUCT<
        externalTelephoneNumber: STRING,
        internalTelephoneNumber: STRING,
        externalExtensionNumber: STRING,
        internalExtensionNumber: STRING
      >
    >>,
    sca: STRUCT<
      buNumber: STRING,
      eligibilityTypes: ARRAY<STRUCT<code: STRING,desc: STRING>>,
      name: STRING,
      origCountryCode: STRING,
      stdzdPostalCode: STRING,
      typeCode: STRING
    >,
    serviceAmenities: ARRAY<STRUCT<
      code: STRING,
      desc: STRING,
      detailedHours: STRUCT<
        regularWeeklyScheduleDays0Sun6Sat: ARRAY<STRUCT<
          openAbsoluteTime: STRING,
          closeAbsoluteTime: STRING,
          dayOfWeekStart0Sun6Sat: INT,
          dayOfWeekEnd0Sun6Sat: INT
        >>
      >,
      nowDisplayName: STRING,
      nowNumber: STRING,
      nowTypeCode: STRING,
      nowTypeDesc: STRING,
      storeNowDisplayName: STRING,
      subLoc1Code: STRING,
      subLoc1Desc: STRING,
      subLoc1Type: STRING,
      subLoc2Code: STRING,
      subLoc2Desc: STRING,
      subLoc2Type: STRING,
      telephone: STRUCT<
        externalTelephoneNumber: STRING,
        internalTelephoneNumber: STRING,
        externalExtensionNumber: STRING,
        internalExtensionNumber: STRING
      > 
    >>,
    shorterNames: STRUCT<medName: STRING,shortName: STRING>,
    socialMedias: ARRAY<STRUCT<code: STRING, desc: STRING, url: STRING>>,
    storeNumber: STRING,
    storePlanningChannel: STRUCT<
      marketCode: STRING,marketDesc: STRING,
      planningChannelCode: STRING,
      planningChannelDesc: STRING,
      planningChannelTypeCode: STRING,
      planningChannelTypeDesc: STRING,
      pricingChannels: ARRAY<STRUCT<pricingChannelCode: STRING, pricingChannelDesc: STRING>>
    >,
    storeReserveAreaInfo: STRUCT<desc: STRING,name: STRING>,
    telephone: STRUCT<externalTelephoneNumber: STRING,internalTelephoneNumber: STRING,externalExtensionNumber: STRING,internalExtensionNumber: STRING>,
    timeZoneName: STRING,
    typeCategoryCode: STRING,
    typeCategoryDesc: STRING,
    typeCode: STRING,
    typeDesc: STRING,
    typeIsInventoryStore: BOOLEAN,
    typeIsSellingStore: BOOLEAN,
    uiEligbilityTypes: ARRAY<STRUCT<code: STRING,desc: STRING>>,
    locationAddlInfo: STRUCT<grossSquareFootage: INT>,
    locationAmenities: STRUCT<
      amenities: ARRAY<STRUCT<amenityCategoryCode: STRING,amenityCategoryDesc: STRING,code: STRING,desc: STRING>>,
      subLocation1s: ARRAY<STRUCT<
        code: STRING,
        desc: STRING,
        subLocation2s: ARRAY<STRUCT<
          amenities: ARRAY<STRUCT<amenityCategoryCode: STRING,amenityCategoryDesc: STRING,code: STRING,desc: STRING>>,
          code: STRING, 
          desc: STRING,
          detailedHours:STRUCT<
            regularWeeklyScheduleDays0Sun6Sat: ARRAY<STRUCT<
              openAbsoluteTime: STRING,
              closeAbsoluteTime: STRING,
              dayOfWeekStart0Sun6Sat: INT,
              dayOfWeekEnd0Sun6Sat: INT
              >>
          >,
          telephone: STRUCT<externalTelephoneNumber: STRING,internalTelephoneNumber: STRING,externalExtensionNumber: STRING,internalExtensionNumber: STRING>
        >>
      >>
    >,
    displayDepartments: ARRAY<STRUCT<code: STRING,desc: STRING>>,
    selectAssocStores: ARRAY<STRUCT<
      locationDesc: STRING,
      locationNumber: STRING,
      locationTypeCode: STRING,
      locationTypeDesc: STRING,
      storeDesc: STRING,
      storeDisplayName: STRING,
      storeNumber: STRING,
      storeTypeCode: STRING,
      storeTypeDesc: STRING
    >>,
    dma: STRING
  >,
  currentDataUpdatedTimeStamp STRING,
  currentDataUpdatedTimeStampMS STRING,
  storeNumber STRING,
  rootLevelChanges ARRAY<STRUCT<node: STRING,attribute: STRING>>
)
USING delta
LOCATION 's3://nap-org-prod/delta/objects/daily/deltaStoreObjectProd/';

ALTER TABLE nsp.org_store_dim_delta ALTER COLUMN storeNumber FIRST;

-- Merges From Landing Table into the Dim Table 
MERGE INTO nsp.org_store_dim_delta as target
USING temp_store_delta_v2 s
ON (target.storeNumber = s.storeNumber)
when matched then
   update set 
      target.currentData = s.currentData,
      target.currentdataupdatedtimestamp = s.currentdataupdatedtimestamp,
      target.currentdataupdatedtimestampms = s.currentdataupdatedtimestampms,
      target.rootlevelchanges = s.rootlevelchanges
when not matched
   then insert 
   (
    currentData, 
    currentdataupdatedtimestamp, 
    currentdataupdatedtimestampms, 
    storeNumber, 
    rootlevelchanges
  )
  VALUES 
   (
    s.currentData, 
    s.currentdataupdatedtimestamp, 
    s.currentdataupdatedtimestampms, 
    s.storeNumber, 
    s.rootlevelchanges
  )
;

-- Deletes Data Files
VACUUM nsp.org_store_dim_delta;

-- Optimizes by StoreNumber
OPTIMIZE nsp.org_store_dim_delta ZORDER BY (storeNumber);
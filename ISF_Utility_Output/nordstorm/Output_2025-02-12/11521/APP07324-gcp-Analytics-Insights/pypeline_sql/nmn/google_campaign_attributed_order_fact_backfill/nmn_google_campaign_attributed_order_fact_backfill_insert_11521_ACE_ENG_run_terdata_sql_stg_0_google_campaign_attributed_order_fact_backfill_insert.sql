/*
T2/Table Name: T2DL_DAS_NMN.google_campaign_attributed_order_fact
Team/Owner: NMN / Customer Engagement Analytics
Usage: This script to Copy data from old output table to the new output table through a one-time DAG
*/

SET QUERY_BAND = 'App_ID=APP08998;
     DAG_ID=nmn_google_campaign_attributed_order_fact_backfill_insert_11521_ACE_ENG;
     Task_Name=google_campaign_attributed_order_fact_backfill_insert;'
     FOR SESSION VOLATILE;

--- QB blob omitted for space

INSERT INTO T2DL_DAS_NMN.google_campaign_attributed_order_fact
SELECT
      Advertiser ,
      TRIM(CAST(AdvertiserId as VARCHAR(50))) as AdvertiserId ,
      TRIM(CAST(OrderId as VARCHAR(50))) as OrderId ,
      TRIM(CAST(CreativeId as VARCHAR(50))) as CreativeId ,
      TRIM(CAST(LineItemId as VARCHAR(50))) as LineItemId ,
      Ord ,
      GfpActivityAdEventTime ,
      GfpActivityAdEventType ,
      TRIM(CAST(AdUnitId as VARCHAR(50))) as AdUnitId ,
      AttConsentStatus ,
      AudienceSegmentIds ,
      BandWidth ,
      TRIM(CAST(BandwidthGroupId as VARCHAR(2))) as BandwidthGroupId ,
      TRIM(CAST(BandwidthId as VARCHAR(50))) as BandwidthId ,
      Browser ,
      TRIM(CAST(BrowserId as VARCHAR(50))) as BrowserId ,
      Buyer ,
      CreativeSize ,
      CreativeSizeDelivered ,
      CreativeVersion ,
      CustomTargeting ,
      DealId ,
      DealType ,
      DeviceCategory ,
      "Domain" ,
      EventKeyPart ,
      EventTimeUsec2 ,
      TRIM(CAST(GfpActivityId as VARCHAR(50))) as GfpActivityId ,
      GfpActivityName ,
      IsCompanion ,
      IsInterstitial ,
      KeyPart ,
      MobileAppId ,
      MobileCapability ,
      MobileCarrier ,
      MobileDevice ,
      NativeFormat ,
      NativeStyle ,
      OS ,
      TRIM(CAST(OSId as VARCHAR(50))) as OSId ,
      OSVersion ,
      OptimizationType ,
      ProcessingDateAndHour ,
      Product ,
      PublisherProvidedID ,
      Quantity ,
      RefererURL ,
      RequestLanguage ,
      RequestedAdUnitSizes ,
      Revenue ,
      ServingRestriction ,
      TargetedCustomCriteria ,
      "Time" ,
      TimeUsec2 ,
      UserId ,
      UserIdentifierStatus ,
      TimeAsDate ,
      FileDate ,
      dw_sys_load_tmstp
FROM T2DL_DAS_NMN.google_campaign_attributed_order_fact_old;

SET QUERY_BAND = NONE FOR SESSION;

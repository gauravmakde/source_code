create temporary view kafka_vw as
SELECT
      Cast(headers['SystemTime'] AS BIGINT) last_updated_millis
	  , valueupdatedtime
	  , rmsskuid
	  , locationid
	  , immediatelysellableqty
	  , stockonhandqty
	  , unavailableqty
	  , custorderreturn
	  , preinspectcustomerreturn
	  , unusableqty
	  , rtvreqstresrv
	  , metreqstresrv
	  , tcpreview
	  , tcclubhouse
	  , tcmini1
	  , tcmini2
	  , tcmini3
	  , problem
	  , damagedreturn
	  , damagedcosmeticreturn
	  , ertmholds
	  , ndunavailableqty
	  , pbholdsqty
	  , comcoholds
	  , wmholds
	  , storereserve
	  , tcholds
	  , returnsholds
	  , fpholds
	  , transfersreserveqty
	  , returntovendorqty
	  , intransitqty
	  , storetransferreservedqty
	  , storetransferexpectedqty
	  , backorderreserveqty
	  , omsbackorderreserveqty
	  , onreplenishment
	  , lastreceiveddate
	  , locationtype
	  , availableforonorder
	  , epmid
	  , upc
	  , returninspectionqty
	  , receivingqty
	  , damagedqty
	  , holdqty
FROM kafka_src;

insert overwrite table ascp.inventory_stock_quantity_orc_kafka
select 
     last_updated_millis
	,valueupdatedtime
	,rmsskuid
	,locationid
	,immediatelysellableqty
	,stockonhandqty
	,unavailableqty
	,custorderreturn
	,preinspectcustomerreturn
	,unusableqty
	,rtvreqstresrv
	,metreqstresrv
	,tcpreview
	,tcclubhouse
	,tcmini1
	,tcmini2
	,tcmini3
	,problem
	,damagedreturn
	,damagedcosmeticreturn
	,ertmholds
	,ndunavailableqty
	,pbholdsqty
	,comcoholds
	,wmholds
	,storereserve
	,tcholds
	,returnsholds
	,fpholds
	,transfersreserveqty
	,returntovendorqty
	,intransitqty
	,storetransferreservedqty
	,storetransferexpectedqty
	,backorderreserveqty
	,omsbackorderreserveqty
	,onreplenishment
	,lastreceiveddate
	,locationtype
	,availableforonorder
	,epmid
	,upc
	,returninspectionqty
	,receivingqty
	,damagedqty
	,holdqty
	,CASE
		when (cast(locationid as integer) >= 0	 and cast(locationid as integer) <= 25	   ) then '1'
		when (cast(locationid as integer) >  25	 and cast(locationid as integer) <= 136    ) then '2'
		when (cast(locationid as integer) >  136 and cast(locationid as integer) <= 187    ) then '3'
		when (cast(locationid as integer) >  187 and cast(locationid as integer) <= 239    ) then '4'
		when (cast(locationid as integer) >  239 and cast(locationid as integer) <= 270    ) then '5'
		when (cast(locationid as integer) >  270 and cast(locationid as integer) <= 328    ) then '6'
		when (cast(locationid as integer) >  328 and cast(locationid as integer) <= 348    ) then '7'
		when (cast(locationid as integer) >  348 and cast(locationid as integer) <= 379    ) then '8'
		when (cast(locationid as integer) >  379 and cast(locationid as integer) <= 433    ) then '9'
		when (cast(locationid as integer) >  433 and cast(locationid as integer) <= 521    ) then '10'
		when (cast(locationid as integer) >  521 and cast(locationid as integer) <= 567    ) then '11'
		when (cast(locationid as integer) >  567 and cast(locationid as integer) <= 600    ) then '12'
		when (cast(locationid as integer) >  600 and cast(locationid as integer) <= 673    ) then '13'
		when (cast(locationid as integer) >  673 and cast(locationid as integer) <= 734    ) then '14'
		when (cast(locationid as integer) >  734 and cast(locationid as integer) <= 768    ) then '15'
		when (cast(locationid as integer) >  768 and cast(locationid as integer) <= 797    ) then '16'
		when (cast(locationid as integer) >  797 and cast(locationid as integer) <= 832    ) then '17'
		when (cast(locationid as integer) >  832 and cast(locationid as integer) <= 873    ) then '18'
		when (cast(locationid as integer) >  873 and cast(locationid as integer) <= 896    ) then '19'
		when (cast(locationid as integer) >  896 and cast(locationid as integer) <= 5186581) then '20'
		when (cast(locationid as integer) >  5186581) then '21'
	END grp 
from kafka_vw;


-- deduplicate and write to s3:

--- partition '1'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '1')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '1'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '1'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '1')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '1'
;

--- partition '2'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '2')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '2'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '2'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '2')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '2'
;

--- partition '3'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '3')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '3'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '3'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '3')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '3'
;

--- partition '4'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '4')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '4'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '4'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '4')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '4'
;

--- partition '5'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '5')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '5'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '5'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '5')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '5'
;

--- partition '6'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '6')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '6'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '6'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '6')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '6'
;

--- partition '7'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '7')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '7'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '7'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '7')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '7'
;

--- partition '8'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '8')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '8'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '8'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '8')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '8'
;

--- partition '9'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '9')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '9'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '9'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '9')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '9'
;

--- partition '10'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '10')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '10'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '10'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '10')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '10'
;

--- partition '11'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '11')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '11'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '11'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '11')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '11'
;

--- partition '12'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '12')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '12'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '12'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '12')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '12'
;

--- partition '13'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '13')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '13'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '13'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '13')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '13'
;

--- partition '14'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '14')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '14'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '14'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '14')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '14'
;

--- partition '15'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '15')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '15'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '15'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '15')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '15'
;

--- partition '16'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '16')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '16'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '16'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '16')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '16'
;

--- partition '17'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '17')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '17'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '17'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '17')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '17'
;

--- partition '18'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '18')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '18'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '18'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '18')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '18'
;

--- partition '19'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '19')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '19'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '19'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '19')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '19'
;

--- partition '20'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '20')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '20'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '20'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '20')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '20'
;

--- partition '21'
INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_tmp PARTITION (grp = '21')
SELECT
	  s.last_updated_millis
	, s.valueupdatedtime
	, s.rmsskuid
	, s.locationid
	, s.immediatelysellableqty
	, s.stockonhandqty
	, s.unavailableqty
	, s.custorderreturn
	, s.preinspectcustomerreturn
	, s.unusableqty
	, s.rtvreqstresrv
	, s.metreqstresrv
	, s.tcpreview
	, s.tcclubhouse
	, s.tcmini1
	, s.tcmini2
	, s.tcmini3
	, s.problem
	, s.damagedreturn
	, s.damagedcosmeticreturn
	, s.ertmholds
	, s.ndunavailableqty
	, s.pbholdsqty
	, s.comcoholds
	, s.wmholds
	, s.storereserve
	, s.tcholds
	, s.returnsholds
	, s.fpholds
	, s.transfersreserveqty
	, s.returntovendorqty
	, s.intransitqty
	, s.storetransferreservedqty
	, s.storetransferexpectedqty
	, s.backorderreserveqty
	, s.omsbackorderreserveqty
	, s.onreplenishment
	, s.lastreceiveddate
	, s.locationtype
	, s.availableforonorder
	, s.epmid
	, s.upc
	, s.returninspectionqty
	, s.receivingqty
	, s.damagedqty
	, s.holdqty
FROM
     (SELECT
		  u.last_updated_millis
		, u.valueupdatedtime
		, u.rmsskuid
		, u.locationid
		, u.immediatelysellableqty
		, u.stockonhandqty
		, u.unavailableqty
		, u.custorderreturn
		, u.preinspectcustomerreturn
		, u.unusableqty
		, u.rtvreqstresrv
		, u.metreqstresrv
		, u.tcpreview
		, u.tcclubhouse
		, u.tcmini1
		, u.tcmini2
		, u.tcmini3
		, u.problem
		, u.damagedreturn
		, u.damagedcosmeticreturn
		, u.ertmholds
		, u.ndunavailableqty
		, u.pbholdsqty
		, u.comcoholds
		, u.wmholds
		, u.storereserve
		, u.tcholds
		, u.returnsholds
		, u.fpholds
		, u.transfersreserveqty
		, u.returntovendorqty
		, u.intransitqty
		, u.storetransferreservedqty
		, u.storetransferexpectedqty
		, u.backorderreserveqty
		, u.omsbackorderreserveqty
		, u.onreplenishment
		, u.lastreceiveddate
		, u.locationtype
		, u.availableforonorder
		, u.epmid
		, u.upc
		, u.returninspectionqty
		, u.receivingqty
		, u.damagedqty
		, u.holdqty
        , ROW_NUMBER() OVER (PARTITION BY u.rmsskuid, u.locationid, COALESCE(u.locationtype, '') ORDER BY u.last_updated_millis DESC) AS rn
      FROM(
           SELECT 
				  CAST(0 AS BIGINT) last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
		   FROM ascp.inventory_stock_quantity_orc_v1
		   where grp = '21'
           UNION ALL
           SELECT 
				  last_updated_millis
				, valueupdatedtime
				, rmsskuid
				, locationid
				, immediatelysellableqty
				, stockonhandqty
				, unavailableqty
				, custorderreturn
				, preinspectcustomerreturn
				, unusableqty
				, rtvreqstresrv
				, metreqstresrv
				, tcpreview
				, tcclubhouse
				, tcmini1
				, tcmini2
				, tcmini3
				, problem
				, damagedreturn
				, damagedcosmeticreturn
				, ertmholds
				, ndunavailableqty
				, pbholdsqty
				, comcoholds
				, wmholds
				, storereserve
				, tcholds
				, returnsholds
				, fpholds
				, transfersreserveqty
				, returntovendorqty
				, intransitqty
				, storetransferreservedqty
				, storetransferexpectedqty
				, backorderreserveqty
				, omsbackorderreserveqty
				, onreplenishment
				, lastreceiveddate
				, locationtype
				, availableforonorder
				, epmid
				, upc
				, returninspectionqty
				, receivingqty
				, damagedqty
				, holdqty
			FROM ascp.inventory_stock_quantity_orc_kafka
		    WHERE grp = '21'
           ) u
     ) s
WHERE s.rn = 1;

INSERT OVERWRITE TABLE ascp.inventory_stock_quantity_orc_v1 PARTITION (grp = '21')
SELECT
	  valueupdatedtime
	, rmsskuid
	, locationid
	, immediatelysellableqty
	, stockonhandqty
	, unavailableqty
	, custorderreturn
	, preinspectcustomerreturn
	, unusableqty
	, rtvreqstresrv
	, metreqstresrv
	, tcpreview
	, tcclubhouse
	, tcmini1
	, tcmini2
	, tcmini3
	, problem
	, damagedreturn
	, damagedcosmeticreturn
	, ertmholds
	, ndunavailableqty
	, pbholdsqty
	, comcoholds
	, wmholds
	, storereserve
	, tcholds
	, returnsholds
	, fpholds
	, transfersreserveqty
	, returntovendorqty
	, intransitqty
	, storetransferreservedqty
	, storetransferexpectedqty
	, backorderreserveqty
	, omsbackorderreserveqty
	, onreplenishment
	, lastreceiveddate
	, locationtype
	, availableforonorder
	, epmid
	, upc
	, returninspectionqty
	, receivingqty
	, damagedqty
	, holdqty
FROM ascp.inventory_stock_quantity_orc_tmp 
WHERE grp = '21'
;

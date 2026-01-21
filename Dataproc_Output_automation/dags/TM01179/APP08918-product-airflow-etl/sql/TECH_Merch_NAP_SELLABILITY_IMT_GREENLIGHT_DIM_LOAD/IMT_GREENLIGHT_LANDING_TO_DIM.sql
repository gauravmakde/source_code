
/*SET QUERY_BAND='AppName=NAP-Merch-Datalab-load;AppRelease=1;AppFreq=Daily;AppPhase=stg-dim;AppSubArea=GREEN_LIGHT;'  UPDATE FOR SESSION;*/
BEGIN TRANSACTION;



MERGE INTO {{params.gcp_project_id}}.t2dl_das_ete_instrumentation.greenlight_dim AS tgt
USING (SELECT stylegroupnumber, nordstromcolorcode, divisionname, subdivisionname, departmentname, departmentcode, ndirectproductlabel, vpn, nordstromcolorname, longdescription, sellingstatus, PARSE_DATE('%F', createddate) AS createddate, PARSE_DATE('%F', livedate) AS livedate, PARSE_DATE('%F', featureddate) AS featureddate, inventory, isfulfillmentcenteronhand, PARSE_DATE('%F', SUBSTR(firstreceiveddate, 1, 10)) AS firstreceiveddate, dropshipfeedinventory, PARSE_DATE('%F', SUBSTR(dropshipfeeddateinventory, 1, 10)) AS dropshipfeeddateinventory, isfulllinestoreonhand, flsapproved, isfulfillmentcenteronorder, PARSE_DATE('%F', backorderdate) AS backorderdate, copystatus, productcopyvalidationstate, PARSE_DATE('%F', copycompletedate) AS copycompletedate, legacymain, imagestatus, PARSE_DATE('%F', imagecompletedate) AS imagecompletedate, swatch, assetsource, live, published, PARSE_DATE('%F', initialpublishdate) AS initialpublishdate, PARSE_DATE('%F', lastpublishdate) AS lastpublishdate, prepubapproved, unpublished, unpublishreasons, pubhold, pubholdreasons, PARSE_DATE('%F', screadydate) AS screadydate, scpriority, producttype1, producttype2, anniversary, styleid, available, rmsskuids, epmchoiceid, productlabelcode, PARSE_DATE('%F', modifieddate) AS modifieddate
    FROM {{params.gcp_project_id}}.t2dl_das_ete_instrumentation.greenlight_ldg
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY rmsskuids ORDER BY published DESC)) = 1) AS t0
ON LOWER(t0.rmsskuids) = LOWER(tgt.rmsskuids)
WHEN MATCHED THEN UPDATE SET
    stylegroupnumber = t0.stylegroupnumber,
    nordstromcolorcode = t0.nordstromcolorcode,
    divisionname = t0.divisionname,
    subdivisionname = t0.subdivisionname,
    departmentname = t0.departmentname,
    departmentcode = t0.departmentcode,
    ndirectproductlabel = t0.ndirectproductlabel,
    vpn = t0.vpn,
    nordstromcolorname = t0.nordstromcolorname,
    longdescription = t0.longdescription,
    sellingstatus = t0.sellingstatus,
    createddate = t0.createddate,
    livedate = t0.livedate,
    featureddate = t0.featureddate,
    inventory = t0.inventory,
    isfulfillmentcenteronhand = t0.isfulfillmentcenteronhand,
    firstreceiveddate = t0.firstreceiveddate,
    dropshipfeedinventory = t0.dropshipfeedinventory,
    dropshipfeeddateinventory = t0.dropshipfeeddateinventory,
    isfulllinestoreonhand = t0.isfulllinestoreonhand,
    flsapproved = t0.flsapproved,
    isfulfillmentcenteronorder = t0.isfulfillmentcenteronorder,
    backorderdate = t0.backorderdate,
    copystatus = t0.copystatus,
    productcopyvalidationstate = t0.productcopyvalidationstate,
    copycompletedate = t0.copycompletedate,
    legacymain = t0.legacymain,
    imagestatus = t0.imagestatus,
    imagecompletedate = t0.imagecompletedate,
    swatch = t0.swatch,
    assetsource = t0.assetsource,
    live = t0.live,
    published = t0.published,
    initialpublishdate = t0.initialpublishdate,
    lastpublishdate = t0.lastpublishdate,
    prepubapproved = t0.prepubapproved,
    unpublished = t0.unpublished,
    unpublishreasons = t0.unpublishreasons,
    pubhold = t0.pubhold,
    pubholdreasons = t0.pubholdreasons,
    screadydate = t0.screadydate,
    scpriority = t0.scpriority,
    producttype1 = t0.producttype1,
    producttype2 = t0.producttype2,
    anniversary = t0.anniversary,
    styleid = t0.styleid,
    available = t0.available,
    epmchoiceid = t0.epmchoiceid,
    productlabelcode = t0.productlabelcode,
    modifieddate = t0.modifieddate,
    dw_batch_user = 'T2DL_NAP_DAS_BATCH',
    dw_sys_udpt_tmstp = CAST(FORMAT_TIMESTAMP('%F %H:%M:%E3S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
WHEN NOT MATCHED THEN INSERT VALUES(t0.stylegroupnumber, t0.nordstromcolorcode, t0.divisionname, t0.subdivisionname, t0.departmentname, t0.departmentcode, t0.ndirectproductlabel, t0.vpn, t0.nordstromcolorname, t0.longdescription, t0.sellingstatus, t0.createddate, t0.livedate, t0.featureddate, t0.inventory, t0.isfulfillmentcenteronhand, t0.firstreceiveddate, t0.dropshipfeedinventory, t0.dropshipfeeddateinventory, t0.isfulllinestoreonhand, t0.flsapproved, t0.isfulfillmentcenteronorder, t0.backorderdate, t0.copystatus, t0.productcopyvalidationstate, t0.copycompletedate, t0.legacymain, t0.imagestatus, t0.imagecompletedate, t0.swatch, t0.assetsource, t0.live, t0.published, t0.initialpublishdate, t0.lastpublishdate, t0.prepubapproved, t0.unpublished, t0.unpublishreasons, t0.pubhold, t0.pubholdreasons, t0.screadydate, t0.scpriority, t0.producttype1, t0.producttype2, t0.anniversary, t0.styleid, t0.available, t0.rmsskuids, t0.epmchoiceid, t0.productlabelcode, t0.modifieddate, 'T2DL_NAP_DAS_BATCH', CAST(FORMAT_TIMESTAMP('%F %H:%M:%E3S', CURRENT_DATETIME('PST8PDT')) AS DATETIME), CAST(FORMAT_TIMESTAMP('%F %H:%M:%E3S', CURRENT_DATETIME('PST8PDT')) AS DATETIME));

COMMIT TRANSACTION;



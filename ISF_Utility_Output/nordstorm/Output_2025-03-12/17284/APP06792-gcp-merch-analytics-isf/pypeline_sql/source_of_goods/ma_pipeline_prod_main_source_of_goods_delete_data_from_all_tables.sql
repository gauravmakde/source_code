/*
Name:                delete_data_from_all_tables
APPID-Name:          APP09479
Purpose:             Delete data from Clarity secure schema tables after testing in Merch Analytics repo Dev instance
Variable(s):         {{dsa_ai_secure_schema}} PROTO_DSA_AI_BASE_VWS for dev or PRD_NAP_DSA_AI_BASE_VWS for prod
Author(s):           Jevon Barlas
Date Created:        9/4/2024
Date Last Updated:   9/4/2024
*/

-- delete all data from inflows table
delete from {dsa_ai_secure_schema}.source_of_goods_inflows_fact
;

-- delete all data from time range table
delete from {dsa_ai_secure_schema}.source_of_goods_range_fact
;

-- delete all data from final fact table
delete from {dsa_ai_secure_schema}.source_of_goods_fact
;

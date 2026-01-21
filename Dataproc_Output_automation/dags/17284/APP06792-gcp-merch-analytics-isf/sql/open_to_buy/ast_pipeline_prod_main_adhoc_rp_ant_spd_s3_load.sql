

DROP TABLE IF EXISTS `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_adhoc;


CREATE TABLE IF NOT EXISTS `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_adhoc (
week_idnt STRING,
dept STRING,
dept_desc STRING,
cls_idnt STRING,
scls_idnt STRING,
supp_idnt STRING,
supp_name STRING,
banner_id STRING,
ft_id STRING,
rp_antspnd_u STRING,
rp_antspnd_c STRING,
rp_antspnd_r STRING
) ; --CLUSTER BY week_idnt, banner_id, dept, cls_idnt;


--GRANT SELECT ON t2dl_das_open_to_buy.rp_anticipated_spend_adhoc TO PUBLIC



DROP TABLE IF EXISTS `{{params.gcp_project_id}}`.t2dl_das_open_to_buy.rp_anticipated_spend_adhoc ;
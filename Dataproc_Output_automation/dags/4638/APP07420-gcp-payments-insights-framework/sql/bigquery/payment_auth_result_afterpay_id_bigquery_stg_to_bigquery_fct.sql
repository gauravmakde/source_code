

BEGIN

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_afterpay_id_fact
WHERE authorization_result_id IN (SELECT authorization_result_id
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_afterpay_id_ldg);


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.payment_authorization_result_afterpay_id_fact (authorization_result_id, bin_type)
(SELECT *
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_afterpay_id_ldg);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.payment_authorization_result_afterpay_id_ldg;

END;


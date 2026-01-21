-- the table comparison is limited by using the data only for the last 3 days
CREATE TEMPORARY VIEW missing_ids_view AS
SELECT DISTINCT
-- loyalty IDs has 'lylty::' prefix that are removed by using the 'substr' command
    substr(
            decode(dlq.headers['usid'],'UTF-8'), 8
    ) AS missing_id
FROM acp_etl_landing.loyaltymember_object_model_dlq AS dlq
WHERE (
        (dlq.year=year(current_date) AND dlq.month=month(current_date) AND dlq.day = day(current_date)) OR
        (dlq.year=year(date_sub(current_date, 1)) AND dlq.month=month(date_sub(current_date, 1)) AND dlq.day = day(date_sub(current_date, 1))) OR
        (dlq.year=year(date_sub(current_date, 2)) AND dlq.month=month(date_sub(current_date, 2)) AND dlq.day = day(date_sub(current_date, 2)))
    )
  AND NOT EXISTS (
    SELECT 1
    FROM object_model.loyaltymember_object_model_avro AS om
    WHERE dlq.headers['usid'] = om.headers['usid'] AND
        (
            (om.year=year(current_date) AND om.month=month(current_date) AND om.day = day(current_date)) OR
            (om.year=year(date_sub(current_date, 1)) AND om.month=month(date_sub(current_date, 1)) AND om.day = day(date_sub(current_date, 1))) OR
            (om.year=year(date_sub(current_date, 2)) AND om.month=month(date_sub(current_date, 2)) AND om.day = day(date_sub(current_date, 2)))
        )
    );

INSERT OVERWRITE missing_create_ids
SELECT * FROM missing_ids_view;

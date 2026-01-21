import logging
from datetime import datetime
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import (
    Client,
    TableReference
)
from google.cloud.bigquery.table import RowIterator


class FetchKafkaBatchRunIdOperator(BaseOperator):
    """
    Fetches the latest batch run id from the offsets table.
    If the source table is not provided, it will only fetch the latest batch run id from the offsets table.
    If the offsets table does not have any records, it will aggregate records with batch_run_id greater than the default_latest_run_id.
    If the source table is provided, it will aggregate records with batch_run_id greater than the latest batch_run_id from the offsets table.
    If there are no records in the source table, None will be returned.

    :param gcp_project_id: The GCP project ID.
    :param gcp_connection_id: The Airflow GCP connection ID.
    :param gcp_region: The BigQuery tables' GCP region.
    :param topic_name: The Kafka topic name.
    :param consumer_group_name: The Kafka consumer group name.
    :param offsets_table: Fully qualified Kafka Offsets table name in the format: `project_id`.`dataset_id`.`table_id`.
    :param source_table: Fully qualified Kafka Offsets source table name in the format: `project_id`.`dataset_id`.`table_id`.
    :param default_latest_run_id: The default latest batch run id.
    """

    @apply_defaults
    def __init__(self, *,
                 gcp_project_id: str,
                 gcp_connection_id: str,
                 gcp_region: str,
                 topic_name: str,
                 consumer_group_name: str,
                 offsets_table: str,
                 source_table: Optional[str] = None,
                 default_latest_run_id: int = int(datetime.today().strftime('%Y%m%d000000')),
                 **kwargs):
        super().__init__(**kwargs)
        self.gcp_project_id = gcp_project_id
        self.gcp_connection_id = gcp_connection_id
        self.gcp_region = gcp_region
        self.topic_name = topic_name
        self.consumer_group_name = consumer_group_name
        self.source_table = source_table
        self.offsets_table = offsets_table
        self.default_latest_run_id = default_latest_run_id

    def execute(self, context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_connection_id)
        client = hook.get_client(project_id=self.gcp_project_id, location=self.gcp_region)

        latest_run_id = self.__get_latest_batch_run_id(client)

        if self.source_table:
            self.__refresh_external_metadata_cache(client)
            self.__aggregate_from_source(
                client,
                latest_run_id or self.default_latest_run_id,
            )
            latest_run_id = self.__get_latest_batch_run_id(client)
        logging.info(f'Latest batch run id: {latest_run_id}')
        return latest_run_id if latest_run_id else int((datetime.today()).strftime('%Y%m%d%H%M%S'))

    def __get_latest_batch_run_id(self, client: Client) -> Optional[int]:
        query = f"""
            SELECT batch_run_id
            FROM {self.offsets_table}
            WHERE topic_name = '{self.topic_name}' 
                AND consumer_group_name = '{self.consumer_group_name}'
            ORDER BY batch_run_id DESC
            LIMIT 1
        """
        row = next(self.__execute_query(client, query), None)
        return row[0] if row else int((datetime.today()).strftime('%Y%m%d%H%M%S'))

    def __refresh_external_metadata_cache(self, client: Client):
        table_ref = self.source_table.replace('`', '')
        table = client.get_table(TableReference.from_string(table_ref))
        if table.table_type == "EXTERNAL" and table.external_data_configuration and table.external_data_configuration.hive_partitioning:
            if table.external_data_configuration.to_api_repr().get('metadataCacheMode') == 'MANUAL':
                prefix: str = table.external_data_configuration.hive_partitioning.source_uri_prefix
                first_partition_index = prefix.find('{')
                prefix = prefix[:first_partition_index] if first_partition_index > 0 else prefix
                self.__execute_query(
                    client,
                    f"""
                        CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('{table_ref}', ['{prefix}topic_name={self.topic_name}/consumer_group_name={self.consumer_group_name}/*']) 
                    """
                )

    def __aggregate_from_source(self, client: Client, latest_batch_run_id: int) -> None:
        query = f"""
            INSERT INTO {self.offsets_table} 
            (topic_name, consumer_group_name, batch_run_id, start_offset, end_offset, batch_row_count, inserted_timestamp)
            WITH
              topic_consumer_data AS (
                SELECT
                  topic_name,
                  consumer_group_name,
                  batch_run_id,
                  batch_row_count,
                  inserted_timestamp,
                  FIRST_VALUE(start_offset) OVER (item_window)  AS start_offset,
                  LAST_VALUE(end_offset) OVER (item_window)     AS end_offset
                FROM {self.source_table}
                WHERE topic_name = '{self.topic_name}'
                    AND consumer_group_name = '{self.consumer_group_name}' 
                    AND batch_run_id > {latest_batch_run_id}
                WINDOW 
                  item_window AS (
                    PARTITION BY topic_name, consumer_group_name
                    ORDER BY batch_run_id, inserted_timestamp 
                  ) 
              ),
              aggregated AS (
                SELECT
                  topic_name,
                  consumer_group_name,
                  MAX(batch_run_id)         AS batch_run_id,
                  SUM(batch_row_count)      AS batch_row_count,
                  MAX(inserted_timestamp)   AS inserted_timestamp,
                  ANY_VALUE(start_offset)   AS start_offset,
                  ANY_VALUE(end_offset)     AS end_offset,
                FROM topic_consumer_data
                GROUP BY topic_name, consumer_group_name
              )
            SELECT 
              topic_name, 
              consumer_group_name, 
              batch_run_id, 
              start_offset, 
              end_offset, 
              batch_row_count, 
              inserted_timestamp
            FROM aggregated
        """
        self.__execute_query(client, query)

    def __execute_query(self, client: Client, query: str) -> RowIterator:
        logging.info(f'Executing query: {query}')
        return client.query(query).result()

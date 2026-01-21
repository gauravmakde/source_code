import logging
from datetime import datetime
from datetime import timezone

from airflow.models.dag import DagModel


def get_datetime_for_sensor(current_execution_datetime, **kwargs):
    external_dag_id = kwargs["params"]["external_dag_id"]
    external_dag = DagModel.get_dagmodel(dag_id=external_dag_id)
    schedule_time = external_dag.next_dagrun.time()
    current_execution_date = current_execution_datetime.date()
    lookup_datetime = datetime.combine(
        current_execution_date, schedule_time, timezone.utc
    )
    logging.info(f"Datetime to sense '{external_dag_id}': {lookup_datetime}")
    return lookup_datetime

from datetime import datetime, timedelta, timezone

def get_dag_run_date_range():
  # Airflow execution date is always current_date-1 hence subtract 2 days
  previous_date_time = datetime.now() - timedelta(days=2)
  # enlarge time "window" to start at 12:00:00 UTC
  previous_run_date = previous_date_time.replace(hour=0, minute=0, second=0, microsecond=0)
  # Airflow execution date is always current_date-1
  current_date_time = datetime.now() - timedelta(days=1)

  return {
    'execution_date_lte': current_date_time.isoformat(),
    'execution_date_gte': previous_run_date.isoformat()
  }

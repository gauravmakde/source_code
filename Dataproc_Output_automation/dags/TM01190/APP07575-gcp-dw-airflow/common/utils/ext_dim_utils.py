import logging
from datetime import datetime
from datetime import date
from airflow.operators.email_operator import EmailOperator
from google.cloud import storage


def __extract_dma_ymd(key, prefix):
    start_pos = len(prefix)
    year = int(key[start_pos : start_pos + 4])
    month = int(key[start_pos + 5 : start_pos + 7])
    day = int(key[start_pos + 8 : start_pos + 10])
    return year, month, day


def __get_most_recent_dma_source_key(dma_source_bucket, prefix):
    # Initialize a GCS client
    client = storage.Client()
    bucket = client.bucket(dma_source_bucket)

    # List all objects in the bucket with the specified prefix
    blobs = bucket.list_blobs(prefix=prefix)

    # Filter the objects to include only those ending with '.dat'
    file_keys = [blob.name for blob in blobs if blob.name.endswith(".dat")]

    # Sort the keys based on the date extracted from their names
    sorted_keys = sorted(file_keys, key=lambda key: __extract_dma_ymd(key, prefix))

    # Check if there are no sorted keys and raise an exception if true
    if len(sorted_keys) == 0:
        raise Exception(
            f"DMA files have not been found in path: gs://{dma_source_bucket}/{prefix}"
        )

    # Get the most recent key (the last one in the sorted list)
    most_recent_key = sorted_keys.pop()
    return most_recent_key


def __get_dma_last_update_date(dma_source_bucket, dma_source_prefix, dma_folder):
    source_prefix = f"{dma_source_prefix}/{dma_folder}/"
    most_recent_key = __get_most_recent_dma_source_key(dma_source_bucket, source_prefix)
    year, month, day = __extract_dma_ymd(most_recent_key, source_prefix)
    last_update_date = datetime.combine(
        date(year=year, month=month, day=day), datetime.min.time()
    )
    return last_update_date


def alert_on_missing_dma(
    dma_source_bucket,
    dma_source_prefix,
    dma_folder,
    dma_alert_delta,
    dma_alert_day,
    alert_email_addr,
    dag,
    context=None,
):
    current_date = datetime.combine(date.today(), datetime.min.time())
    dma_last_update_date = __get_dma_last_update_date(
        dma_source_bucket, dma_source_prefix, dma_folder
    )
    dma_update_delta_in_days = abs((current_date - dma_last_update_date).days)
    if dma_update_delta_in_days > int(dma_alert_delta) and current_date.day == int(
        dma_alert_day
    ):
        logging.info(f"Sending the alert to {alert_email_addr.split(',')}")
        email = EmailOperator(
            task_id="email_task",
            to=alert_email_addr.split(","),
            subject="DMA files were not updated for more than 3 months",
            html_content=f"Last update: {dma_last_update_date.strftime('%Y/%m/%d')}",
            dag=dag,
        )
        email.execute(context=context)
    else:
        logging.info("No alerts needed")


def send_dma_lag_metric(dma_source_bucket, dma_source_prefix, dma_folder, newrelic):
    current_date = datetime.combine(date.today(), datetime.min.time())
    source_prefix = f"{dma_source_prefix}/{dma_folder}/"
    dma_last_update_date = __get_most_recent_dma_source_key(
        dma_source_bucket, source_prefix
    )
    year, month, day = __extract_dma_ymd(dma_last_update_date, source_prefix)
    dma_last_update_date_formatted = datetime(year, month, day).strftime("%Y/%m/%d")

    newrelic.send_metric(
        name="napdw.org_dma_lag",
        value=str((current_date - dma_last_update_date).days),
        attributes={"last_update": dma_last_update_date_formatted},
    )


def copy_dma_data(source_bucket, source_prefix, dest_bucket, dest_key):
    dma_source_key = __get_most_recent_dma_source_key(source_bucket, source_prefix)

    # Initialize a GCS client
    client = storage.Client()
    source_bucket_obj = client.bucket(source_bucket)
    dest_bucket_obj = client.bucket(dest_bucket)

    # Get the source blob
    source_blob = source_bucket_obj.blob(dma_source_key)

    # Create a new blob in the destination bucket
    dest_blob = dest_bucket_obj.blob(dest_key)

    # Copy the source blob to the destination blob
    dest_blob.copy_from(source_blob)

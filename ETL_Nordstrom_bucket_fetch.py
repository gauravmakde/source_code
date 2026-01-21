from google.cloud import storage


from google.cloud import storage

def download_blob(bucket_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    print(bucket)

    # Get the blob
    # blob = bucket.blob(source_blob_name)
    #
    # # Download the blob to a local file
    # blob.download_to_filename(destination_file_name)
    #
    # print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

# Example usage
bucket_name = "dm-del-composer-bucket"
# source_blob_name = "path/to/your/file/in/gcs"
destination_file_name = "dm-del-composer-bucket/dags"

download_blob(bucket_name, destination_file_name)
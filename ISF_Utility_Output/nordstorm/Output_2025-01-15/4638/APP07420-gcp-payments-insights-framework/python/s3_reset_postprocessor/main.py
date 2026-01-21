import boto3
import os


def main():
    s3 = boto3.resource('s3')
    bucket_name = os.environ.get('RESET_S3_BUCKET')
    bucket = s3.Bucket(bucket_name)
    processed_folder_name = 'processed'
    store_files_path_name = 'to_process/reset-store-matched-avro-data-files/'
    online_files_path_name = 'to_process/reset-online-matched-avro-data-files/'

    for object in bucket.objects.filter(Prefix=online_files_path_name):
        if object.key != online_files_path_name:
            s3.meta.client.copy({"Bucket": bucket_name, "Key": object.key}, bucket_name,
                                processed_folder_name + '/' + object.key.split("/", 1)[1])
            s3.Object(bucket_name, object.key).delete()

    for object in bucket.objects.filter(Prefix=store_files_path_name):
        if object.key != store_files_path_name:
            s3.meta.client.copy({"Bucket": bucket_name, "Key": object.key}, bucket_name,
                                processed_folder_name + '/' + object.key.split("/", 1)[1])
            s3.Object(bucket_name, object.key).delete()

    print('Successfully completed.')


if __name__ == "__main__":
    main()

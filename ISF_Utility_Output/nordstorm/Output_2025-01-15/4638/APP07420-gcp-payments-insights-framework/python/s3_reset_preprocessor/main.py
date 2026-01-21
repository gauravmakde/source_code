import boto3
import os


def main():
    bucket_name = os.environ.get('RESET_S3_BUCKET')
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    avro_schema_folder_name = 'avro-schema'
    processing_folder_name = 'to_process'
    processed_folder_name = 'processed'

    for obj in bucket.objects.all():
        if processed_folder_name not in obj.key and processing_folder_name not in obj.key and avro_schema_folder_name not in obj.key:
            print('copying ' + obj.key + ' to ' + processing_folder_name + '...')
            s3.meta.client.copy({'Bucket': bucket_name, 'Key': obj.key}, bucket_name,
                                processing_folder_name + '/' + obj.key)
            s3.Object(bucket_name, obj.key).delete()
            print('deleting: ' + obj.key)


if __name__ == '__main__':
    main()

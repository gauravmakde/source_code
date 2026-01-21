import boto3
import os


def main():
    s3 = boto3.resource('s3')
    bucket = os.environ.get('FRAUD_WORLDPAY_BIN_BUCKET')
    prefix_processed = 'processed/'
    prefix_raw = 'raw/'

    for object in s3.Bucket(bucket).objects.filter(Prefix=prefix_raw):
        key_old = object.key

        if key_old == prefix_raw:
            continue

        key_new = key_old.replace(prefix_raw, prefix_processed, 1)

        s3.meta.client.copy_object(
            Bucket=bucket,
            Key=key_new,
            CopySource={
                'Bucket': bucket,
                'Key': key_old
            }
        )
        s3.meta.client.delete_object(
            Bucket=bucket,
            Key=key_old
        )

    s3.meta.client.put_object(
        Bucket=bucket,
        Key=prefix_raw,
        Body=''
    )


if __name__ == "__main__":
    main()

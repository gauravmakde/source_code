import boto3
import botocore

def createS3Object(bucket_name, fileKeyPath):
    s3 = boto3.resource('s3')
    s3_bucket = bucket_name
    s3_key = fileKeyPath
    print("The s3 bucket name is: " + s3_bucket)
    print("The s3 key path is: " + s3_key)
    object = s3.Object(s3_bucket, s3_key)
    return object

def placeTriggerFile(bucket_name, fileKeyPath):
    string_data = "TriggerFile"
    object = createS3Object(bucket_name, fileKeyPath)
    object.put(Body=string_data)
    print("Trigger file successfully placed")

def removeTriggerFile(bucket_name, fileKeyPath):
    object = createS3Object(bucket_name, fileKeyPath)
    object.delete()
    print("Trigger file removed successfully")

def fileSensor(bucket_name, fileKeyPath):
    object = createS3Object(bucket_name, fileKeyPath)
    try:
        object.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise Exception("key/file does not exist !!")
        else:
            print (e)
            raise
    else:
        print("key/file found in s3 !!")






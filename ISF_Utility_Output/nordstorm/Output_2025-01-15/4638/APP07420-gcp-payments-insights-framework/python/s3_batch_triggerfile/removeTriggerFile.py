from TriggerFileUtil import removeTriggerFile
import argparse

def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--s3_bucket', help='s3 bucket name', required=True)
    arg_parser.add_argument('--s3_key', help='s3 key/path name', required=True)
    return arg_parser.parse_args()

def main():
    args = get_args()
    s3_bucket = args.s3_bucket
    s3_key = args.s3_key
    removeTriggerFile(s3_bucket, s3_key)

if __name__ == "__main__":
    main()
import logging
import json
import pandas as pd

success_file_ind = "_SUCCESS"


def __s3_audit_file_count(s3, bucket_name, prefix_list):
    metamorph_sum = 0

    for prefix in prefix_list:

        # Get the list of keys for prefix
        keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)
        logging.info(f"Keys found for prefix({prefix}): {keys}")

        # Filter out the _SUCCESS indicator file
        for key in keys:
            if key.endswith(success_file_ind):
                keys.remove(key)

        # Checking the length of the above list:
        audit_files_count = len(keys)
        logging.info(f"Audit files found: {keys}")
        logging.info(f"Number of audit files found: {audit_files_count}")

        # Get the count of all the audit files from s3
        if audit_files_count == 0:
            print("There are no audit files in this path... exiting")
            exit(255)
        elif audit_files_count > 0:
            logging.info(f"The audit files found: {keys}")
            s3_object_details = {}
            df = []
            for key in keys:
                s3_object_details.update(
                    s3.get_object(bucket_name=bucket_name, key=key).get()
                )
                df.append(
                    pd.read_csv(s3_object_details["Body"], header=None, names=["count"])
                )
            audit_df = pd.concat(df, ignore_index=True)
            metamorph_count = audit_df["count"].sum()
            logging.info(
                f"The value of the above mentioned audit files is: {metamorph_count}"
            )

            metamorph_sum = metamorph_count + metamorph_sum

    return str(metamorph_sum)


def get_s3_audit_file_count(s3, bucket_name, manifest_path):
    content_object = s3.get_object(bucket_name=bucket_name, key=manifest_path)
    file_content = content_object.get()["Body"].read().decode("utf-8")
    json_content = json.loads(file_content)
    key_list = []

    for entry in json_content["entries"]:
        key = entry["url"].split(f"{bucket_name}/")[1]
        key = key.replace("/data/", "/audit/")
        key_list.append(key)

    print(f"Manifest file list: {key_list}")
    s3_audit_files_count = __s3_audit_file_count(s3, bucket_name, key_list)

    return s3_audit_files_count

import re


topic_name="ascp-inventory-stock-quantity-object-model-avro"
consumer_group_name="onix2-ascp-inventory-stock-quantity-object-model-avro_ascp_ihub_refresh_16780_TECH_SC_NAP_insights_kafka_load_kafka"
batch_id=20241008094755
start_offset_log_string='PartitionOffset(6,1657462132), PartitionOffset(7,1660504784), PartitionOffset(8,1639463698), PartitionOffset(9,1664284563), PartitionOffset(2,2225026545), PartitionOffset(3,2240309605), PartitionOffset(4,2228068693), PartitionOffset(5,2254908492), PartitionOffset(0,2239997843), PartitionOffset(1,2239170817), PartitionOffset(10,1624100353), PartitionOffset(11,1634057322)'
num_records_per_partition=1000

def for_partition_kafka(start_offset_log_string):

    re_parttioned=r"PartitionOffset(\([^)]*\))"
    list1=re.findall(re_parttioned,start_offset_log_string)
    counter=len(list1)
    start_offset_string = "'{"
    end_offset_string = "'{"
    for item in list1:
        var = item.strip('()').split(',')
        start_offset_string += f'"{var[0]}":{var[1]},'
        end_offset_string += f'"{var[0]}":{round(float(var[1])+num_records_per_partition)},'
    start_offset_string.rstrip(',')
    start_offset_string += "}'"
    end_offset_string.rstrip(',')
    end_offset_string += "}'"
    expected_output=f"""
    INSERT INTO ONEHOP_ETL_APP_DB.KAFKA_CONSUMER_OFFSET_BATCH_DETAILS VALUES("{topic_name}",
    "{consumer_group_name}",
    {batch_id},
    {start_offset_string},
    {end_offset_string},
    {num_records_per_partition*counter},
    CURRENT_TIMESTAMP()
    )
                    """
    return expected_output

output = for_partition_kafka(start_offset_log_string)

print(output)

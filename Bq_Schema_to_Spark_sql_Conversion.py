import json

Input_code_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//table_json.txt"
Output_code_location="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//inventory_item_return_lifecycle_parquet.txt"
# Read JSON from file
with open(Input_code_location) as json_file:
    table_json = json_file.read()

# Parse JSON
json_loads = json.loads(table_json)

# Initialize SQL query
insert_spark_sql = '''
insert overwrite table bq_table
select distinct
'''

# Recursive function to build STRUCT definition
def get_struct_definition(fields):
    struct_fields = []
    for field in fields:
        field_name = field["name"]
        field_type = field["type"].lower()  # Convert to lowercase for Spark compatibility
        field_mode = field["mode"]
        field_fields = field["fields"]  # Nested fields

        if field_type == "record":
            # Recursively process nested RECORD fields
            struct_definition = get_struct_definition(field_fields)
            if field_mode == "REPEATED":
                struct_fields.append(f"{field_name}:array<struct<{struct_definition}>>")
            else:
                struct_fields.append(f"{field_name}:struct<{struct_definition}>")
        elif field_mode == "REPEATED":
            struct_fields.append(f"{field_name}:array<{field_type}>")
        else:
            struct_fields.append(f"{field_name}:{field_type}")

    return ", ".join(struct_fields)

# Function to generate Spark SQL CAST statement
def function_to_create_spark_sql(single, insert_spark_sql):
    column_type = single.get("type").lower()  # Normalize type to lowercase
    column_name = single.get("name")
    column_mode = single.get("mode")
    column_fields = single.get("fields")

    # Supported data types in Spark
    supported_types = {
        "string": "string",
        "boolean": "boolean",
        "integer": "int",
        "float": "float",
        "datetime": "timestamp",
        "date": "date",
        "bytes": "binary"
    }

    if column_type in supported_types:  # Handle all primitive types
        spark_type = supported_types[column_type]
        insert_spark_sql += f"cast({column_name} as {spark_type}) AS {column_name},\n"

    elif column_type == "record":
        struct_definition = get_struct_definition(column_fields)
        if column_mode == "REPEATED":
            insert_spark_sql += f"cast({column_name} as array<struct<{struct_definition}>>) AS {column_name},\n"
        else:
            insert_spark_sql += f"cast({column_name} as struct<{struct_definition}>) AS {column_name},\n"

    return insert_spark_sql

# Loop through JSON and process each column
for single in json_loads:
    insert_spark_sql = function_to_create_spark_sql(single, insert_spark_sql)

# Remove trailing comma and finalize SQL query
insert_spark_sql = insert_spark_sql.rstrip(",\n")  # Remove last comma
insert_spark_sql += "\n"

# Print final SQL query
print(insert_spark_sql)

with open(Output_code_location,"w") as output_file:
    output_file.write(insert_spark_sql)


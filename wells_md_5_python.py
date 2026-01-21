import hashlib
import pandas as pd
import os
import time
import numpy as np

# Get the current date and time
datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.mkdir(directory)

first_parquet_file = f"{directory}/parquet_file_1.parquet"
second_parquet_file = f"{directory}/parquet_file_2.parquet"
# larger_first_parquet_file=f"{directory}/large_sample_1.parquet"

def parquet_file(data, filename):
    # Create DataFrame
    df = pd.DataFrame(data)

    # Convert to Parquet format
    file_path = os.path.join(directory, f"{filename}.parquet")
    df.to_parquet(file_path, engine="pyarrow", index=False)

    print(f"Sample Parquet file '{file_path}' created successfully.")

#to create huge has files
# def parquet_file():
#     num_rows = 10_000_000  # 10 million rows
#
#     # Generate sample data
#     data = {
#         "id": np.arange(1, num_rows + 1),
#         "name": np.random.choice(["Alice", "Bob", "Charlie", "David", "Eve"], num_rows),
#         "age": np.random.randint(20, 60, num_rows),
#         "city": np.random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"], num_rows),
#         "salary": np.random.randint(30000, 120000, num_rows),
#     }
#
#     # Create DataFrame
#     df = pd.DataFrame(data)
#
#     # Save as Parquet file
#     df.to_parquet("large_sample.parquet", engine="pyarrow", index=False)
#
# parquet_file()
# exit()
def calculate_file_md5(file_path):
    """Compute the MD5 hash of a given file."""
    md5_hash = hashlib.md5()  # Initialize MD5 hash object

    with open(file_path, "rb") as f:  # Open file in binary mode
        for chunk in iter(lambda: f.read(4096), b""):  # Read file in 4KB chunks
            md5_hash.update(chunk)  # Update the hash with the chunk
            # print(md5_hash)
    return md5_hash.hexdigest()  # Return the final MD5 hash as a hexadecimal string


# Sample data
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Gaurav", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 30, 35, 40, 45],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
}

data_2 = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Gaurav", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 30, 35, 40, 45],
    "city": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
}

# Generate parquet files
parquet_file(data, "parquet_file_1")
parquet_file(data_2, "parquet_file_2")

# Calculate MD5 hashes for both Parquet files
first_md5 = calculate_file_md5(first_parquet_file)
# first_md5 = calculate_file_md5(larger_first_parquet_file)
second_md5 = calculate_file_md5(second_parquet_file)


# Compare hashes
if first_md5 == second_md5:
    print("✅ Parquet files are identical.")
else:
    print("❌ Parquet files are different.")

print(f"MD5 Hash of first file: {first_md5}")
print(f"MD5 Hash of second file: {second_md5}")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BinaryType
from fsspec.implementations.local import LocalFileSystem
import json
import os
from functools import reduce
from pyspark.sql.functions import lit, row_number

from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("Polygraphs Processing").getOrCreate()

# Define schema for data.csv DataFrame
schema = StructType([
    StructField("steps", IntegerType(), True),
    StructField("duration", FloatType(), True),
    StructField("action", StringType(), True),
    StructField("undefined", StringType(), True),
    StructField("converged", StringType(), True),
    StructField("polarized", StringType(), True),
    StructField("uid", StringType(), True)
])

# Define schema for configuration DataFrame
config_schema = StructType([
    StructField("trials", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("kind", StringType(), True),
    StructField("op", StringType(), True),
    StructField("epsilon", FloatType(), True)
])

# Define schema for binary data DataFrame
bin_schema = StructType([
    StructField("binary_data", BinaryType(), True),
    StructField("path", StringType(), True)
])

def extract_params(config_json_path):
    """Extracts required key-value pairs from configuration.json.

    Args:
        config_json_path (str): Path to the configuration.json file.

    Returns:
        tuple: A tuple containing the extracted params (trials, size, kind, op, epsilon).
    """

    with filesystem.open(config_json_path, "r") as f:
        config_data = json.load(f)

    return (
        config_data.get("trials"),  # Use .get() to handle potential missing keys
        config_data.get("network", {}).get("size"),  # Use .get() for nested keys
        config_data.get("network", {}).get("kind"),
        config_data.get("op"),
        config_data.get("epsilon"),
    )


# Create a LocalFileSystem instance
filesystem = LocalFileSystem()

# List all directories with dates in the results folder
results_folder = "~/polygraphs-cache/results"

date_folders = filesystem.ls(results_folder)

# Initialize an empty list to store all DataFrames
dataframes = []

# Process each date folder
for date_folder in date_folders:
    # List all UID folders in the date folder
    uid_folders = filesystem.ls(date_folder)
    
    # Process each UID folder
    for uid_folder in uid_folders:
        # Read data from data.csv into DataFrame
        data_csv_path = os.path.join(uid_folder, "data.csv")
        if filesystem.exists(data_csv_path):
            df_csv = spark.read.csv(data_csv_path, header=True, schema=schema)
            # Read data from configuration.json into DataFrame
            config_json_path = os.path.join(uid_folder, "configuration.json")
            
            if filesystem.exists(config_json_path):
                trials, size, kind, op, epsilon = extract_params(config_json_path)
                # Create a DataFrame from the extracted data
                config_df = spark.createDataFrame([(trials, size, kind, op, epsilon)], schema=config_schema)
                
                # Cross join the DataFrame with the configuration DataFrame
                df_csv = df_csv.crossJoin(config_df)
                # Append the DataFrame to the list
                dataframes.append(df_csv)
                dataframes
        else:
            continue  # Skip processing if data.csv does not exist

df = reduce(lambda df1, df2: df1.union(df2), dataframes)

binary_data_list = []
for date_folder in date_folders:
    # List all UID folders in the date folder
    uid_folders = filesystem.ls(date_folder)
    # Process each UID folder
    for uid_folder in uid_folders:
        bin_files = [bin_file for bin_file in filesystem.ls(uid_folder) if bin_file.endswith(".bin")]
        num_bin_files = len(bin_files)
        for i in range(1, num_bin_files + 1): 
            bin_file_path = os.path.join(uid_folder, str(i) + ".bin")
            if filesystem.exists(bin_file_path):
                with filesystem.open(bin_file_path, "rb") as bin_file:
                    binary_data = bin_file.read()
                binary_data_list.append((binary_data, bin_file_path))  
                
                
binary_df = spark.createDataFrame(binary_data_list, bin_schema)

w = Window().orderBy(lit(None))
df = df.withColumn('row_num', row_number().over(w))
binary_df = binary_df.withColumn('row_num', row_number().over(w))

result_df = df.join(binary_df, 'row_num').drop('row_num')

# df = result_df.select('uid', 'steps', 'path')
# df.show(truncate=200)

result_df.show()
# Stop the SparkSession
spark.stop()

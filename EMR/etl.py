import re
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, TimestampType, IntegerType
from pyspark.sql.functions import col,hour

# Initialize Spark
sc = SparkContext()
spark = SparkSession(sc)
input_path = "s3a://twitter-log/weblog/input/"
output_path = "s3a://twitter-log/weblog/loganalysis/output/"

# Define the schema for the DataFrame
log_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("request_type", StringType(), True),
    StructField("url", StringType(), True),
    StructField("response_size", IntegerType(), True)
])

def get_processed_data(data):
    pattern_ip = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
    pattern_date = r'\[\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4}\]'
    pattern_request_type = r'([A-Z]+) \S+'
    pattern_url = r'"([A-Z]+) ([^"]+) HTTP/\d+\.\d+"'
    pattern_response_size = r' (\d+)$'

    ip = re.search(pattern_ip, data).group()
    date_str = re.search(pattern_date, data).group()
    request_type = re.search(pattern_request_type, data).group(1)
    url = re.search(pattern_url, data).group(2)
    response_size = int(re.search(pattern_response_size, data).group(1))

    # Parse the date string into a datetime object
    timestamp_format = "[%d/%b/%Y:%H:%M:%S %z]"
    timestamp_datetime = datetime.strptime(date_str, timestamp_format)

    processed_data = {
        "ip": ip,
        "date": timestamp_datetime,
        "request_type": request_type,
        "url": url,
        "response_size": response_size
    }
    return processed_data

data = sc.textFile(input_path)

rdd = data.map(get_processed_data)

dataFrame = spark.createDataFrame(rdd, schema=log_schema)
dataFrame = dataFrame.withColumn("hour",hour(col("date")))

dataFrame.write\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", '\001')\
    .save(output_path)
    
spark.stop()

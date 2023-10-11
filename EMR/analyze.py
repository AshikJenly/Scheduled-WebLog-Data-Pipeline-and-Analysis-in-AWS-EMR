from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.types import IntegerType
import datetime

PARENT_FOLDER = datetime.datetime.now().strftime("%Y-%m-%d__%H-%M-%S")


input_path = "<input_path>"
output_base_path = f"<output_path>/{PARENT_FOLDER}"


spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

log_df = spark.read.option("header",True).option("sep","\001").csv(input_path)


highest_hitting_time = log_df.groupBy("hour").count().orderBy(col("count").desc()).limit(1)

most_frequent_request_type = log_df.groupBy("request_type").count().orderBy(col("count").desc()).limit(1)

log_df = log_df.withColumn("response_size", log_df["response_size"].cast(IntegerType()))
response_stats = log_df.agg({"response_size": "min", "response_size": "max", "response_size": "avg"})

highest_hitting_time.write.option("header",True).option("sep","\001").csv(f"{output_base_path}/hitting")
most_frequent_request_type.write.option("header",True).option("sep","\001").csv(f"{output_base_path}/frequent_request")
response_stats.write.option("header",True).option("sep","\001").csv(f"{output_base_path}/respose_size")

spark.stop()

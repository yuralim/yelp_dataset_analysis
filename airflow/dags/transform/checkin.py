import pyspark.sql.functions as F

# Read the checkin file from S3
checkin_df = spark.read.json('s3a://{{ input_file_name }}')

# Load checkin data and write a parquet
checkin_table_df = checkin_df \
    .filter(F.col("business_id").isNotNull()) \
    .withColumn("splited_date", F.explode(F.split("date", ", "))) \
    .withColumn("casted_date", F.to_timestamp("splited_date", "yyyy-MM-dd HH:mm:ss")) \
    .select(
        "business_id",
        F.col("casted_date").alias("date")
    )
checkin_table_df.write.parquet("s3a://{{ temp_file_path }}/checkin.parquet")
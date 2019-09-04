import pyspark.sql.functions as F

# Read the business file from S3
business_df = spark.read.json('s3a://{{ input_file_name }}')
business_df.createOrReplaceTempView('business')

# Filter invalid rows
business_df = spark.sql("""
select
    *
from business
where business_id is not null and is_open = 1
""")
business_df.createOrReplaceTempView('business')

# Load business data and write a parquet
business_table_df = spark.sql("""
select
    business_id,
    address,
    city,
    latitude,
    longitude,
    name,
    postal_code,
    state
from business
""")
business_table_df.write.parquet("s3a://{{ temp_file_path }}/business.parquet")

# Load business cagetory data and write a parquet
business_category_table_df = business_df \
    .select("business_id", "categories") \
    .filter(F.col("categories").isNotNull()) \
    .withColumn("categories", F.explode(F.split("categories", ", ")))
business_category_table_df.write.parquet("s3a://{{ temp_file_path }}/business_category.parquet")
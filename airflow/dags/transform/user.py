import pyspark.sql.functions as F

# Read the user file from S3
user_df = spark.read.json('s3a://{{ input_file_name }}')
user_df.createOrReplaceTempView("yelp_user")

# Load user data and write a parquet
user_table_df = spark.sql("""
select
    user_id,
    name
from yelp_user
where user_id is not null
""")
user_table_df.write.parquet("s3a://{{ temp_file_path }}/yelp_user.parquet")

# Load user_elite data and write a parquet
user_elite_table_df = spark.sql("""
select
    user_id,
    elite
from yelp_user
where user_id is not null 
and elite is not null and length(elite) > 0
""")
user_elite_table_df = user_elite_table_df \
    .withColumn("year", F.explode(F.split("elite", ","))) \
    .select("user_id", "year")
user_elite_table_df.write.parquet("s3a://{{ temp_file_path }}/yelp_user_elite.parquet")

# Load user_friend data and write a parquet
user_friend_table_df = spark.sql("""
select
    user_id,
    friends
from yelp_user
where user_id is not null 
and elite is not null and length(elite) > 0 
and friends is not null and length(friends) > 0
""")
user_friend_table_df = user_friend_table_df \
    .withColumn("friend_id", F.explode(F.split("friends", ","))) \
    .select("user_id", "friend_id")
user_friend_table_df.write.parquet("s3a://{{ temp_file_path }}/yelp_user_friend.parquet")

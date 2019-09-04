import pyspark.sql.functions as F
from pyspark.sql.types import *
import re
import uuid

# Read the checkin file from S3
tip_df = spark.read.json('s3a://{{ input_file_name }}')

uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())
tip_df = tip_df.withColumn("tip_id", uuid_udf())
tip_df.createOrReplaceTempView("tip")

# Extract only required fields
tip_staging_df = spark.sql("""
select
    tip_id,
    user_id,
    to_timestamp(date, 'yyyy-MM-dd HH:mm:ss') as date,
    business_id,
    lower(text) as text
from tip
where text is not null
""")

# Build the sentiment analyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

def get_sentiment_analysis_score(sentence):
    score = analyser.polarity_scores(sentence)
    return score['compound']

def get_sentiment_analysis_result(score):
    if score >= 0.05:
        return "POSITIVE"
    elif score <= -0.05:
        return "NEGATIVE"
    else:
        return "NEUTRAL"
    
get_sentiment_analysis_score_udf = F.udf(lambda x: get_sentiment_analysis_score(x), DoubleType())
get_sentiment_analysis_result_udf = F.udf(lambda x: get_sentiment_analysis_result(x), StringType())

# Load tip data and write a parquet
tip_table_df = tip_staging_df \
    .withColumn("sa_score", get_sentiment_analysis_score_udf("text")) \
    .withColumn("sentiment", get_sentiment_analysis_result_udf("sa_score")) \
    .select(
        "tip_id",
        "user_id",
        "date",
        "business_id",
        "text",
        "sentiment")
tip_table_df.write.parquet("s3a://{{ temp_file_path }}/tip.parquet")

# Build the tokenizer
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')
def tokenize(sentence):
    tokens = tokenizer.tokenize(sentence)
    return tokens

stop_words = set(stopwords.words('english'))
def is_not_stopword(word):
    return not word in stop_words

lemmatizer = WordNetLemmatizer()
def lemmatize(word):
    return lemmatizer.lemmatize(word)

def is_valid_word(word):
    return len(word) > 1 and not re.match("^([^a-z|0-9]|\'[a-z|0-9])$", word) and not word.isdecimal()
    
tokenize_udf = F.udf(lambda x: tokenize(x), ArrayType(StringType()))
is_not_stopword_udf = F.udf(lambda x: is_not_stopword(x), BooleanType())
lemmatize_udf = F.udf(lambda x: lemmatize(x), StringType())
is_valid_word_udf = F.udf(lambda x: is_valid_word(x), BooleanType())

# Load tip_text data and write a parquet
tip_text_table_df = tip_staging_df \
    .withColumn("word", F.explode(tokenize_udf("text"))) \
    .withColumn("word", lemmatize_udf("word")) \
    .filter(is_not_stopword_udf("word")) \
    .filter(is_valid_word_udf("word")) \
    .select(
        "tip_id",
        "word")
tip_text_table_df.write.parquet("s3a://{{ temp_file_path }}/tip_text.parquet")

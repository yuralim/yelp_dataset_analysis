import pyspark.sql.functions as F
from pyspark.sql.types import *
import re

# Read the checkin file from S3
review_df = spark.read.json('s3a://{{ input_file_name }}')
review_df.createOrReplaceTempView("review")

# Extract only required fields
review_staging_df = spark.sql("""
select
    review_id,
    user_id,
    business_id,
    stars,
    to_timestamp(date, 'yyyy-MM-dd HH:mm:ss') as date,
    lower(text) as text
from review
where review_id is not null and text is not null
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

# Load review data and write a parquet
review_table_df = review_staging_df \
    .withColumn("sa_score", get_sentiment_analysis_score_udf("text")) \
    .withColumn("sentiment", get_sentiment_analysis_result_udf("sa_score")) \
    .select(
        "review_id",
        "user_id",
        "business_id",
        "stars",
        "date",
        "text",
        "sentiment")
review_table_df.write.parquet("s3a://{{ temp_file_path }}/review.parquet")

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

# Load review_text data and write a parquet
review_text_table_df = review_staging_df \
    .withColumn("word", F.explode(tokenize_udf("text"))) \
    .withColumn("word", lemmatize_udf("word")) \
    .filter(is_not_stopword_udf("word")) \
    .filter(is_valid_word_udf("word")) \
    .select(
        "review_id",
        "word")
review_text_table_df.write.parquet("s3a://{{ temp_file_path }}/review_text.parquet")

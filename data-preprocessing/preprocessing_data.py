from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import countDistinct, col, when
from pyspark.sql.window import Window
import os
from dotenv import load_dotenv
load_dotenv()s

SPARK_MASTER = os.getenv("SPARK_MASTER")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Configure Spark
spark = (SparkSession.builder
         .appName("DataProcessingJob")
         .master(SPARK_MASTER)
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "2G")
         .config("spark.driver.memory", "4G")
         .config("spark.executor.cores", "1")
         .config("spark.python.worker.memory", "1G")
         .config("spark.driver.maxResultSize", "3G")
         .config("spark.kryoserializer.buffer.max", "1024M")
         .getOrCreate())

# Set up Google Cloud Storage Hadoop configuration
spark.conf.set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

# Define file paths for user and post data
user_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/extracted_data/user_data.parquet"
post_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/extracted_data/post_data.parquet"

# Read data into Spark DataFrames
df_user = spark.read.parquet(user_path)
df_post = spark.read.parquet(post_path)

# Explore and print information about the DataFrames

# Handling missing values, drop duplicates, and drop columns
df_user = df_user.na.fill('none', ["verified_type"]).dropDuplicates().drop(
    'fast_followers_count', 'is_translator', 'translator_type', 'want_retweets', 'protected'
)
df_post = df_post.drop('lang')

# Sorting df_post by 'screen_name' and 'created_at'
window_spec = Window.partitionBy("screen_name").orderBy("created_at")
df_post = df_post.withColumn("time_diff", F.col("created_at").cast("long") - F.lag("created_at").over(window_spec))
average_time_diff = df_post.groupby('screen_name').agg(F.avg('time_diff').alias('average_time_diff'))
df_post = df_post.join(average_time_diff, on='screen_name', how='left')

# Data transformations and handling views column
df_post = df_post.withColumn("time_diff_seconds", F.col("time_diff").cast("double"))
df_post = df_post.withColumn("average_time_diff_seconds", F.col("average_time_diff").cast("double"))

# Calculate absolute time difference in seconds and compare with the interval
df_post = df_post.withColumn("time_bot", (F.abs(F.col("time_diff_seconds") - F.col("average_time_diff_seconds")) < (5 * 60)).cast("int"))

# Grouping and aggregating for time_bot
time_bot_count = df_post.groupby('screen_name').agg(F.sum('time_bot').alias('time_bot_count'))

# Joining df_user with time_bot_count
df_user = df_user.join(time_bot_count, on='screen_name', how='left').na.fill(0, ["time_bot_count"])

# Handling views column and creating views/like column
df_post = df_post.withColumn("views", F.when(F.col("views") == "Unavailable", 0).otherwise(F.col("views").cast("int")))
df_post = df_post.withColumn("views/like", F.when(F.col("favorite_count") != 0, F.col("views") / F.col("favorite_count")).otherwise(0))

# Filtering rows and creating views_score column
filtered_rows = (F.col("views/like").cast("float") > 50) & (F.col("views").cast("float") > 7000)
df_post = df_post.withColumn("views_score", F.when(filtered_rows, 1).otherwise(0))

# Grouping and aggregating for views_score
v_count = df_post.groupby('screen_name').agg(F.sum('views_score').alias('views_score'))

# Joining df_user with v_count
df_user = df_user.join(v_count, on='screen_name', how='left')

# Creating following/follower column and friend_score column
df_user = df_user.withColumn("following/follower", F.when(F.col("followers_count") != 0, F.col("friends_count") / F.col("followers_count")).otherwise(0))
filtered_rows = (F.col("following/follower").cast("float") > 1) & (F.col("friends_count").cast("int") > 500)
df_user = df_user.withColumn("friend_score", F.when(filtered_rows, 1).otherwise(0))

# Creating bot_score column
df_user = df_user.withColumn("bot_score", F.col("friend_score") + F.col("views_score") + F.col("time_bot_count"))

# Data Transformation
df_post = df_post.withColumn('created_at', F.from_unixtime('created_at').cast('timestamp'))
df_user = df_user.withColumn('created_at', F.from_unixtime('created_at').cast('timestamp'))

# Summary statistics
summary_stats = df_user.summary()

# Get the 75th percentile value for 'bot_score'
percentile_75 = float(summary_stats.filter("summary = '75%'").select("bot_score").collect()[0]['bot_score'])

# Define conditions for updating 'verified_type'
condition1 = (col("verified_type") == "none") & (col("bot_score") >= percentile_75) & (col("verified") == False)
condition2 = (col("verified_type") == "none") & (col("bot_score") >= percentile_75) & (col("verified") == True)
condition3 = (col("verified_type") == "none") & (col("bot_score") < percentile_75) & (col("verified") == False)
condition4 = (col("verified_type") == "none") & (col("bot_score") < percentile_75) & (col("verified") == True)

df_user = df_user.withColumn(
    "verified_type",
    when(condition1, "bot")
    .when(condition2, "project")
    .when(condition3, "default_user")
    .when(condition4, "KOL")
    .otherwise(col("verified_type"))
)

# Connect to Google Bucket and write transformed data
user_transform_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/transformed-data/user_data_cleaned"
post_transform_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/transformed-data/post_data_cleaned"

df_user.write.parquet(user_transform_path)
df_post.write.parquet(post_transform_path)

# Stop Spark session
spark.stop()

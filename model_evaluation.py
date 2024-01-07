from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os
from dotenv import load_dotenv
load_dotenv()

SPARK_MASTER = os.getenv("SPARK_MASTER")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master(SPARK_MASTER)
            .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
            .config("spark.executor.memory", "2G")  #excutor excute only 2G
            .config("spark.driver.memory","4G") 
            .config("spark.executor.cores","1") #Cluster use only 3 cores to excute
            .config("spark.python.worker.memory","1G") # each worker use 1G to excute
            .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
            .config("spark.kryoserializer.buffer.max","1024M")
            .getOrCreate())

#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

## Connect to the file in Google Bucket with Spark

post_path_1 = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/transformed-data/post_data_cleaned/part-00000-f0ebfdae-3b6b-4a5f-adef-0c3bb8642547-c000.snappy.parquet"
post_path_2 = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/transformed-data/post_data_cleaned/part-00001-f0ebfdae-3b6b-4a5f-adef-0c3bb8642547-c000.snappy.parquet"
user_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/transformed-data/user_data_cleaned/part-00000-98b54bfe-89b6-4cdb-8e50-6a45c3dbe627-c000.snappy.parquet"

df_user = spark.read.parquet(user_path)
df_post_1 = spark.read.parquet(post_path_1)
df_post_2 = spark.read.parquet(post_path_2)
# Add row numbers to df_post_2 using zipWithIndex
df_post_2 = df_post_2.withColumn("row_num", F.monotonically_increasing_id())
df_post_2 = df_post_2.withColumn("row_num", F.col("row_num") - 1)  # Adjust row numbers to start from 0

# Exclude the first row
df_post_2 = df_post_2.filter("row_num > 0").drop("row_num")
# Find common columns
common_columns = list(set(df_post_2.columns) & set(df_post_2.columns))
# Select only the common columns and concatenate the DataFrames
concatenated_df_post = df_post_1.select(common_columns).union(df_post_2.select(common_columns))

# Filter KOL and Project accounts
filtered_user_df = df_user.filter((F.col('verified_type') == 'project'))
# Rename columns in the user_df to avoid conflicts after the join
user_df_renamed = filtered_user_df.select(
    F.col("verified_type").alias("user_verified_type"),
    F.col("can_dm").alias("user_can_dm"),
    F.col("can_media_tag").alias("user_can_media_tag"),
    F.col("created_at").alias("user_created_at"),
    F.col("default_profile").alias("user_default_profile"),
    F.col("description").alias("user_description"),
    F.col("favourites_count").alias("user_favourites_count"),
    F.col("followers_count").alias("user_followers_count"),
    F.col("friends_count").alias("user_friends_count"),
    F.col("has_custom_timelines").alias("user_has_custom_timelines"),
    F.col("listed_count").alias("user_listed_count"),
    F.col("location").alias("user_location"),
    F.col("media_count").alias("user_media_count"),
    F.col("screen_name").alias("user_screen_name"),
    F.col("statuses_count").alias("user_statuses_count"),
    F.col("verified").alias("user_verified"),
    F.col("profile_url").alias("user_profile_url"),
    F.col("time_bot_count").alias("user_time_bot_count"),
    F.col("views_score").alias("user_views_score"),
    F.col("following/follower").alias("user_following_follower"),
    F.col("friend_score").alias("user_friend_score"),
    F.col("bot_score").alias("user_bot_score")
)

# Join user_df_renamed with tweet_df
combined_df = concatenated_df_post.join(user_df_renamed, concatenated_df_post['screen_name'] == user_df_renamed['user_screen_name'], how='inner')
# Drop the duplicated 'user_screen_name' column
combined_df = combined_df.drop('user_screen_name')

# Group by user and aggregate metrics
user_metrics = combined_df.groupBy("screen_name").agg(
    F.count("id").alias("tweet_count"),
    F.sum("favorite_count").alias("total_favorite_count"),
    F.sum("retweet_count").alias("total_retweet_count"),
    F.sum("reply_count").alias("total_reply_count"),
    F.sum("quote_count").alias("total_quote_count"),
    F.sum("views").alias("total_views"),
    F.sum("bookmark_count").alias("total_bookmark_count"),
    F.avg("user_favourites_count").alias("user_favourites_count"),
    F.avg("user_statuses_count").alias("user_statuses_count"),
    F.avg("user_followers_count").alias("user_followers_count"),
    F.avg("user_friends_count").alias("user_friends_count"),
    F.avg("user_media_count").alias("user_media_count"),
    F.avg("user_time_bot_count").alias("user_time_bot_count"),
    F.avg("user_friend_score").alias("user_friend_score"),
    F.avg("user_bot_score").alias("user_bot_score"),
    F.avg("user_listed_count").alias("user_listed_count")
)

selected_features = [
    "tweet_count",
    "total_favorite_count",
    "total_retweet_count",
    "total_reply_count",
    "total_quote_count",
    "total_views",
    "total_bookmark_count",
    "user_favourites_count",
    "user_statuses_count",
    "user_followers_count",
    "user_friends_count",
    "user_media_count",
    "user_time_bot_count",
    "user_friend_score",
    "user_bot_score",
    "user_listed_count"
]
# Assemble features into a single vector
vec_assembler = VectorAssembler(inputCols=selected_features, outputCol="features")
user_metrics = vec_assembler.transform(user_metrics)

# Scale features
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(user_metrics)
user_metrics = scaler_model.transform(user_metrics)
  
## Test to find the suitable values of k
# silhouette_score=[] 
# evaluator = ClusteringEvaluator(predictionCol='prediction', 
#                                 featuresCol='scaled_features', 
#                                 metricName='silhouette', 
#                                 distanceMeasure='squaredEuclidean') 
  
# for i in range(2,10): 
#     kmeans=KMeans(featuresCol='scaled_features', k=i) 
#     model=kmeans.fit(user_metrics) 
#     predictions=model.transform(user_metrics) 
#     score=evaluator.evaluate(predictions) 
#     silhouette_score.append(score) 
#     print('Silhouette Score for k =',i,'is',score)

# Assume k=3 for demonstration purposes
k = 3
kmeans = KMeans(k=k, seed=42, featuresCol="scaled_features", predictionCol="cluster")
model = kmeans.fit(user_metrics)

# Make predictions
predictions = model.transform(user_metrics)

# Evaluate clustering using Silhouette Score
evaluator_silhouette = ClusteringEvaluator(featuresCol="scaled_features", predictionCol="cluster", metricName="silhouette", distanceMeasure='cosine')
silhouette_score = evaluator_silhouette.evaluate(predictions)
print(f"Silhouette Score: {silhouette_score}")

# Analyze results
predictions.groupBy("cluster").count().show()

predictions = predictions.drop('features')
predictions = predictions.drop('scaled_features')

## Connect to the file in Google Bucket with Spark
predictions_path = f"gs://it4043e-it5384/it4043e/it4043e_group11_problem2/prediction-data-project"

predictions.write.parquet(predictions_path)

spark.stop()
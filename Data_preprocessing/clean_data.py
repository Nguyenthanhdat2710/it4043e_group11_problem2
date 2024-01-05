import json
import pandas as pd
from functools import reduce
from google.cloud import storage
from google.cloud.storage import Client

def upload_file_to_google_cloud_storage(bucket_name, file_name, local_csv_path):
    client = storage.Client.from_service_account_json('service-account\key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_csv_path, content_type='application/octet-stream')

    print(f'The file {local_csv_path} has been uploaded to {file_name} in the {bucket_name} bucket.')

# Create a client using the credentials
client = Client.from_service_account_json('service-account/key.json')
# Replace 'your_bucket_name' with the name of your Google Cloud Storage bucket
bucket_name = 'it4043e-it5384'

# Replace 'source_object_name' with the name of the object you want to download
folder = 'it4043e/it4043e_group11_problem2/crawl_data/'
files = ['output.json']
destination = 'data/'
destination_files = ['output.json']

# Get the bucket and the specific blob (object) within the bucket
bucket = client.get_bucket(bucket_name)

def download_blob(source_blob_name, destination_file_name):
    # Download the blob to a specified destination
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(f'File {source_blob_name} downloaded to {destination_file_name}.')

for i in range(0, len(files)):
    source_blob_name = folder + files[i]
    destination_file_name = destination + destination_files[i]
    download_blob(source_blob_name, destination_file_name)

with open("jobs/extract-field/tweet.txt", "r") as post_file:
    post_tag = [data.strip().split(" ") for data in post_file]
    post_data = {key: [] for key in [sub[-1] for sub in post_tag]}

with open("jobs/extract-field/user.txt", "r") as user_file:
    user_tag = [data.strip().split(" ") for data in user_file]
    user_data = {key: [] for key in [sub[-1] for sub in user_tag]}

file = open("local_data/output.json", 'r', encoding="utf-8")
data = json.load(file)
unique_user = set()

for post in data:
    # Check language: if English -> continue
    if post["original_tweet"]["lang"] != "en":
        continue

    # Check if user is in set or not
    if post["author"]["original_user"]["screen_name"] not in unique_user:
        unique_user.add(post["author"]["original_user"]["screen_name"])

        # Check if post has "verified_type": if not, set to None
        if "verified_type" not in post['author']['original_user']:
            user_data["verified_type"].append(None)
        else:
            user_data["verified_type"].append(post['author']['original_user']['verified_type'])

        for utag in user_tag[1:]:
            user_data[utag[-1]].append(reduce(lambda d, k: d[k], utag, post))

    # Get post data
    hashtagtext = ""
    for idx, hashtag in enumerate(post['hashtags']):
        if idx < len(post['hashtags']) - 1:
            hashtagtext += hashtag["text"] + ", "
        else:
            hashtagtext += hashtag["text"]
    post_data['hashtags'].append(hashtagtext)
    for ptag in post_tag[1:]:
        post_data[ptag[-1]].append(reduce(lambda d, k: d[k], ptag, post))

# Detect bot
df_user = pd.DataFrame(user_data)
df_post = pd.DataFrame(post_data)

df_user['verified_type'] = df_user['verified_type'].fillna(value='none')
df_post['created_at'] = pd.to_datetime(df_post['created_at'], format='%a %b %d %H:%M:%S +0000 %Y')
df_post['created_at'] = (pd.to_datetime(df_post['created_at']).view('int64') // 10**9).astype('int64')
df_user['created_at'] = (pd.to_datetime(df_user['created_at']).view('int64') // 10**9).astype('int64')

# Convert to parquet file
df_user.to_parquet('user_data_test.parquet', index=False)
df_post.to_parquet('user_data_test.parquet', index=False)

upload_file_to_google_cloud_storage('it4043e-it5384', 'it4043e/it4043e_group11_problem2/crawl_data/user_data_test.parquet', 'user_data_test.parquet')

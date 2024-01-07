import json
import pandas as pd
from functools import reduce
from google.cloud import storage

def upload_to_gcs(bucket_name, file_name, local_path):
    client = storage.Client.from_service_account_json('service-account/key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(local_path, content_type='application/octet-stream')

    print(f'File {local_path} uploaded to {file_name} in {bucket_name} bucket.')

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client.from_service_account_json('service-account/key.json')
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(f'File {source_blob_name} downloaded to {destination_file_name}.')

with open("jobs/extract-field/tweet.txt", "r") as post_file:
    post_tags = [data.strip().split(" ") for data in post_file]

with open("jobs/extract-field/user.txt", "r") as user_file:
    user_tags = [data.strip().split(" ") for data in user_file]

with open("data/output.json", 'r', encoding="utf-8") as json_file:
    data = json.load(json_file)

unique_user = set()
user_data = {key: [] for key in [sub[-1] for sub in user_tags]}
post_data = {key: [] for key in [sub[-1] for sub in post_tags]}

for post in data:
    if post["original_tweet"]["lang"] != "en":
        continue
    
    if post["author"]["original_user"]["screen_name"] not in unique_user:
        unique_user.add(post["author"]["original_user"]["screen_name"])

        user_data["verified_type"].append(post['author']['original_user'].get('verified_type', None))
        for utag in user_tags[1:]:
            user_data[utag[-1]].append(reduce(lambda d, k: d[k], utag, post))

    hashtag_text = ", ".join([hashtag["text"] for hashtag in post['hashtags']])
    post_data['hashtags'].append(hashtag_text)
    for ptag in post_tags[1:]:
        post_data[ptag[-1]].append(reduce(lambda d, k: d[k], ptag, post))

df_user = pd.DataFrame(user_data).fillna(value='none')
df_post = pd.DataFrame(post_data)
df_user['created_at'] = pd.to_datetime(df_user['created_at']).view('int64') // 10**9
df_post['created_at'] = pd.to_datetime(df_post['created_at']).view('int64') // 10**9

df_user.to_parquet('user_data_test.parquet', index=False)
df_post.to_parquet('post_data_test.parquet', index=False)

upload_to_gcs('it4043e-it5384', 'it4043e/it4043e_group11_problem2/extracted_data/user_data_test.parquet', 'user_data_test.parquet')
upload_to_gcs('it4043e-it5384', 'it4043e/it4043e_group11_problem2/extracted_data/post_data_test.parquet', 'post_data_test.parquet')

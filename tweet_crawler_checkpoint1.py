from typing import List, Union
from tweety import Twitter
import pandas as pd
import os
import json
import sys

# Xác định đường dẫn cho tệp cấu hình YAML
current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import yaml

# Đọc tệp YAML cấu hình
def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Đọc cấu hình YAML thành công")
    return config

# Chuyển đổi và ghi dữ liệu vào tệp JSON
def convert_and_write_to_json(data, json_filename):
    # Mở tệp JSON ở chế độ ghi
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Ghi dữ liệu vào tệp JSON
        json_file.write('[')
        for idx, tweet in enumerate(data):
            json.dump(tweet, json_file, ensure_ascii=False, indent=4, default=str)
            if idx < len(data) - 1:
                json_file.write(',')  # Thêm dấu phẩy giữa các đối tượng
            json_file.write('\n')
        json_file.write(']')

# Thu thập dữ liệu Twitter
def crawl_tweet(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 50
) -> List[pd.DataFrame]:
    for keyword in keywords:
        print(f"Đang thu thập với từ khóa '{keyword}'")

        # Sử dụng API Twitter để tìm tweet dựa trên từ khóa và tiêu chí
        tweets = app.search(f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}", pages=pages, wait_time=wait_time)

        # Chuyển đổi và ghi dữ liệu vào tệp JSON
        convert_and_write_to_json(tweets, f"{keyword}.json")

        # In thông tin tweet
        for tweet in tweets:
            print(tweet.__dict__)

if __name__ == "__main__":
    # Đường dẫn đến tệp cấu hình YAML
    CONFIG_PATH = os.path.join(os.getcwd(), "config.yaml")
    config = read_yaml(path=CONFIG_PATH)

    # Đăng nhập vào tài khoản Twitter
    app = Twitter("session")
    with open("account.key", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)

    # Gọi hàm để thu thập dữ liệu từ Twitter
    crawl_tweet(
        app=app,
        keywords=config['keywords'],
        min_faves=config['min_faves'],
        min_retweets=config['min_retweet'],
        pages=config['pages'],
        wait_time=config['wait_time']
    )

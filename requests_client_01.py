import base_client_01
import producer_02
import pandas as pd
import time
import json
import requests

sample_row = producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[0]
r = requests.post(url='http://127.0.0.1:9875/rating', json=json.dumps(sample_row))
print(r)
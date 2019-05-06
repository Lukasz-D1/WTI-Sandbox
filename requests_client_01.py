import base_client_01
import producer_02
import pandas as pd
import time
import json
import requests

sample_rows = []
for i in range(100):
    sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[i])

#
# sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[0])
# sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[1])
# sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[2])
# sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[3])
# sample_rows.append(producer_02.get_dataframe_as_dict(producer_02.merge_user_ratings_with_movie_genres())[4])
#
# first_get = requests.get(url='http://127.0.0.1:9875/ratings')
# print(first_get.text)
#
# r = requests.post(url='http://127.0.0.1:9875/rating', json=json.dumps(sample_rows[0]))
# print(r.text)
#
# r2 = requests.post(url='http://127.0.0.1:9875/rating', json=json.dumps(sample_rows[1]))
# print(r2.text)
#
# r3 = requests.post(url='http://127.0.0.1:9875/rating', json=json.dumps(sample_rows[2]))
# print(r3.text)
#
# r4 = requests.post(url='http://127.0.0.1:9875/rating', json=json.dumps(sample_rows[3]))
# print(r4.text)
#
# r5 = requests.get(url='http://127.0.0.1:9875/ratings')
# print(r5.text)
#
# # r6 = requests.delete(url='http://127.0.0.1:9875/ratings')
# # print(r6.text)
#
# r7 = requests.get(url='http://127.0.0.1:9875/ratings')
# print(r7.text)
#
# r8 = requests.get(url='http://127.0.0.1:9875/avg-genre-ratings/all-users')
# print(r8.text)
#
# r9 = requests.get(url='http://127.0.0.1:9875/avg-genre-ratings/75')
# print(r9.text)
#
# r10 = requests.get(url='http://127.0.0.1:9875/user-profile/75')
# print(r10.text)

if __name__ == '__main__':
    first_get = requests.get(url='http://0.0.0.0:9898/ratings')
    print(first_get.text)

    r = requests.post(url='http://0.0.0.0:9898/rating', json=json.dumps(sample_rows[0]))
    print(r.text)

    r2 = requests.post(url='http://0.0.0.0:9898/rating', json=json.dumps(sample_rows[1]))
    print(r2.text)

    r3 = requests.post(url='http://0.0.0.0:9898/rating', json=json.dumps(sample_rows[2]))
    print(r3.text)

    r4 = requests.post(url='http://0.0.0.0:9898/rating', json=json.dumps(sample_rows[3]))
    print(r4.text)

    r5 = requests.get(url='http://0.0.0.0:9898/ratings')
    print(r5.text)

    # r6 = requests.delete(url='http://127.0.0.1:9875/ratings')
    # print(r6.text)

    r7 = requests.get(url='http://0.0.0.0:9898/ratings')
    print(r7.text)

    r8 = requests.get(url='http://0.0.0.0:9898/avg-genre-ratings/all-users')
    print(r8.text)

    r9 = requests.get(url='http://0.0.0.0:9898/avg-genre-ratings/75')
    print(r9.text)

    r10 = requests.get(url='http://0.0.0.0:9898/user-profile/75')
    print(r10.text)

    for i in range(1000):
        req = requests.get(url='http://0.0.0.0:9898/avg-genre-ratings/all-users')
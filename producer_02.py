import base_client_01
import pandas as pd
import json
import time
import copy
import numpy as np

redis_client = base_client_01.redis_client

user_ratings_columns = {
    'userID': int,
    'movieID': int,
    'rating': float,
    'date_day': int,
    'date_month': int,
    'date_year': int,
    'date_minute': int,
    'date_second': int
}

user_ratings = pd.read_csv('/home/lukasz/user_ratedmovies.dat', sep='\t', dtype=user_ratings_columns, encoding='latin=1', nrows=1000)

movies_genres_columns = {
    'movieID': int,
    'genre': str
}

movie_genres = pd.read_csv('/home/lukasz/movie_genres.dat', sep='\t', dtype=movies_genres_columns, encoding='latin-1')

def convert_dataframe_to_object(data_frame):
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.astype.html
    return data_frame.astype(object)

def print_json_from_dataframe(data_frame):
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.iterrows.html
    for index, row in convert_dataframe_to_object(data_frame).iterrows():
        row_as_json = row.to_json(orient='index')
        print(row_as_json)

def get_dataframe_as_json(data_frame):
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
    return data_frame.to_json(orient='index')

def get_dataframe_as_dict(data_frame):
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html
    return data_frame.to_dict('index')

# lab 4 zad 3
def get_dict_as_dataframe(dictionary):
    return pd.DataFrame.from_dict(dictionary, orient="index")

def produce_messages_to_queue(data_frame_to_be_sent):
    queue_name = "produced_queue_01"
    message = get_dataframe_as_dict(data_frame_to_be_sent)
    for i in range(100):
        base_client_01.send_dictionary_to_queue(queue_name, message[i])
        # https://docs.python.org/3/library/stdtypes.html#str.format
        print("Sent {}. message to: {}. Content:".format(i, queue_name))
        print(message[i])
        time.sleep(1)

def generate_pivoted_movie_genres_table():
    # https://docs.python.org/2/library/copy.html
    # https://docs.python.org/3/library/copy.html#copy.deepcopy
    temp_movie_genres = copy.deepcopy(movie_genres)
    temp_movie_genres['genre'] = 'genre-' + temp_movie_genres['genre'].astype(str)
    temp_movie_genres['dummyCol']=1
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.pivot_table.html
    temp_movie_genres_pivoted = temp_movie_genres.pivot_table(index='movieID', columns='genre', values='dummyCol', fill_value=0)
    return temp_movie_genres_pivoted

def merge_user_ratings_with_movie_genres():
    movie_genres_pivoted = generate_pivoted_movie_genres_table()
    user_ratings_with_movie_genres = pd.merge(user_ratings, movie_genres_pivoted, on='movieID')
    return user_ratings_with_movie_genres

# lab 4 zad 1
def get_user_ratings_with_movie_genres():
    merged_tables = merge_user_ratings_with_movie_genres()
    merged_tables_dict = merged_tables.to_dict()
    column_names = list(merged_tables_dict)
    genres_names = []
    for column_name in column_names:
        if column_name[:6] == "genre-":
            genres_names.append(column_name)
    return merged_tables, genres_names

# def calc_avg_for_genre():
#     merged_ratings_with_genres, genres_names = get_user_ratings_with_movie_genres()
#     genres_dict_with_avg = dict.fromkeys(genres_names)
#     for genre in genres_dict_with_avg:
#         genres_dict_with_avg[genre]=[]
#     merged_ratings_with_genres_dict = merged_ratings_with_genres.to_dict(orient='index')
#     for index in merged_ratings_with_genres_dict:
#         for key in merged_ratings_with_genres_dict[index]:
#             if key in genres_dict_with_avg:
#                 if merged_ratings_with_genres_dict[index][key] == 1:
#                     genres_dict_with_avg[key].append(merged_ratings_with_genres_dict[index]['rating'])
#     for key in genres_dict_with_avg:
#         genres_dict_with_avg[key]=np.nanmean(genres_dict_with_avg[key])
#     merged_ratings_with_genres_dict_unbiased = copy.deepcopy(merged_ratings_with_genres_dict)
#     for index in merged_ratings_with_genres_dict_unbiased:
#         for key in merged_ratings_with_genres_dict_unbiased[index]:
#             if key in genres_dict_with_avg:
#                 if merged_ratings_with_genres_dict_unbiased[index][key] == 1:
#                     merged_ratings_with_genres_dict_unbiased[index]['rating'] = merged_ratings_with_genres_dict_unbiased[index]['rating']-genres_dict_with_avg[key]
#     return genres_dict_with_avg, merged_ratings_with_genres_dict_unbiased

def comapre():
    x = merge_user_ratings_with_movie_genres()
    y = get_dataframe_as_dict(x)
    z = get_dict_as_dataframe(y)
    result_of_comp = x == z
    print(result_of_comp)


def calc_avg_for_genre(df, genres_list):
    merged_ratings_with_genres, genres_names = df, genres_list
    genres_dict_with_avg = dict.fromkeys(genres_names)
    for genre in genres_dict_with_avg:
        genres_dict_with_avg[genre] = []
    merged_ratings_with_genres_dict = merged_ratings_with_genres.to_dict(orient='index')
    for index in merged_ratings_with_genres_dict:
        for key in merged_ratings_with_genres_dict[index]:
            if key in genres_dict_with_avg:
                if merged_ratings_with_genres_dict[index][key] == 1:
                    genres_dict_with_avg[key].append(merged_ratings_with_genres_dict[index]['rating'])
    for key in genres_dict_with_avg:
        genres_dict_with_avg[key] = np.nanmean(genres_dict_with_avg[key])
    merged_ratings_with_genres_dict_unbiased = copy.deepcopy(merged_ratings_with_genres_dict)
    for index in merged_ratings_with_genres_dict_unbiased:
        for key in merged_ratings_with_genres_dict_unbiased[index]:
            if key in genres_dict_with_avg:
                if merged_ratings_with_genres_dict_unbiased[index][key] == 1:
                    merged_ratings_with_genres_dict_unbiased[index]['rating'] = merged_ratings_with_genres_dict_unbiased[index]['rating'] - genres_dict_with_avg[key]
    return genres_dict_with_avg, merged_ratings_with_genres_dict_unbiased


def calc_avg_for_user(df, genres_list, userID):
    df_for_user = df.loc[df['userID'] == int(userID)]
    x, y = calc_avg_for_genre(df_for_user, genres_list)
    x['userID'] = userID
    return x, y

def user_dif(user_mean,all_mean):
    new_mean={}
    for genres,mean in user_mean.items():
        if genres == 'userID':
            continue
        elif mean> 0.0:
            new_mean[genres]=all_mean[genres]-mean
        else:
            new_mean[genres]=mean
    pom=[v for v in new_mean.values() ]
    pom=np.array(pom)
    nans=np.isnan(pom)
    pom[nans]=0
    return new_mean,pom
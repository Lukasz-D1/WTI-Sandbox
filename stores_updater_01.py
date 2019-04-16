import copy
import redis
import json
import numpy as np
import producer_02

class api_logic:

    def __init__(self):
        self.raw_ratings_data = []
        self.user_ratings_with_genres, self.genres_column_names = producer_02.get_user_ratings_with_movie_genres()
        self.avg_genre_ratings_for_user = {}
        self.user_profile = {}
        self.queue_name = "ratings_list"
        self.redis_ratings_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=2)
        self.redis_profiles_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=1)
        self.ratings_list_name = "ratings_list"



    def add_rating(self, new_rating):
        self.redis_ratings_client.rpush(self.queue_name, json.dumps(new_rating))
        #self.raw_ratings_data.append(new_rating)

    def get_ratings(self):
        dummy_dict = []
        ratings_dummy = self.redis_ratings_client.lrange(self.queue_name, 0, -1)
        for value in ratings_dummy:
            dummy_dict.append(json.loads(value))
        return json.dumps(dummy_dict)
        #return self.raw_ratings_data

    def delete_ratings(self):
        queue = self.get_ratings()
        length_of_queue = len(queue)
        del_queue = self.redis_ratings_client.ltrim(self.queue_name, length_of_queue, -1)
        #self.raw_ratings_data=[]

    def compute_avg_genre_ratings(self):
        self.avg_genres, merged_unbiased = producer_02.calc_avg_for_genre(self.user_ratings_with_genres, self.genres_column_names)
        avg_genres_key_str = "avg_genre_ratings"
        self.redis_profiles_client.set(avg_genres_key_str, json.dumps(self.avg_genres))
        return self.avg_genres

    def compute_avg_genre_ratings_for_user(self, user_id):
        user_averages, x = producer_02.calc_avg_for_user(self.user_ratings_with_genres, self.genres_column_names, user_id)
        user_key_str = "avg_genre_ratings_user_" + str(user_id)
        self.redis_profiles_client.set(user_key_str, json.dumps(user_averages))
        return user_averages

    def compute_user_profile(self, user_id):
        self.compute_avg_genre_ratings()
        self.compute_avg_genre_ratings_for_user(user_id)
        self.avg_genre_ratings_for_user[user_id] = json.loads(self.redis_profiles_client.get("avg_genre_ratings_user_"+str(user_id)))
        self.avg_genres = json.loads(self.redis_profiles_client.get("avg_genre_ratings"))
        self.user_profile[user_id] = {key: self.avg_genre_ratings_for_user[user_id][key] - self.avg_genres.get(key, 0) for key in self.avg_genre_ratings_for_user[user_id].keys()}
        return self.user_profile[user_id]

if __name__ == '__main__':
    api_logic_obj = api_logic()
    api_logic_obj.compute_avg_genre_ratings()
    api_logic_obj.compute_avg_genre_ratings_for_user(75)
    api_logic_obj.compute_user_profile(75)
import copy
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import simple_cassandra_client_01
import json
import numpy as np
import producer_02
import math
import pandas as pd
import cassandra_client_01

class api_logic:

    def __init__(self):
        # Poprzedni kontener dla przechowywania ratingow
        self.raw_ratings_data = []
        self.user_ratings_with_genres, self.genres_column_names = producer_02.get_user_ratings_with_movie_genres()
        self.avg_genre_ratings_for_user = {}
        self.user_profile = {}
        # Wybrana przeze mnie nazwa kolejki dla listy wszystkich ratingow
        # Klienci redisa obslugujacy baze danych (kolejke), ktora przechowuje liste wszystkich ratingow
        # self.redis_ratings_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=2)
        # self.redis_profiles_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=1)
        # queue name to wczesniej bylo ratings list
        self.cassandra_client = cassandra_client_01.cassandra_client();

    # Metoda obslugujaca dodawanie new_rating do Redisowej listy
    def add_rating(self, new_rating):
        # patrz redis_ratings_client
        # Do redisowej kolejki przechowujacej ratingi dodajemy new_rating
        # Za pomoca metody rpush(<nazwa_kolejki>, <dodawany rating>)
        self.cassandra_client.insert_into_table_ratings(new_rating)

        # Poprzedni sposob, w ktorym zamiast dodawac do redisowej kolejki dodawalismy ratingi do prostej tablicy
        #self.raw_ratings_data.append(new_rating)

    # Metoda obslugujaca pobieranie wszystkich ratingow z redisowej bazy
    def get_ratings(self):
        # Deklaracja tymczasowej tablicy
        dummy_dict = []
        # Za pomoca funkcji lrange wyciagamy wszystkie ratingi z okreslonej kolejki do zmiennej
        ratings_dummy = self.cassandra_client.session.execute("SELECT * FROM keyspace_01.user_ratings;")
        # Ta petla zamienia kazdy element z powyzszej tablicy na slownik i dolacza go to tymczasowej tablicy zadeklarowanej na poczatku
        for row in ratings_dummy:
            dummy_dict.append(row)
        # Zwracamy tablice zawierajaca slowniki reprezentujace ratingi
        # w formie JSON wymagana przez GET
        return dummy_dict

        # Wczesniej zwracalismy prosta tablice
        #return self.raw_ratings_data

    # Metoda obslugujaca usuniecie ratingow z redisowej kolejki
    def delete_ratings(self):
        self.cassandra_client.clear_table("user_ratings;")

    def compute_avg_genre_ratings(self):
        list_of_ratings = self.get_ratings()
        df = pd.DataFrame.from_dict(list_of_ratings)
        self.avg_genres, merged_unbiased = producer_02.calc_avg_for_genre(df, self.genres_column_names)
        for key, value in self.avg_genres.items():
            if np.isnan(value):
                self.avg_genres[key] = np.nan_to_num(self.avg_genres[key])
        return self.avg_genres

    def compute_avg_genre_ratings_for_user(self, user_id):
        list_of_ratings = self.get_ratings()
        df = pd.DataFrame.from_dict(list_of_ratings)
        user_averages, x = producer_02.calc_avg_for_user(df, self.genres_column_names, str(user_id))
        for key, value in user_averages.items():
            if key[:6] == "genre-":
                if np.isnan(user_averages[key]):
                    user_averages[key] = np.nan_to_num(user_averages[key])
        return user_averages

    def compute_user_profile(self, user_id):
        user_id = int(user_id)
        self.compute_avg_genre_ratings()
        self.compute_avg_genre_ratings_for_user(user_id)
        self.avg_genre_ratings_for_user = self.compute_avg_genre_ratings_for_user(user_id)
        self.avg_genre_ratings_for_user.pop("userID")
        self.avg_genres = self.compute_avg_genre_ratings()
        self.user_profile = {key: self.avg_genre_ratings_for_user[key] - self.avg_genres[key] for key in self.avg_genre_ratings_for_user.keys()}
        return self.user_profile

if __name__ == '__main__':
    api_logic_obj = api_logic()
    api_logic_obj.compute_avg_genre_ratings()
    api_logic_obj.compute_avg_genre_ratings_for_user(75)
    print(api_logic_obj.compute_avg_genre_ratings())
    print(api_logic_obj.compute_avg_genre_ratings_for_user(75))
    print(api_logic_obj.compute_user_profile(75))
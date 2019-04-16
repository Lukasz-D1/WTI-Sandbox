import copy
import redis
import json
import numpy as np
import producer_02

class api_logic:

    def __init__(self):
        # Poprzedni kontener dla przechowywania ratingow
        self.raw_ratings_data = []
        self.user_ratings_with_genres, self.genres_column_names = producer_02.get_user_ratings_with_movie_genres()
        self.avg_genre_ratings_for_user = {}
        self.user_profile = {}
        # Wybrana przeze mnie nazwa kolejki dla listy wszystkich ratingow
        self.queue_name = "ratings_list"
        # KLient redisa obslugujacy baze danych (kolejke), ktora przechowuje liste wszystkich ratingow
        self.redis_ratings_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=2)
        self.redis_profiles_client = redis.StrictRedis(host='localhost', charset='utf-8', port=6381, db=1)
        self.ratings_list_name = "ratings_list"


    # Metoda obslugujaca dodawanie new_rating do Redisowej listy
    def add_rating(self, new_rating):
        # patrz redis_ratings_client
        # Do redisowej kolejki przechowujacej ratingi dodajemy new_rating
        # Za pomoca metody rpush(<nazwa_kolejki>, <dodawany rating>)
        self.redis_ratings_client.rpush(self.queue_name, json.dumps(new_rating))

        # Poprzedni sposob, w ktorym zamiast dodawac do redisowej kolejki dodawalismy ratingi do prostej tablicy
        #self.raw_ratings_data.append(new_rating)

    # Metoda obslugujaca pobieranie wszystkich ratingow z redisowej bazy
    def get_ratings(self):
        # Deklaracja tymczasowej tablicy
        dummy_dict = []
        # Za pomoca funkcji lrange wyciagamy wszystkie ratingi z okreslonej kolejki do zmiennej
        ratings_dummy = self.redis_ratings_client.lrange(self.queue_name, 0, -1)
        # Ta petla zamienia kazdy element z powyzszej tablicy na slownik i dolacza go to tymczasowej tablicy zadeklarowanej na poczatku
        for value in ratings_dummy:
            dummy_dict.append(json.loads(value))
        # Zwracamy tablice zawierajaca slowniki reprezentujace ratingi
        # w formie JSON wymagana przez GET
        return json.dumps(dummy_dict)

        # Wczesniej zwracalismy prosta tablice
        #return self.raw_ratings_data

    # Metoda obslugujaca usuniecie ratingow z redisowej kolejki
    def delete_ratings(self):
        # Do tymczasowej kolejki, wrzucamy wszystkie ratingi z bazy
        queue = self.get_ratings()
        # Do zmiennej length_of_queue zapisujemy dlugosc powyzszej tymczasowej kolejki
        length_of_queue = len(queue)
        # Usuwamy wszystkie rzeczy z kolejki za pomoca funkcji ltrim(<nazwa kolejki>, start, stop) wywolanej na odpowiednim kliencie redisa
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
import pandas as pd
import numpy as np
import pprint
from elasticsearch import Elasticsearch, helpers, exceptions


class ElasticClient:

    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

        self._users_index_params = {
            "name": "users",
            "doc_type": "user"
        }

        self._movies_index_params = {
            "name": "movies",
            "doc_type": "movie"
        }

    def index_documents(self):
        df = pd.read_csv('/home/lukasz/user_ratedmovies.dat', delimiter='\t', nrows=1000) \
                 .loc[:, ['userID', 'movieID', 'rating']]

        means = df.groupby(['userID'], as_index=False, sort=False) \
            .mean() \
            .loc[:, ['userID', 'rating']] \
            .rename(columns={'rating': 'ratingMean'})

        df = pd.merge(df, means, on="userID", how="left", sort=False)

        df['ratingNormal'] = df['rating'] - df['ratingMean']

        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)

        print("Indexing users")
        index_users = [{
            "_index": self._users_index_params["name"],
            "_type": self._users_index_params["doc_type"],
            "_id": index,
            "_source": {
                'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")

        print("Indexing movies")
        index_movies = [{
            "_index": self._movies_index_params["name"],
            "_type": self._movies_index_params["doc_type"],
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id):

        return self.es.get(index=self._users_index_params["name"],
                           doc_type=self._users_index_params["doc_type"],
                           id=int(user_id))["_source"]

    def get_users_that_like_movie(self, movie_id):

        return self.es.get(index=self._movies_index_params["name"],
                           doc_type=self._movies_index_params["doc_type"],
                           id=int(movie_id))["_source"]

    def collaborative_filtering_users(self, user_id):
        # Filmy, ktore polubil uzytkownik o danym ID
        movies_liked_by_user = self.get_movies_liked_by_user(user_id)['ratings']

        # Slownik przechowujacy filmy z informacja kto je polubil
        movies_and_users_who_liked_them_dict = dict.fromkeys(movies_liked_by_user)

        # Kontener na propozycje filmowe
        suggested_movies = []

        # Dla kazdego filmu w slowniku...
        for movie_id in movies_and_users_who_liked_them_dict:

            # Przypisz uzytkownikow ktorzy polubili dany film i...
            movies_and_users_who_liked_them_dict[movie_id] = self.get_users_that_like_movie(movie_id)['whoRated']

            # Dla kazdego uzytkownika, ktory polubyil dany film...
            for user_who_liked_movie in movies_and_users_who_liked_them_dict[movie_id]:

                # Jesli to nie jest uzytkownik, dla ktorego robimy filtracje to...
                if user_who_liked_movie != user_id:

                    # Z filmow, ktore polubil drugi uzytkownik, wybierz wszystkie te, ktorych nie polubil ten pierwszy
                    distinction_of_two_users = set(
                        self.get_movies_liked_by_user(user_who_liked_movie)['ratings']) - set(
                        self.get_movies_liked_by_user(user_id)['ratings'])

                    # Dla kazdego takiego znalezionego filmu...
                    for movie in distinction_of_two_users:

                        # Jesli film nie jest juz wpisany na liste polecanych...
                        if movie not in suggested_movies:

                            # Dopisz film do listy polecanych
                            suggested_movies.append(movie)
        return sorted(suggested_movies)

    @staticmethod
    def as_list(x):
        if type(x) is list:
            return x
        else:
            return [x]

    def get_user_document(self, user_id):
        try:
            res = self.es.get(index=self._users_index_params["name"],
                              doc_type=self._users_index_params["doc_type"],
                              id=int(user_id))
            return res
        except exceptions.NotFoundError as error:
            return error.info

    def get_user_ratings(self, user_id):
        res = self.get_user_document(user_id)
        print(res)
        if res['found'] is True:
            movies = res["_source"]["ratings"]
            return sorted(movies)
        else:
            return []

    def get_movie_document(self, movie_id):
        try:
            res = self.es.get(index=self._movies_index_params["name"],
                              doc_type=self._movies_index_params["doc_type"],
                              id=int(movie_id))
            return res
        except exceptions.NotFoundError as error:
            return error.info

    def get_movie_raters(self, movie_id):
        res = self.get_movie_document(movie_id)
        print(res)
        if res['found'] is True:
            users = res["_source"]["whoRated"]
            return sorted(users)
        else:
            return []

    def add_user_document(self, user_id, movies_rated_by_user):
        """
        Method that adds a new user document. If movies_rated_by_user parameter is a single integer, it will
        transform that value into a single-element list. The method decides whether to add a new movie document
        (if a certain movie_id is missing from movies index), or to update existing movie document.

        :param user_id: integer representing a new user's ID we want to put into users index
        :param movies_rated_by_user: integer/list of movie IDs (already existing or missing from movies index)

        :return: elasticSearch query
        """
        body = {
            "ratings": self.as_list(movies_rated_by_user)
        }

        for movie in self.as_list(movies_rated_by_user):
            query = self.get_movie_document(movie)
            if query['found'] is False:
                new_movie_body = {
                    "whoRated": self.as_list(user_id)
                }
                self.es.index(index=self._movies_index_params["name"],
                              doc_type=self._movies_index_params["doc_type"],
                              id=movie,
                              body=new_movie_body)
            else:
                updated_movie_doc = self.get_movie_document(movie)
                updated_movie_body = {
                    "doc": {
                        "whoRated": list(set(updated_movie_doc["_source"]["whoRated"] + self.as_list(user_id)))
                    }
                }
                self.es.update(index=self._movies_index_params["name"],
                               doc_type=self._movies_index_params["doc_type"],
                               id=movie,
                               body=updated_movie_body)

        return self.es.index(index=self._users_index_params["name"],
                             doc_type=self._users_index_params["doc_type"],
                             id=user_id,
                             body=body)

    def add_movie_document(self, movie_id, list_of_users):
        """
        Method that adds a new movie document. If list_of_users parameter is a single integer, it will
        transform that value into a single-element list. The method decides whether to add a new user document
        (if a certain user_id is missing from users index), or to update existing users document.

        :param movie_id: integer representing a new movie's ID we want to put into movies index
        :param list_of_users: integer/list of user IDs (already existing or missing from users index)

        :return: elasticSearch query
        """
        body = {
            "whoRated": self.as_list(list_of_users)
        }
        for user in self.as_list(list_of_users):
            query = self.get_user_document(user)
            if query['found'] is False:
                new_user_body = {
                    "ratings": self.as_list(movie_id)
                }
                self.es.index(index=self._users_index_params["name"],
                              doc_type=self._users_index_params["doc_type"],
                              id=user,
                              body=new_user_body)
            else:
                updated_user_doc = self.get_user_document(user)
                updated_user_body = {
                    "doc": {
                        "ratings": list(set(updated_user_doc["_source"]["ratings"] + self.as_list(movie_id)))
                    }
                }
                self.es.update(index=self._users_index_params["name"],
                               doc_type=self._users_index_params["doc_type"],
                               id=user,
                               body=updated_user_body)

        return self.es.index(index=self._movies_index_params["name"],
                             doc_type=self._movies_index_params["doc_type"],
                             id=int(movie_id), body=body)

    def update_user_document(self, user_id, movies_seen, index="users"):
        res = self.get_user_document(user_id)
        print(res)
        if res['found'] is True:
            body = {
                "doc": {
                    "ratings": list(set(res["_source"]["ratings"] + self.as_list(movies_seen)))
                }
            }

            for movie in self.as_list(movies_seen):
                query = self.get_movie_document(movie)
                if query['found'] is False:
                    new_movie_body = {
                        "whoRated": self.as_list(user_id)
                    }
                    self.es.index(index=self._movies_index_params["name"],
                                  doc_type=self._movies_index_params["doc_type"],
                                  id=movie,
                                  body=new_movie_body)
                else:
                    updated_movie_doc = self.get_movie_document(movie)
                    updated_movie_body = {
                        "doc": {
                            "whoRated": list(set(updated_movie_doc["_source"]["whoRated"] + self.as_list(user_id)))
                        }
                    }
                    self.es.update(index=self._movies_index_params["name"],
                                   doc_type=self._movies_index_params["doc_type"],
                                   id=movie,
                                   body=updated_movie_body)

            return self.es.update(index=self._users_index_params["name"],
                                  doc_type=self._users_index_params["doc_type"],
                                  id=user_id,
                                  body=body)
        else:
            print("No user {} in index: {}".format(user_id, self._users_index_params["name"]))
            return []

    def update_movie_document(self, movie_id, list_of_users, index="movies"):
        res = self.get_movie_document(movie_id)
        print(res)

        if res['found'] is True:
            body = {
                "doc": {
                    "whoRated": list(set(res["_source"]["whoRated"] + self.as_list(list_of_users)))
                }
            }
            for user in self.as_list(list_of_users):
                query = self.get_user_document(user)
                if query['found'] is False:
                    new_user_body = {
                        "ratings": self.as_list(movie_id)
                    }
                    self.es.index(index=self._users_index_params["name"],
                                  doc_type=self._users_index_params["doc_type"],
                                  id=user,
                                  body=new_user_body)
                else:
                    updated_user_doc = self.get_user_document(user)
                    updated_user_body = {
                        "doc": {
                            "ratings": list(set(updated_user_doc["_source"]["ratings"] + self.as_list(movie_id)))
                        }
                    }
                    self.es.update(index=self._users_index_params["name"],
                                   doc_type=self._users_index_params["doc_type"],
                                   id=user,
                                   body=updated_user_body)

            return self.es.update(index=self._movies_index_params["name"],
                                  doc_type=self._movies_index_params["doc_type"],
                                  id=movie_id,
                                  body=body)
        else:
            print("No movie {} in index {}".format(movie_id, self._movies_index_params["doc_type"]))
            return []

    def delete_user_document(self, user_id):
        return self.es.delete(index=self._users_index_params["name"],
                              doc_type=self._users_index_params["doc_type"],
                              id=user_id)

    def delete_movie_document(self, movie_id):
        return self.es.delete(index=self._movies_index_params["name"],
                              doc_type=self._movies_index_params["doc_type"],
                              id=movie_id)


if __name__ == "__main__":
    ec = ElasticClient()
    ec.index_documents()

    u1_id = 75

    print(sorted(ec.get_movies_liked_by_user(75)['ratings']))

    print(ec.get_users_that_like_movie(296))

    print(sorted(ec.get_movies_liked_by_user(78)['ratings']))

    print(ec.collaborative_filtering_users(75))

    # list(set(ec.get_movies_liked_by_user(75)['ratings']).intersection(ec.get_movies_liked_by_user(78)['ratings']))
    # sorted(set(ec.get_movies_liked_by_user(78)['ratings'])-set(ec.get_movies_liked_by_user(75)['ratings']))

    # print()
    # user_document = ec.get_movies_liked_by_user(75)
    # movie_id = np.random.choice(user_document['ratings'])
    # movie_document = ec.get_users_that_like_movie(movie_id)
    # random_user_id = np.random.choice(movie_document['whoRated'])
    # random_user_document = ec.get_movies_liked_by_user(random_user_id)
    #
    # print('User 75 liked following movies:')
    # print(user_document)
    #
    # print('Movie {} is liked by following users:'.format(movie_id))
    # print(movie_document)
    #
    # print('Is user 75 among users in movie {} document?'.format(movie_id))
    # print(movie_document['whoRated'].index(75)!=-1)
    #
    # import random
    #
    # some_test_movie_ID=1
    # print("Some test movie ID", some_test_movie_ID)
    # list_of_users_who_liked_movie_of_given_ID = \
    #     ec.get_users_that_like_movie(some_test_movie_ID)['whoRated']
    #
    # print('List of users who liked test movie:', *list_of_users_who_liked_movie_of_given_ID)
    # index_of_random_user = random.randint(0, len(list_of_users_who_liked_movie_of_given_ID))
    #
    # print("Index of random user who liked movie: ", index_of_random_user)
    # some_test_user_ID = list_of_users_who_liked_movie_of_given_ID[index_of_random_user]
    #
    # movies_liked_by_user_of_test_ID = ec.get_movies_liked_by_user(some_test_user_ID)["ratings"]
    #
    # print("IDs of movies liked by random user who liked the test movie: ", *movies_liked_by_user_of_test_ID)

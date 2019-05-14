from cassandra.cluster import Cluster
from cassandra.query import dict_factory

class cassandra_client:
    def __init__(self):
        self.keyspace = "keyspace_01"
        self.cluster = Cluster(['127.0.0.1'], port=9042)
        self.session = self.cluster.connect()
        self.create_keyspace(keyspace=self.keyspace)
        self.create_table_ratings(keyspace=self.keyspace)
        self.session.row_factory = dict_factory


    def create_keyspace(self, keyspace):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'}
        """)


    # def create_table_profiles(self, keyspace):
    #     self.session.execute("""
    #         CREATE TABLE IF NOT EXISTS """ + keyspace + """.user_profiles(
    #         user_id int,
    #         avg_movie_rating float,
    #         PRIMARY KEY(user_id)
    #         )
    #     """)

    def clear_table(self, table):
        self.session.execute("TRUNCATE "+self.keyspace+ "."+table)


    def create_table_ratings(self, keyspace):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS """ + keyspace + """.user_ratings(
            "userID" int,
            "movieID" int,
            rating float,
            "genre-Action" int,
            "genre-Adventure" int,
            "genre-Animation" int,
            "genre-Children" int,
            "genre-Comedy" int,
            "genre-Crime" int,
            "genre-Documentary" int,
            "genre-Drama" int,
            "genre-Fantasy" int,
            "genre-Film-Noir" int,
            "genre-Horror" int,
            "genre-IMAX" int,
            "genre-Musical" int,
            "genre-Mystery" int,
            "genre-Romance" int,
            "genre-Sci-Fi" int,
            "genre-Short" int,
            "genre-Thriller" int,
            "genre-War" int,
            "genre-Western" int,            
            PRIMARY KEY(\"userID\")
            )
        """)

    def insert_into_table_ratings(self, new_rating):
        self.session.execute("INSERT INTO keyspace_01.user_ratings (\"userID\", \"movieID\", rating, \"genre-Action\","
                             "\"genre-Adventure\", \"genre-Animation\", \"genre-Children\", \"genre-Comedy\", \"genre-Crime\","
                             "\"genre-Documentary\", \"genre-Drama\", \"genre-Fantasy\", \"genre-Film-Noir\", \"genre-Horror\", \"genre-IMAX\","
                             "\"genre-Musical\", \"genre-Mystery\", \"genre-Romance\", \"genre-Sci-Fi\", \"genre-Short\", \"genre-Thriller\","
                             "\"genre-War\", \"genre-Western\") VALUES (%(userID)s, %(movieID)s, %(rating)s, %(genre-Action)s,"
                             "%(genre-Adventure)s, %(genre-Animation)s, %(genre-Children)s, %(genre-Comedy)s, "
                             "%(genre-Crime)s, %(genre-Documentary)s, %(genre-Drama)s, %(genre-Fantasy)s, %(genre-Film-Noir)s,"
                             "%(genre-Horror)s, %(genre-IMAX)s, %(genre-Musical)s, %(genre-Mystery)s, "
                             "%(genre-Romance)s, %(genre-Sci-Fi)s, %(genre-Short)s, %(genre-Thriller)s,"
                             "%(genre-War)s, %(genre-Western)s)", {
                                    "userID": new_rating["userID"],
                                    "movieID": new_rating["movieID"],
                                    "rating": new_rating["rating"],
                                    "genre-Action": new_rating["genre-Action"],
                                    "genre-Adventure": new_rating["genre-Adventure"],
                                    "genre-Animation": new_rating["genre-Animation"],
                                    "genre-Children": new_rating["genre-Children"],
                                    "genre-Comedy": new_rating["genre-Comedy"],
                                    "genre-Crime": new_rating["genre-Crime"],
                                    "genre-Documentary": new_rating["genre-Documentary"],
                                    "genre-Drama": new_rating["genre-Drama"],
                                    "genre-Fantasy": new_rating["genre-Fantasy"],
                                    "genre-Film-Noir": new_rating["genre-Film-Noir"],
                                    "genre-Horror": new_rating["genre-Horror"],
                                    "genre-IMAX": new_rating["genre-IMAX"],
                                    "genre-Musical": new_rating["genre-Musical"],
                                    "genre-Mystery": new_rating["genre-Mystery"],
                                    "genre-Romance": new_rating["genre-Romance"],
                                    "genre-Sci-Fi": new_rating["genre-Sci-Fi"],
                                    "genre-Short": new_rating["genre-Short"],
                                    "genre-Thriller": new_rating["genre-Thriller"],
                                    "genre-War": new_rating["genre-War"],
                                    "genre-Western": new_rating["genre-Western"]
        })

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

def create_keyspace(session, keyspace):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'}
    """)

def create_table(session, keyspace, table):
    session.execute("""
        CREATE TABLE IF NOT EXISTS """ + keyspace + """."""+table+"""(
        user_id int,
        avg_movie_rating float,
        PRIMARY KEY(user_id)
        )
    """)

def push_data_table(session, keyspace,table,userID,avgMovieRating):
    session.execute("""
        INSERT INTO """ + keyspace + """.""" + table + """ (user_id, avg_movie_rating)
        VALUES (%(user_id)s, %(avg_movie_rating)s)
        """,{'user_id':userID, 'avg_movie_rating':avgMovieRating})

def get_data_table(session, keyspace, table):
    rows = session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    for row in rows:
        print(row)

def clear_table(session, keyspace, table):
    session.execute("TRUNCATE "+keyspace+"."+table+";")

def delete_table(session, keyspace, table):
    session.execute("DROP TABLE " +keyspace + "."  + table + ";")

if __name__ == '__main__':
    print('SIMPLE CASSANDRA CLIENT')
    keyspace = "user_ratings"
    table = "user_avg_rating"

    print("Podlaczanie do klastra...")
    cluster = Cluster(['127.0.0.1'], port=9042)
    session=cluster.connect()

    print("Utworzenie przestrzeni kluczy...")
    create_keyspace(session, keyspace)

    print("Ustanowienie przestrzeni kluczy dla sesji...")
    session.set_keyspace(keyspace)

    # Kreator slownikow
    session.row_factory = dict_factory

    print("Tworzenie tabeli...")
    create_table(session, keyspace, table)

    print("Dodawanie danych do tabeli...")
    push_data_table(session, keyspace, table, userID=1337, avgMovieRating=4.2)

    print("Odczytywanie danych z tabeli...")
    get_data_table(session, keyspace, table)

    print("Czyszczenie tabeli...")
    clear_table(session, keyspace, table)

    print("Odczytywanie danych z tabeli...")
    get_data_table(session, keyspace, table)

    print("Usuwanie tableli z przestrzeni kluczy...")
    delete_table(session, keyspace, table)

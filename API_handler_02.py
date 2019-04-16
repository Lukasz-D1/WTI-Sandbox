import flask
from flask import Flask
import producer_02
import base_client_01
import json
import random
import stores_updater_01

# Tworzenie aplikacji Flask, ktora umozliwi dzialanie naszego API
app = Flask(__name__)

# Tworzenie instancji obiektu obslugujacego Redisowe bazy danych (API Logic)
api_logic_obj = stores_updater_01.api_logic()

# Tworzenie serwera localhost na porcie 9875
def server():
    app.run(host='127.0.0.1', port=9875)

# Definicja endpointu '/', ktory zwraca Hello World
@app.route('/')
def hello_world():
    return 'Hello World!'

# Definicja endpointu '/rating' - metoda POST
# Umozliwia on dodanie pojedynczego ratingu do Redisowej listy ratingow
@app.route('/rating', methods=['POST'])
def add_rating():
    # Pobierz new_rating w postaci JSON z wyslanego zapytania POST za pomoca flask.request.get_json()
    new_rating = flask.request.get_json()
    # Przetworz pobrany w postaci JSON new_rating na slownik za pomoca json.loads(new_rating)
    new_rating_to_dict = json.loads(new_rating)
    # Wywoluje metode add_rating() obietku api_logic, ktorej zadaniem jest obsluga dodawania new_rating do kolejki
    api_logic_obj.add_rating(new_rating_to_dict)
    # Zwroc dodany rating, oraz 201 czyli informacje o sukcesie
    return new_rating, 201

# Definicja endpointu '/ratings' - metoda GET
# Umozliwia on odczytanie wszystkich ratingow aktualnie znajdujacych sie w redisowej liscie przechowujacej ratingi
@app.route('/ratings', methods=['GET'])
def get_movie_ratings():
    # Zwroc ratingi w formie JSON, oraz kod 201
    return json.dumps(api_logic_obj.get_ratings()), 201

# Definicja endpointu '/ratings' - metoda DELETE
# Umozliwia usuniecie wszystkich ratingow, ktore aktualnie przechowuje redisowa kolejka
@app.route('/ratings', methods=['DELETE'])
def delete_ratings():
    # Wywolanie metody delete_ratings() obiektu api_logic
    api_logic_obj.delete_ratings()
    # Zwrocenie wiadomosci o usunieciu wszystkich ratingow
    return "Deleted all ratings", 201

@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_genre_ratings_for_all_users():
    return json.dumps(api_logic_obj.compute_avg_genre_ratings()), 201

@app.route('/avg-genre-ratings/<userID>', methods=['GET'])
def get_avg_genre_rating_for_specific_user(userID):
    return json.dumps(api_logic_obj.compute_avg_genre_ratings_for_user(userID)), 201

@app.route('/user-profile/<userID>', methods=['GET'])
def get_user_profile(userID):
    return json.dumps(api_logic_obj.compute_user_profile(userID)), 201

server()

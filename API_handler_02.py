import flask
from flask import Flask
import producer_02
import base_client_01
import json
import random
import stores_updater_01

app = Flask(__name__)

api_logic_obj = stores_updater_01.api_logic()

def server():
    app.run(host='127.0.0.1', port=9875)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/rating', methods=['POST'])
def add_rating():
    new_rating = flask.request.get_json()
    new_rating_to_dict = json.loads(new_rating)
    api_logic_obj.add_rating(new_rating_to_dict)
    return new_rating, 201

@app.route('/ratings', methods=['DELETE'])
def delete_ratings():
    api_logic_obj.delete_ratings()
    return "Deleted all ratings", 201

@app.route('/ratings', methods=['GET'])
def get_movie_ratings():
    return json.dumps(api_logic_obj.get_ratings()), 201

@app.route('/all_ratings', methods=['GET'])
def get_all_movie_ratings():
    return producer_02.get_dataframe_as_json(producer_02.merge_user_ratings_with_movie_genres())

@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_genre_ratings_for_all_users():
    return json.dumps(api_logic_obj.compute_avg_genre_ratings()), 201

@app.route('/avg-genre-ratings/<userID>', methods=['GET'])
def get_avg_genre_rating_for_specific_user(userID):
    return json.dumps(api_logic_obj.compute_avg_genre_ratings_for_user(userID)), 201

server()

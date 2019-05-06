import flask
from flask import Flask
import producer_02
import base_client_01
import json
import random

app = Flask(__name__)

container = []

def random_values_for_genres():
    random_genres_values={}
    for dictionary in container:
        column_names=list(dictionary)
        print(column_names)
        for column_name in column_names:
            print(column_name)
            if column_name[:6]=="genre-":
                random_genres_values[column_name]=random.random()
    return random_genres_values

def random_values_for_genres_for_specific_user(userID):
    random_genres_values = {}
    for dictionary in container:
        random_genres_values["userID"] = userID
        if dictionary["userID"] == int(userID):
            column_names = list(dictionary)
            for column_name in column_names:
                if column_name[:6] == "genre-":
                    random_genres_values[column_name] = random.random()
    return random_genres_values

def server():
    app.run(host='127.0.0.1', port=9875)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/rating', methods=['POST'])
def add_rating():
    new_rating = flask.request.get_json()
    new_rating_to_dict = json.loads(new_rating)
    container.append(new_rating_to_dict)
    return new_rating, 201

@app.route('/rating', methods=['DELETE'])
def delete_ratings():
    container.clear()
    return "Deleted all ratings", 201

@app.route('/ratings', methods=['GET'])
def get_movie_ratings():
    return json.dumps(container), 201

@app.route('/all_ratings', methods=['GET'])
def get_all_movie_ratings():
    return producer_02.get_dataframe_as_json(producer_02.merge_user_ratings_with_movie_genres())

@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_avg_genre_ratings_for_all_users():
    return json.dumps(random_values_for_genres()), 201

@app.route('/avg-genre-ratings/<userID>', methods=['GET'])
def get_avg_genre_rating_for_specific_user(userID):
    return json.dumps(random_values_for_genres_for_specific_user(userID)), 201

if __name__ == '__main__':
    server()

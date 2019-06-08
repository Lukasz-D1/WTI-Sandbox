from flask import Flask, jsonify, abort, request
from simple_elastic_client_01 import ElasticClient

app = Flask(__name__)
es = ElasticClient()


@app.route("/user/document/<user_id>", methods=["GET"])
def get_user(user_id):
    try:
        result = es.get_movies_liked_by_user(user_id)
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/document/<movie_id>", methods=["GET"])
def get_movie(movie_id):
    try:
        result = es.get_users_that_like_movie(movie_id)
        return jsonify(result)
    except:
        abort(404)


@app.route("/user/preselection/<user_id>", methods=["GET"])
def user_preselection(user_id):
    try:
        result = es.collaborative_filtering_users(user_id)
        result = {
            "moviesFound": result
        }
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/preselection/<movie_id>", methods=["GET"])
def movies_preselection(movie_id):
    return "Not implemented yet"


@app.route("/user/document/<user_id>", methods=["PUT"])
def add_user_document(user_id):
    try:
        movies_liked_by_user = request.json
        es.add_user_document(user_id, movies_liked_by_user)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["PUT"])
def add_movie_document(movie_id):
    try:
        users_that_like_movie = request.json
        es.add_movie_document(movie_id, users_that_like_movie)
        return "OK", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["POST"])
def update_user_document(user_id):
    try:
        movies_liked_by_user = request.json
        es.update_user_document(user_id, movies_liked_by_user)
        return "OK", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["POST"])
def update_movie_document(movie_id):
    try:
        users_that_like_movie = request.json
        es.update_movie_document(movie_id, users_that_like_movie)
        return "OK", 200
    except:
        abort(400)


@app.route("/user/document/<user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        movies_liked_by_user = request.json
        es.delete_user_document(user_id)
        return "OK", 200
    except:
        abort(400)


@app.route("/movie/document/<movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        users_that_like_movie = request.json
        es.delete_movie_document(movie_id)
        return "OK", 200
    except:
        abort(400)


if __name__ == '__main__':
    es.index_documents()
    app.run()

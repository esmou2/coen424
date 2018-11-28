from flask import Flask, request, json
from flask_cors import CORS

from engine import RecommendationEngine

app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def test():
    return "hey"


@app.route("/", methods=["POST"])
def send_recommendation():
    # if request.data.get_json():
    return json.dumps(recommendation_engine.get_prediction(request.get_json()))
    # return json.dumps({"error": "bad data format"}), 500


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

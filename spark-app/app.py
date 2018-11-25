from flask import Flask, request, json

from engine import RecommendationEngine

app = Flask(__name__)


@app.route("/", methods=["GET"])
def test():
    return "hey"


@app.route("/", methods=["POST"])
def send_recommendation():
    return json.dumps(recommendation_engine.get_prediction(request.get_json()))


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

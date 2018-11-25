from flask import Flask, request, json

from engine import RecommendationEngine

app = Flask(__name__)


@app.route("/", methods=["GET"])
def test():
    return "hey"


@app.route("/", methods=["POST"])
def send_recommendation():
    date = request.get_json()
    category = date["category"]
    main_category = date["main_category"]
    duration = date["duration"]
    usd_goal_real = date["usd_goal_real"]

    return json.dumps(recommendation_engine.get_accuracy(request.data))


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

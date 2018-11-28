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
    received_data = request.get_json()
    if received_data and received_data.hasOwnProperty("category") and received_data.hasOwnProperty(
            "main_category") and received_data.hasOwnProperty("duration") and received_data.hasOwnProperty(
            "usd_goal_real"):
        return json.dumps(recommendation_engine.get_prediction(received_data))
    return json.dumps({"error": "bad data format"}), 400


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

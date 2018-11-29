from flask import Flask, request, json
from flask_cors import CORS, cross_origin

from engine import RecommendationEngine

app = Flask(__name__)
CORS(app, support_credentioals=True)


@app.route("/", methods=["GET"])
def test():
    return "hey"


@app.route("/", methods=["POST"])
@cross_origin(support_credentioals=True)
def send_recommendation():
    print(request)
    print(request.get_json())
    print(request.data)
    received_data = request.get_json()
    if received_data and set(received_data.keys()) == {"category", "main_category", "duration", "usd_goal_real"}:
        prediction, metrics = recommendation_engine.get_results(received_data)
        data = {'prediction': prediction, 'metrics': metrics}
        return json.dumps(data)
    return json.dumps({"error": "bad data format"}), 400


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

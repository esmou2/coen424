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
    if received_data and set(received_data.keys()) == {"category", "main_category", "duration", "usd_goal_real"}:
        prediction, state_count, m_cat_count, m_cat_count_state, m_cat_sum_goals = recommendation_engine.get_results(received_data)
        data = {}
        data['prediction'] = prediction
        data['state_count'] = state_count
        data['m_cat_count'] = m_cat_count
        data['m_cat_count_state'] = m_cat_count_state
        data['m_cat_sum_goals'] = m_cat_sum_goals
        return json.dumps(data)
    return json.dumps({"error": "bad data format"}), 400


def create_app(spark_session):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session)
    return app

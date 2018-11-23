from flask import Blueprint, json

main = Blueprint('main', __name__)
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask


@main.route("/", methods=["GET"])
def test():
    return "hey"


@main.route("/", methods=["POST"])
def send_recommendation():
    x = {
        "name": "John",
        "age": 30,
        "city": "New York"
    }

    return json.dumps(x)


def create_app(spark_context):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app

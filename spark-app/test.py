from flask import Flask
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession

app = Flask(__name__)


@app.route('/', methods=["GET"])
def hello():
    return "YOYO"

model = ALS(userCol="userId", itemCol="movieId", ratingCol="rating").fit(ratings)


if __name__ == '__main__':

    app.run()

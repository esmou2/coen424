from flask import Flask
from pyspark.sql import SparkSession

app = Flask(__name__)


@app.route('/', methods=["GET"])
def hello():
    return "YOYO"


if __name__ == '__main__':

    app.run()

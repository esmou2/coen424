from pyspark.sql import SparkSession

from app import create_app


def init_spark_session():
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .getOrCreate()
    spark.sparkContext.addPyFile('engine.py')
    spark.sparkContext.addPyFile('app.py')
    return spark


if __name__ == "__main__":
    ss = init_spark_session()
    app = create_app(ss)
    app.run(host="0.0.0.0", port="3000")

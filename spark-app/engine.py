from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


class RecommendationEngine:

    def count(self):
        return self.df.count()

    def label_for_results(s):
        if s == 'failed':
            return 0.0
        elif s == 'successful':
            return 1.0
        else:
            return -1.0

    def __init__(self, ss):
        self.ss = ss
        self.df = ss.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.df.printSchema()
        label = udf(self.label_for_results, DoubleType())
        labeled_data = self.df.select(label("state").alias('label'), "duration", "usd_goal_real") \
            .where('label >= 0')
        labeled_data.take(1).show()

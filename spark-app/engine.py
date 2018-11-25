class RecommendationEngine:

    def count(self):
        return self.df.count()

    def __init__(self, ss):
        self.ss = ss
        self.df = ss.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.df.printSchema()

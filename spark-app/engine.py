from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, json
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString


class RecommendationEngine:

    def count(self):
        return self.df.count()

    def get_prediction(self, j):
        json_obj = self.ss.sparkContext.parallelize([json.dumps(j)])
        new_data = self.ss.read.json(json_obj)
        predictions = self.model_rf.transform(new_data)
        result = predictions.rdd.map(lambda x: {"prediction": x.predictedLabel}).collect()
        self.state_count.show()
        self.main_category_count.show()
        self.main_category_count_state.show()
        return result

    def __init__(self, ss):
        self.ss = ss
        df = ss.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.labeled_data = df.select("state", "main_category", "duration", "usd_goal_real")
        predict_data, test_data, train_data = self._split_data()
        pipeline_rf = self._create_pipeline()
        self.model_rf = pipeline_rf.fit(train_data)
        self._test_classifier(test_data)

        self.state_count = self.labeled_data.groupby("state").count()
        self.main_category_count = self.labeled_data.groupby("main_category").count()
        self.main_category_count_state = self.labeled_data.groupby(["main_category", "state"]).count()

    def _test_classifier(self, test_data):
        predictions = self.model_rf.transform(test_data)
        predictions.show(10)
        evaluator_rf = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                         metricName="accuracy")
        accuracy = evaluator_rf.evaluate(predictions)
        print("Accuracy = %g" % accuracy)
        print("Test Error = %g" % (1.0 - accuracy))

    def _create_pipeline(self):
        string_indexer_label = StringIndexer(inputCol="state", outputCol="label").fit(self.labeled_data)
        string_indexer_main_category = StringIndexer(inputCol="main_category", outputCol="main_category_IX")
        # string_indexer_category = StringIndexer(inputCol="category", outputCol="category_IX")
        vector_assembler_features = VectorAssembler(
            inputCols=["main_category_IX", "duration", "usd_goal_real"],
            outputCol="features")
        rf = RandomForestClassifier(labelCol="label", featuresCol="features")
        label_converter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                        labels=string_indexer_label.labels)
        pipeline_rf = Pipeline(
            stages=[string_indexer_label, string_indexer_main_category,
                    vector_assembler_features, rf, label_converter])
        return pipeline_rf

    def _split_data(self):
        splitted_data = self.labeled_data.randomSplit([0.8, 0.18, 0.02], 24)
        train_data = splitted_data[0]
        test_data = splitted_data[1]
        predict_data = splitted_data[2]
        return predict_data, test_data, train_data

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, json
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString


class RecommendationEngine:

    def get_results(self, j):
        predictions = self._get_prediction(j)
        result = predictions.rdd.map(lambda x: {"prediction": x.predictedLabel}).collect()
        m_cat_count, m_cat_count_state, m_cat_sum_goals = self._get_metrics(j.get("category"))
        return result, self._extract_metrics(m_cat_count, m_cat_count_state, m_cat_sum_goals)

    def _extract_metrics(self, m_cat_count, m_cat_count_state, m_cat_sum_goals):
        data = {"per_failed_project": self.per_failed_project, "per_successful_project": self.per_successful_project,
                "per_projects_in_cat": (m_cat_count[0][1] * 100) / self.count,
                "avg_goal_usd": m_cat_sum_goals[0][1] / m_cat_count[0][1],
                "acc": self.accuracy}

        if "failed" in m_cat_count_state[0]:
            data["per_failed_project"] = (m_cat_count_state[0][2] * 100) / m_cat_count[0][1]
            data["per_successful_project"] = (m_cat_count_state[1][2] * 100) / m_cat_count[0][1]
        else:
            data["per_failed_project_in_cat"] = (m_cat_count_state[1][2] * 100) / m_cat_count[0][1]
            data["per_successful_project_in_cat"] = (m_cat_count_state[0][2] * 100) / m_cat_count[0][1]

        return data

    def _get_prediction(self, j):
        json_obj = self.ss.sparkContext.parallelize([json.dumps(j)])
        new_data = self.ss.read.json(json_obj)
        predictions = self.model_rf.transform(new_data)
        return predictions

    def _get_metrics(self, category):
        m_cat_count = self.main_category_count.filter(
            self.main_category_count["main_category"].like(category)).collect()
        m_cat_count_state = self.main_category_count_state.filter(
            self.main_category_count_state["main_category"].like(category)).collect()
        m_cat_sum_goals = self.main_category_sum_goals.filter(
            self.main_category_count_state["main_category"].like(category)).collect()

        return m_cat_count, m_cat_count_state, m_cat_sum_goals

    def __init__(self, ss):
        self.ss = ss
        df = ss.read.format("com.mongodb.spark.sql.DefaultSource").load()
        self.labeled_data = df.select("state", "main_category", "duration", "usd_goal_real")
        self.count = self.labeled_data.count()
        state_count = self.labeled_data.groupby("state").count().collect()
        if "failed" in state_count[0]:
            self.per_failed_project = (state_count[0][1] * 100) / self.count
            self.per_successful_project = (state_count[1][1] * 100) / self.count
        else:
            self.per_failed_project = (state_count[1][1] * 100) / self.count
            self.per_successful_project = (state_count[0][1] * 100) / self.count

        predict_data, test_data, train_data = self._split_data()
        pipeline_rf = self._create_pipeline()
        self.model_rf = pipeline_rf.fit(train_data)
        self._test_classifier(test_data)

        self.main_category_count = self.labeled_data.groupby("main_category").count()
        self.main_category_count_state = self.labeled_data.groupby(["main_category", "state"]).count()
        self.main_category_sum_goals = self.labeled_data.groupby(["main_category"]).sum("usd_goal_real")

    def _test_classifier(self, test_data):
        predictions = self.model_rf.transform(test_data)
        predictions.show(10)
        evaluator_rf = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                         metricName="accuracy")
        self.accuracy = evaluator_rf.evaluate(predictions)

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

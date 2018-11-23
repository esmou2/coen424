import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:

    def __init__(self, sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.sc = sc

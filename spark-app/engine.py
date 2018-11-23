import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:

    def __init__(self, sc):

        logger.info("Starting up the Recommendation Engine: ")
        self.sc = sc

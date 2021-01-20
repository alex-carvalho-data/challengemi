from datetime import datetime
from pyspark.sql import SparkSession
import logging

# Syntax for spark-submit to mongo
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 setup/load_dataset_to_mongo.py

JSON_DATASET_FILE = 'dataset/challenge_hearing_tests.json'
MONGO_URI = 'mongodb://root:alex123@localhost:27017/?authSource=admin'
MONGO_DB = 'mimongo'
MONGO_COLLECTION = 'hearing_test'


class MongoDatasetLoader:
    def __init__(self,
                 mongo_uri=MONGO_URI,
                 mongo_db=MONGO_DB,
                 mongo_collection=MONGO_COLLECTION):
        self.start_time = datetime.now()
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

        # getting spark session
        self.spark = SparkSession.builder \
            .master('local[8]') \
            .appName('JsonDatasetToMongo') \
            .getOrCreate()

        # logging setup
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(filename)s.%(funcName)s()'
                                   '-%(levelname)s-%(message)s')

    def _log_header(self, json_file):
        logging.info("-------------------------------------------------------")
        logging.info("------------- Loading Dataset to Mongo ----------------")
        logging.info("-------------------------------------------------------")
        logging.info('starting...')
        logging.info('initial_params:')
        logging.info('mongo_uri: {}'.format(self.mongo_uri))
        logging.info('mongo_db: {}'.format(self.mongo_db))
        logging.info('mongo_collection: {}'.format(self.mongo_collection))
        logging.info('json_dataset_file: {}'.format(json_file))

    def _log_footer(self):
        logging.info('load completed')
        logging.info("-------------------------------------------------------")
        logging.info('start: {}'.format(self.start_time))
        logging.info('  end: {}'.format(datetime.now()))

    def load(self, json_dataset_file):
        self._log_header(json_dataset_file)

        logging.info('reading df from json dataset file')
        mimi_sample_df = self.spark.read.json(json_dataset_file)

        mimi_sample_df.show(truncate=False)
        mimi_sample_df.printSchema()

        logging.info('writing df to MongoDB')
        mimi_sample_df.write \
            .format('mongo') \
            .mode('append') \
            .option('uri', self.mongo_uri) \
            .option("database", self.mongo_db) \
            .option("collection", self.mongo_collection) \
            .save()

        self.spark.stop()

        self._log_footer()


if __name__ == '__main__':
    mongo_loader = MongoDatasetLoader()
    mongo_loader.load(JSON_DATASET_FILE)

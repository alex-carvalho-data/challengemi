from challenge_functions import clean_hearing_test, compute_insights
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, flatten, to_timestamp
from pyspark.sql.types import IntegerType
import json
import logging

CONNECTIONS_INFO_FILE = 'connections.ini'

# Syntax for spark-submit
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 --jars postgresql-42.2.18.jar pipeline_mongo_postgres.py


class HearingTestsPipeline:
    def __init__(self, connections_config_file):
        self.start_time = datetime.now()

        # logging setup
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(filename)s.%(funcName)s()'
                                   '-%(levelname)s-%(message)s')

        mongo_conn_dic = \
            self.get_connection_info(connections_config_file, 'MongoDB')
        self.mongo_uri = self.generate_mongo_uri(mongo_conn_dic)
        self.mongo_host = mongo_conn_dic['host']
        self.mongo_port = mongo_conn_dic['port']
        self.mongo_db = mongo_conn_dic['db']
        self.mongo_collection = mongo_conn_dic['collection']
        self.mongo_user = mongo_conn_dic['user']
        self.mongo_pass = mongo_conn_dic['pass']

        postgres_conn_dic = \
            self.get_connection_info(connections_config_file, 'PostgresSQL')
        self.postgres_uri = self.generate_postgres_uri(postgres_conn_dic)
        self.postgres_driver = postgres_conn_dic['driver']
        self.postgres_host = postgres_conn_dic['host']
        self.postgres_port = postgres_conn_dic['port']
        self.postgres_db = postgres_conn_dic['db']
        self.postgres_user = postgres_conn_dic['user']
        self.postgres_pass = postgres_conn_dic['pass']
        self.postgres_properties = \
            {
                'driver': postgres_conn_dic['driver'],
                'user': postgres_conn_dic['user'],
                'password': postgres_conn_dic['pass']
            }

        self._log_header()

        logging.info('Getting SparkSession')
        self.spark = SparkSession.builder\
            .master('local[8]')\
            .appName('MongoToPostgres')\
            .getOrCreate()

    @staticmethod
    def generate_postgres_uri(postgres_conn_dic):
        """ It receives a dic with PostgreSQL connection info and generates the
                    connection URI

        :param postgres_conn_dic: dic with PostgreSQL connection info
        :return: MongoDB connection URI
        """
        uri = \
            postgres_conn_dic['base_uri'] \
                .format(host=postgres_conn_dic['host'],
                        port=postgres_conn_dic['port'],
                        db=postgres_conn_dic['db'])

        return uri

    @staticmethod
    def generate_mongo_uri(mongo_conn_dic):
        """ It receives a dic with MongoDB connection info and generates the
            connection URI

        :param mongo_conn_dic: dic with mongoDB connection info
        :return: MongoDB connection URI
        """
        uri = \
            mongo_conn_dic['base_uri'] \
                .format(user=mongo_conn_dic['user'],
                        password=mongo_conn_dic['pass'],
                        host=mongo_conn_dic['host'],
                        port=mongo_conn_dic['port'],
                        db=mongo_conn_dic['db'],
                        collection=mongo_conn_dic['collection'])

        return uri

    @staticmethod
    def get_connection_info(connection_file_name, section):
        logging.info(f'retrieving {section} connection info from file')

        parser = ConfigParser()
        parser.read(connection_file_name)

        logging.info(f'retrieving {section} section')
        section_config_dic = {}
        if parser.has_section(section):
            conn_items = parser.items(section)
            for conn_item in conn_items:
                section_config_dic[conn_item[0]] = conn_item[1]
        else:
            raise Exception(f'Section {section} not found '
                            f'in the {connection_file_name} file.')

        return section_config_dic

    def _log_header(self):
        logging.info("-------------------------------------------------------")
        logging.info("----- Hearing Tests Mongo to Postgres Pipeline --------")
        logging.info("-------------------------------------------------------")
        logging.info('starting...')
        logging.info('initial_params:')
        logging.info('[MongoDB]')
        logging.info(f'mongo_uri: {self.mongo_uri}')
        logging.info(f'mongo_host: {self.mongo_host}')
        logging.info(f'mongo_port: {self.mongo_port}')
        logging.info(f'mongo_db: {self.mongo_db}')
        logging.info(f'mongo_collection: {self.mongo_collection}')
        logging.info(f'mongo_user: {self.mongo_user}')
        logging.info('[PostgreSQL]')
        logging.info(f'postgres_uri: {self.postgres_uri}')
        logging.info(f'postgres_driver: {self.postgres_driver}')
        logging.info(f'postgres_host: {self.postgres_host}')
        logging.info(f'postgres_port: {self.postgres_port}')
        logging.info(f'postgres_db: {self.postgres_db}')
        logging.info(f'postgres_user: {self.postgres_user}')

    def _log_footer(self):
        logging.info('load completed')
        logging.info("-------------------------------------------------------")
        logging.info('start: {}'.format(self.start_time))
        logging.info('  end: {}'.format(datetime.now()))

    def _save_df_as_table(self, df, table_name):
        logging.info('-------------------------------------------------------')
        logging.info(f'   Publishing table {table_name.upper()} to PostgreSQL')
        logging.info('-------------------------------------------------------')

        publish_start = datetime.now()

        try:
            df.write.jdbc(url=self.postgres_uri,
                          table=table_name,
                          mode='append',
                          properties=self.postgres_properties)

            logging.info(f'Table {table_name} publishes successfully')
            logging.info(f'publish start: {publish_start}')
            logging.info(f'  publish end: {datetime.now()}')
        except Exception as e:
            logging.error(f'Publish records in table {table_name.upper()} has '
                          f'FAILED\n',
                          f'Exception {e} occurred.')

    @staticmethod
    def get_insight(json_dic):
        """ Receives a Dictionary and enrich it with the provided function

            :param json_dic: a Dictionary that represents a Json line at the RDD
            :return: :class:dict enriched with the provided insights function
        """
        results_dic = json_dic['results']

        insights = []

        for result_dic in results_dic:
            insight = compute_insights(result_dic)
            insight['side'] = result_dic['side']
            insights.append(insight)

        json_dic['insights'] = insights

        return json_dic

    @staticmethod
    def dic_to_row(json_dic):
        """ Receive a dictionary and converts it to a Row

        :param json_dic: a Dictionary that represents a Json line at the RDD
        :return: :class:Row
        """
        metadata = json_dic['metadata']
        metadata_row = Row(**metadata)

        results = json_dic['results']

        results_array = []

        for result in results:

            # results.audiograms
            audiograms = result['audiograms']
            audiograms_array = []
            for audiogram in audiograms:
                audiograms_array.append(Row(**audiogram))

            # results.noise
            noise = result['noise']

            # results.noise.measurements
            measurements = noise['measurements']
            measurements_array = []
            for measurement in measurements:
                measurements_array.append(Row(**measurement))

            # results.noise.meta.config
            config = noise['meta']['config']
            config_row = Row(**config)

            # results.noise.meta
            meta_row = Row(config=config_row,
                           sample_rate=noise['meta']['sample_rate'],
                           version=noise['meta']['version'])

            noise_row = Row(measurements=measurements_array, meta=meta_row)

            # results.side
            side = result['side']

            result_row = Row(audiograms=audiograms_array,
                             noise=noise_row,
                             side=side)
            results_array.append(result_row)

            # user
            user = json_dic['user']
            user_row = Row(**user)

            # insights
            insights = json_dic['insights']
            insights_array = []
            for insight in insights:
                insights_array.append(Row(**insight))

        return Row(id=json_dic['id'],
                   metadata=metadata_row,
                   results=results_array,
                   user=user_row,
                   insights=insights_array)

    @staticmethod
    def prepare_line(json_string):
        """ Receive a String that represents a Json and this json is a RDD line.

            :return: :class:DataFrame cleaned and enriched with the provided
            functions
        """
        json_dic = json.loads(json_string)

        clean_hearing_test(json_dic)

        enriched_dic = HearingTestsPipeline.get_insight(json_dic)

        return HearingTestsPipeline.dic_to_row(enriched_dic)

    @staticmethod
    def _get_audiograms_df(hearing_tests_df):
        logging.info('generating audiograms DataFrame')

        """
        audiograms_df = \
            hearing_tests_df\
                .select(hearing_tests_df.id,
                        hearing_tests_df.results.side,
                        explode(flatten(hearing_tests_df.results.audiograms))
                        .alias('audiogram'))

        audiograms_df = \
            audiograms_df\
                .select(audiograms_df.id.alias('hearing_test_id'),
                        audiograms_df.audiogram.freq_hz.alias('freq_hz'),
                        audiograms_df.audiogram.threshold_db
                        .alias('threshold_db'))
        """

        audiograms_df = \
            hearing_tests_df \
                .select(hearing_tests_df.id,
                        explode(hearing_tests_df.results).alias('result'))

        audiograms_df = \
            audiograms_df \
                .select(audiograms_df.id,
                        audiograms_df.result.side.alias('side'),
                        explode(audiograms_df.result.audiograms)
                        .alias('audiogram'))

        audiograms_df = \
            audiograms_df \
                .select(audiograms_df.id.alias('hearing_test_id'),
                        audiograms_df.side,
                        audiograms_df.audiogram.freq_hz.alias('freq_hz'),
                        audiograms_df.audiogram.threshold_db
                        .alias('threshold_db'))

        audiograms_df.show(truncate=False)
        audiograms_df.printSchema()

        return audiograms_df

    @staticmethod
    def _get_postgres_hearing_tests_df(hearing_tests_df):
        logging.info('generating hearing tests DataFrame')

        postgres_hearing_tests_df = \
            hearing_tests_df\
                .select(hearing_tests_df.id,
                        hearing_tests_df.metadata.deleted.alias('deleted'),
                        hearing_tests_df.metadata.partner_id
                        .alias('partner_id'),
                        hearing_tests_df.metadata.source_app
                        .alias('source_app'),
                        to_timestamp(
                            hearing_tests_df.metadata.submission_timestamp)
                        .alias('submission_timestamp'),
                        hearing_tests_df.user.id.alias('user_id'))

        postgres_hearing_tests_df.show(truncate=False)
        postgres_hearing_tests_df.printSchema()

        return postgres_hearing_tests_df

    @staticmethod
    def _get_insights_df(hearing_tests_df):
        logging.info('generating insights DataFrame')

        insights_df = \
            hearing_tests_df.select(hearing_tests_df.id,
                                    explode(hearing_tests_df.insights)
                                    .alias('insight'))

        insights_df = \
            insights_df.select(insights_df.id.alias('hearing_test_id'),
                               insights_df.insight.PTA4.alias('pta4'),
                               insights_df.insight.side.alias('side'))

        insights_df.show(truncate=False)
        insights_df.printSchema()

        return insights_df



    @staticmethod
    def _get_noises_df(hearing_tests_df):
        logging.info('generating noises DataFrame')

        noises_df = \
            hearing_tests_df\
                .select(hearing_tests_df.id,
                        explode(hearing_tests_df.results).alias('result'))

        noises_df = \
            noises_df\
                .select(noises_df.id.alias('hearing_test_id'),
                        noises_df.result.noise.meta.config.sampling_interval_s
                        .alias('sampling_interval_s'),
                        noises_df.result.noise.meta.sample_rate
                        .alias('sample_rate'),
                        noises_df.result.noise.meta.version.alias('version'),
                        noises_df.result.side.alias('side'))

        noises_df.show(truncate=False)
        noises_df.printSchema()

        return noises_df

    @staticmethod
    def _get_noise_measurements_df(hearing_tests_df):
        logging.info('generating noises mesurements DataFrame')

        noise_measurements_df = \
            hearing_tests_df \
                .select(hearing_tests_df.id,
                        explode(hearing_tests_df.results).alias('result'))

        noise_measurements_df = \
            noise_measurements_df \
                .select(noise_measurements_df.id,
                        noise_measurements_df.result.side.alias('side'),
                        explode(
                            noise_measurements_df.result.noise.measurements)
                        .alias('measurement'))

        noise_measurements_df = \
            noise_measurements_df \
                .select(noise_measurements_df.id.alias('hearing_test_id'),
                        noise_measurements_df.side,
                        noise_measurements_df.measurement.mean_db
                        .alias('mean_db'),
                        to_timestamp(
                            noise_measurements_df.measurement.timestamp)
                        .alias('measure_timestamp'))

        noise_measurements_df.show(truncate=False)
        noise_measurements_df.printSchema()

        return noise_measurements_df

    @staticmethod
    def _get_users_df(hearing_tests_df):
        logging.info('generating users DataFrame')

        user_df = \
            hearing_tests_df\
                .select(hearing_tests_df.user.id.alias('id'),
                        hearing_tests_df.user.year_of_birth.cast(IntegerType())
                        .alias('year_of_birth'))\
                .distinct()

        user_df.show(truncate=False)
        user_df.printSchema()

        return user_df

    def execute(self):
        logging.info('Executing pipeline')

        logging.info('retrieving hearing_tests DataFrame from MongoDB')
        hearing_tests_df = \
            self.spark.read.format('com.mongodb.spark.sql.DefaultSource')\
                .option('uri', self.mongo_uri)\
                .load()

        hearing_tests_df.show()
        hearing_tests_df.printSchema()

        logging.info('converting hearing_tests df from DF to a RDD of string')
        hearing_tests_rdd = hearing_tests_df.toJSON()

        logging.info('cleaning and enriching dataset with provided functions')
        rich_clean_rdd = \
            hearing_tests_rdd.map(HearingTestsPipeline.prepare_line)
        print(rich_clean_rdd.first())

        logging.info('converting back cleaned and enriched RDD to DF')
        hearing_tests_df = self.spark.createDataFrame(rich_clean_rdd)

        hearing_tests_df.cache()

        users_df = self._get_users_df(hearing_tests_df)
        self._save_df_as_table(users_df, table_name='users')

        postgres_hearing_tests_df = \
            self._get_postgres_hearing_tests_df(hearing_tests_df)
        self._save_df_as_table(postgres_hearing_tests_df,
                               table_name='hearing_tests')

        insights_df = self._get_insights_df(hearing_tests_df)
        self._save_df_as_table(insights_df, table_name='insights')

        audiograms_df = self._get_audiograms_df(hearing_tests_df)
        self._save_df_as_table(audiograms_df, table_name='audiograms')

        noises_df = self._get_noises_df(hearing_tests_df)
        self._save_df_as_table(noises_df, table_name='noises')

        noise_measurements_df = \
            self._get_noise_measurements_df(hearing_tests_df)
        self._save_df_as_table(noise_measurements_df,
                               table_name='noise_measurements')

        self.spark.stop()
        self._log_footer()


if __name__ == '__main__':
    hearing_pipeline = HearingTestsPipeline(CONNECTIONS_INFO_FILE)
    hearing_pipeline.execute()

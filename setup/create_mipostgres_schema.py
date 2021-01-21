# linux dependencies
# sudo apt-get install libpq-dev python-dev python3.7-dev
from configparser import ConfigParser
from datetime import datetime
import logging
import psycopg2

DB_CONFIG_FILE = 'setup/database.ini'
TABLES_DDL = (
    """
    create 
     table users 
           ( id varchar(50) primary key,
             year_of_birth int )
    """,
    """
    create 
     table hearing_tests
           ( id varchar(50) primary key,
             submission_timestamp timestamp,
             partner_id varchar(50),
             source_app varchar(50),
             deleted boolean,
             user_id varchar(50) )
    """,
    """
    create 
     table insights
           ( id serial primary key,
             hearing_test_id varchar(50) not null,
             side varchar(5),
             pta4 numeric,
             unique (hearing_test_id, side),
             foreign key (hearing_test_id)
                references hearing_tests (id) )
    """,
    """
    create 
     table audiograms
           ( id serial primary key,
             hearing_test_id varchar(50) not null,
             side varchar(5),
             freq_hz numeric,
             threshold_db numeric,
             unique (hearing_test_id, side, freq_hz),
             foreign key (hearing_test_id)
                references hearing_tests (id) )
    """,
    """
    create 
     table noises
           ( id serial primary key,
             hearing_test_id varchar(50) not null,
             side varchar(5),
             sampling_interval_s int,
             sample_rate int,
             version varchar(20),
             unique (hearing_test_id, side),
             foreign key (hearing_test_id)
                references hearing_tests (id) )
    """,
    """
    create 
     table noise_measurements
           ( id serial primary key,
             hearing_test_id varchar(50) not null,
             side varchar(5),
             measure_timestamp timestamp,
             mean_db numeric,
             unique (hearing_test_id, side, measure_timestamp),
             foreign key (hearing_test_id)
                references hearing_tests (id) )
    """
)


class PostgresSchemaBuilder:
    """ This class reads a file that contains DDL commands for PostgresSQL
        table creation and creates a DB schema with these tables.
    """
    def __init__(self,
                 db_config_file=DB_CONFIG_FILE):
        """ Retrieve DDL for table creation and database connection info from
            files. It also configures the log

        :param db_config_file:
        :param tables_ddl_file:
        """
        self.start_time = datetime.now()
        self.db_config_file = db_config_file

        # logging setup
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s-%(filename)s.%(funcName)s()'
                                   '-%(levelname)s-%(message)s')
        self._log_header()

        self.db_config_dic = self.config(db_config_file)

    @staticmethod
    def config(filename, section='postgresql'):
        """ Reads a file wuth DB config info and returns a dictionary with this
            information

        :param filename: the name of the file where PostgreSQL connection info
            is stored.
        :param section: the section name at the configuration file, where the
            DB configuration is present. Pattern: [postgresql]
        :return: :class:`dict` with the DB configuration retrieved from config
            file.
        """
        logging.info('retrieving PostgreSQL connection info from file')
        # Create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section
        db_config_dic = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db_config_dic[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file.'
                            .format(section, filename))

        return db_config_dic

    def _log_header(self):
        logging.info("-------------------------------------------------------")
        logging.info("------------- Creating PostgreSQL DB ------------------")
        logging.info("-------------------------------------------------------")
        logging.info('starting...')
        logging.info('initial_params:')
        logging.info('db_config_file: {}'.format(self.db_config_file))

    def _log_footer(self):
        logging.info('PostgreSQL DB creation completed')
        logging.info("-------------------------------------------------------")
        logging.info('start: {}'.format(self.start_time))
        logging.info('  end: {}'.format(datetime.now()))

    def create_tables(self):
        """ Main method. It creates the PostgreSQL DB and its tables

        :return: None
        """
        conn = None
        try:
            logging.info('connect to the PostgreSQL server')
            conn = psycopg2.connect(**self.db_config_dic)
            cursor = conn.cursor()
            # create table one by one
            for table_ddl in TABLES_DDL:
                cursor.execute(table_ddl)
            # close communication with PostgreSQL db server
            cursor.close()
            # commit changes
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        self._log_footer()


if __name__ == '__main__':
    postgres_builder = PostgresSchemaBuilder(DB_CONFIG_FILE)
    postgres_builder.create_tables()

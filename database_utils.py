from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml

class DatabaseConnector:

    def __init__(self, db=''):
        self.db = db

    def read_db_creds(self):
        '''Reads the YAML file containing the database credentials.'''
        with open(self.db, 'r') as stream:
            return yaml.safe_load(stream)
    
    def init_db_engine(self):
        '''Initialises database (AWS RDS / PostgreSQL) using credentials from the YAML file.'''
        data = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['HOST']
        PASSWORD = data['PASSWORD']
        USER = data['USER']
        DATABASE = data['DATABASE']
        PORT = data['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self):
        '''Lists the tables present in the AWS RDS database.'''
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            inspector = inspect(engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        '''Uploads the Pandas Dataframe to the local PostgreSQL database.'''
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print('=== Dataframe uploaded to PostgreSQL database ===\n')

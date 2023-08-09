from sqlalchemy import create_engine
from sqlalchemy import inspect
import yaml

class DatabaseConnector:

    def __init__(self, db=""):
        self.db = db

    def read_db_creds(self):
        '''Reads the YAML file containing the database credentials.'''
        with open(self.db, 'r') as stream:
            return yaml.safe_load(stream)
    
    def init_db_engine(self):
        '''Initialises the AWS RDS database using the credentials from the YAML file.'''
        data = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        PASSWORD = data['RDS_PASSWORD']
        USER = data['RDS_USER']
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']
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
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        PASSWORD = '3ve7xjaa'
        USER = 'postgres'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print('=== Dataframe uploaded to PostgreSQL database ===\n')

if __name__ == "__main__":
    pass

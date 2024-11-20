import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(override=True)


def get_postgres_engine():
    """
    Constructs a SQLalchemy engine object for postgres DB from .env file
    
    Paremeters: None
    
    Returns:
    -sqlachemy engine (sqlalchemy.engine.Engine)
    """
    engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
        host = os.getenv('host'),
        port = os.getenv('port'),
        user = os.getenv('user'),
        password = os.getenv('password'),
        dbname = os.getenv('database')
        )
    )
    
    return engine

def load_csv_to_postgres(table_name, engine):
    """
    Loads data from a csv file to a postgres DB table
    
    Parameters:
    
    -table_name(str): a postgres table
    -engine (sqlalchemy.engine): an SQL alchemy eninge object
    
    """
    
    df = pd.read_csv('output/msftstock.csv')
    
    try:
        with engine.connect() as connection:
            df.to_sql('goldfintech', connection, index=False, if_exists='replace')
            
        print(f'{len(df)} rows successfully Loaded to Postgres DB')
    except Exception as e:
        print(f"An error occurred: {e}")
               
    session = sessionmaker(bind=engine)
    session = session()
    session.commit()  
    
# engine = get_postgres_engine()
    

    

    
# load_csv_to_postgres('goldfintech', engine)
# execute code   
# def execute(engine):       
#     session = sessionmaker(bind=engine)
#     session = session()
#     session.commit()
        
        
   
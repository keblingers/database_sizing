from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv,find_dotenv
import os
from pathlib import Path

def get_variables(db):
    evar = Path("/users/fatah/documents/repo/db_sizing/.varenv")
    load_dotenv(dotenv_path=evar)
    uname = os.environ['UNAME'].split(",")
    passwd = os.environ['PASSWD'].split(",")
    database = os.environ['DATABASE'].split(",")
    host = os.environ['HOST'].split(",")
    db_type = os.environ['DB_TYPE'].split(",")
    db_driver = os.environ['DB_DRIVER'].split(",")

    df = pd.DataFrame(list(zip(uname,passwd,database,host,db_type,db_driver)),columns=['uname','passwd','database','host','db_type','db_driver'])
    data = df.query("database == @db")

    return data['uname'].iloc[0],data['passwd'].iloc[0],data['database'].iloc[0],data['host'].iloc[0],data['db_driver'].iloc[0]

def sqlalchemy_conn(db):
    uname, passwd, dbname, hostname, dbdriver = get_variables(db)
    try:
        engine = create_engine(f'{dbdriver}://{uname}:{passwd}@{hostname}/{dbname}')
        connected = engine.connect()
        return connected
    
    except Exception as error:
        print(error)

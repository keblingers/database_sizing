from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv,find_dotenv
import os
from pathlib import Path

def get_variables(host,db,evar):
    load_dotenv(evar)
    uname = os.environ['UNAME'].split(",")
    passwd = os.environ['PASSWD'].split(",")
    database = os.environ['DATABASE'].split(",")
    host = os.environ['HOST'].split(",")
    #db_type = os.environ['DB_TYPE'].split(",")
    db_driver = os.environ['DB_DRIVER'].split(",")
    disk_drive = os.environ['DISK_DRIVE'].split(",")

    df = pd.DataFrame(list(zip(uname,passwd,database,host,db_driver,disk_drive)),columns=['uname','passwd','database','host','db_driver','disk_drive'])
    data = df.query("host == @host and database == @db")

    return data['uname'].iloc[0],data['passwd'].iloc[0],data['database'].iloc[0],data['host'].iloc[0],data['db_driver'].iloc[0],data['disk_drive'].iloc[0]

def sqlalchemy_conn(host,dbtype,evar):
    uname, passwd, dbname, hostname, dbdriver, ddrive = get_variables(host,dbtype,evar)
    try:
        engine = create_engine(f'{dbdriver}://{uname}:{passwd}@{hostname}/{dbname}')
        connected = engine.connect()
        return connected
    
    except Exception as error:
        print(error)

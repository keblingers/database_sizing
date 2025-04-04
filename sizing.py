import pandas as pd
from db_conn import sqlalchemy_conn
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime, date, timedelta
import argparse

def get_date():
    now = datetime.today().strftime("%d-%m-%Y")

    return now

def read_excel(db,xlfile):
    try:
        print("==== read existing xlsx file =====")
        dbsize = pd.read_excel(xlfile,sheet_name=db)
    except Exception as error:
        print(error)
    return dbsize

def get_size(db,evar):
    try:
        print("==== get new data ====")
        conn = sqlalchemy_conn(db,evar)
        now = get_date()
        dbsize = """SELECT table_schema AS "Database", 
                    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS "Size_MB" 
                    FROM information_schema.TABLES 
                    GROUP BY table_schema;"""
        df = pd.read_sql(dbsize,con=conn)
        df.rename(columns={"Size_MB":f"{now}"},inplace=True)
        df.at['Total',now] = df[now].sum()
    except Exception as error:
        print(error)
    return df

def merge_data(db,xlfile,evar):
    if os.path.exists(xlfile):
        try:
            newdata = get_size(db,evar)
            xldata = read_excel(db,xlfile)
            data = pd.merge(xldata,newdata,on=['Database','Database'],how='outer')
            with pd.ExcelWriter(xlfile) as writer:
                data.to_excel(writer,sheet_name=db,index=False)
            return data
        except Exception as error:
            print(error)
    else:
        try:
            newdata = get_size(db)
            with pd.ExcelWriter(xlfile) as writer:
                newdata.to_excel(writer,sheet_name=db,index=False)
            return newdata
        except Exception as error:
            print(error)
            

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="db sizing",description="db sizing")
    parser.add_argument('-f','--excel-file',required=True, help="excel file that used to save the database size history")
    parser.add_argument('-e','--env-file',required=True,help="env file for configuration")
    args = vars(parser.parse_args())
    evar = Path(args['env_file'])
    load_dotenv(evar)
    xlfile = Path(args['excel_file'])
    database = os.environ['DATABASE'].split(",")
    df = pd.DataFrame(list(zip(database)),columns=['database'])
    for x in database:
        merge_data(x,xlfile,evar)
    
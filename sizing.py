import pandas as pd
from db_conn import sqlalchemy_conn,get_variables
from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime, date, timedelta
import argparse
import subprocess
import re
from io import StringIO
from calculate_growth import  get_max_avg_growth, get_disk_info, how_many_days
from openpyxl import load_workbook

# def get_date():
#     now = datetime.today().strftime("%d-%m-%Y")

#     return now

def read_excel_size(host,db,xlfile):
    try:
        print("==== read existing xlsx file =====")
        dbsize = pd.read_excel(xlfile,sheet_name=f'{host}_{db}')
        df_columns = len(dbsize.columns)
        #database_size = dbsize.loc[()]
    except Exception as error:
        print(error)
    return dbsize,df_columns

def get_size(host,dbtype,evar):
    try:
        print("==== get new data ====")
        conn = sqlalchemy_conn(host,dbtype,evar)
        now = datetime.today().strftime("%d-%m-%Y")
        if dbtype == 'mysql':
            dbsize = """SELECT table_schema AS "database", 
                        ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS "size_mb" 
                        FROM information_schema.TABLES 
                        GROUP BY table_schema;"""
        elif dbtype == 'postgres':
            dbsize = """SELECT
                        pg_database.datname as database,
                        pg_database_size(pg_database.datname)/1024/1024 AS size_mb
                        FROM pg_database;"""
        #print(dbsize)
        df = pd.read_sql(dbsize,con=conn)
        df.rename(columns={"size_mb":f"{now}"},inplace=True)
        df.at['Total',now] = df[now].sum()
    except Exception as error:
        print(error)
    return df

def merge_data(host,dbtype,xlfile,evar):
    
    if os.path.exists(xlfile):
        sheet_name = f'{host}_{dbtype}'
        wb = load_workbook(xlfile,read_only=True)
        if sheet_name in wb.sheetnames:
            try:
                print('file and sheet exist')
                newdata = get_size(host,dbtype,evar)
                xldata = read_excel_size(host,dbtype,xlfile)
                data = pd.merge(xldata[0],newdata,on=['database','database'],how='outer')
                with pd.ExcelWriter(xlfile, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                    data.to_excel(writer,sheet_name=f'{host}_{dbtype}',index=False)
                    
                    return data
            except Exception as error:
                    print(error)
        else:
            try:
                newdata = get_size(host,dbtype,evar)
                with pd.ExcelWriter(xlfile, engine='openpyxl', mode='a') as writer:
                    newdata.to_excel(writer,sheet_name=f'{host}_{dbtype}',index=False)
                return newdata
            except Exception as error:
                print(error)
    else:
        try:
            print('file not exists')
            newdata = get_size(host,dbtype,evar)
            with pd.ExcelWriter(xlfile, engine='openpyxl', mode='w') as writer:
                newdata.to_excel(writer,sheet_name=f'{host}_{dbtype}',index=False)
            return newdata
        except Exception as error:
            print(error)
    
def save_detail_space(filepath,detail_sheet,host,dbtype,dsize,dfree,dused,mgrowth,agrowth):
    wb = load_workbook(filepath,read_only=True)
    if detail_sheet in wb.sheetnames:
        data = pd.read_excel(filepath,sheet_name=detail_sheet)
        data.loc[(data['instance_host'] == host) & (data['instance_type'] == dbtype),'disk_size'] = dsize
        data.loc[(data['instance_host'] == host) & (data['instance_type'] == dbtype),'disk_free_space'] = dfree
        data.loc[(data['instance_host'] == host) & (data['instance_type'] == dbtype),'disk_used_space'] = dused
        data.loc[(data['instance_host'] == host) & (data['instance_type'] == dbtype),'max_growth'] = int(mgrowth)
        data.loc[(data['instance_host'] == host) & (data['instance_type'] == dbtype),'avg_growth'] = int(agrowth)

        with pd.ExcelWriter(filepath, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            data.to_excel(writer,sheet_name=detail_sheet,index=False)

    else:
        size_data = {"instance_host": [host],
                    "instance_type": [dbtype],
                    #"disk_drive": ,
                    "disk_size": [dsize],
                    "disk_free_space": [dfree],
                    "disk_used_space": [dused],
                    "max_growth": [mgrowth],
                    "avg_growth": [agrowth]}
        data = pd.DataFrame(size_data)
        
        with pd.ExcelWriter(filepath, engine='openpyxl', mode='a') as writer:
            data.to_excel(writer,sheet_name=detail_sheet,index=False)

def get_detail_space(filepath,sheetname,insname,instype):
    try:
        data = pd.read_excel(filepath,sheet_name=sheetname)
        free_space = data.loc[(data['instance_host'] == insname) & (data['instance_type'] == instype), 'disk_free_space'].values[0]
        avg_growth = data.loc[(data['instance_host'] == insname) & (data['instance_type'] == instype), 'avg_growth'].values[0]
        max_growth = data.loc[(data['instance_host'] == insname) & (data['instance_type'] == instype), 'max_growth'].values[0]
        return free_space,avg_growth,max_growth
    except Exception as error:
        print(error)

            

def sizing_flow(host,dbtype,xlfile,evar):
    uname, passwd, dbname, hostname, dbdriver, ddrive = get_variables(host,dbtype,evar)
    sheetname = f'{host}_{dbtype}'
    detail_sheet = 'detail_size'
    summary_sheet = 'summary_sheet'
    merge_data(host,dbtype,xlfile,evar)

    pd_columns = read_excel_size(host,dbtype,xlfile)

    if pd_columns[1] > 7:
        max_growth, avg_growth, last_size = get_max_avg_growth(xlfile,sheetname)
        size, used, avail, used_pct = get_disk_info(host,ddrive)
        save_detail_space(xlfile,detail_sheet,host,dbtype,size,avail,used,max_growth,avg_growth)
        free_space,avg_growth,max_growth = get_detail_space(xlfile,detail_sheet,host,dbtype)
        how_many_days(xlfile,summary_sheet,host,dbtype,detail_sheet,avg_growth,free_space,last_size)
    else:
        print("== data size is less than 7 days no need to calculated yet ==")
    



if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="db sizing",description="db sizing")
    parser.add_argument('-f','--excel-file',required=True, help="excel file that used to save the database size history")
    parser.add_argument('-e','--env-file',required=True,help="env file for configuration")
    args = vars(parser.parse_args())
    evar = Path(args['env_file'])
    load_dotenv(evar)
    xlfile = Path(args['excel_file'])
    database = os.environ['DATABASE'].split(",")
    host = os.environ['HOST'].split(",")

    for dbtype,hostname in zip(database,host):
        #print(dbtype,hostname)
        sizing_flow(hostname,dbtype,xlfile,evar)
    
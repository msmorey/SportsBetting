import pandas as pd
import psycopg2 as ppg
import sqlalchemy
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
import nflgame
import configparser
import time
import sys
from colorama import Fore, Back, Style

def setup():
    os.chdir("/Users/acttech/Documents/PersonalProjects/SportsBetting")

    config = configparser.ConfigParser()
    config.read("config.ini")
    db = config["db"]["db_name"]
    host = config["db"]["host"]
    user = config["db"]["user"]
    port = config["db"]["port"]
    password = config["db"]["password"]

    engine = sqlalchemy.create_engine('postgresql+psycopg2://'+user+':'+password+'@'+host+':'+port+'/'+db)
    ppg_engine = ppg.connect(host = host, database = db, user = user, password = password)
    ppg_engine.autocommit = True
    cur = ppg_engine.cursor()

    return [engine, cur]

def colors(row):
    try:
        x = float(row['delta'])
        y = float(row['old_delta'])
    except:
        x = row
        y = 0
    if x > y:
        color = Fore.GREEN
    elif x == y:
        color = Style.RESET_ALL
    else:
        color = Fore.RED
    return color + str(x) + Style.RESET_ALL


def retrieve_dash(engine, old_df = None):
    df = pd.read_sql("SELECT * FROM game_dashboard", engine)
    if old_df == None:
        old_df = df.copy()
    old_df = old_df.rename(columns = {'delta': 'old_delta'})
    df = pd.concat([old_df['old_delta'], df], axis=1)
    df['delta'] = df.apply(colors, axis = 1)
    return df

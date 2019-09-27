import pandas as pd
import psycopg2 as ppg
import sqlalchemy
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
import nflgame
import configparser

os.chdir("/Users/acttech/Documents/PersonalProjects/SportsBetting")

config = configparser.ConfigParser()
config.read("config.ini")
host = config["db"]["host"]
user = config["db"]["user"]
port = config["db"]["port"]
password = config["db"]["password"]


rds = ppg.connect(host = "actdashboards.chego5yclvpv.us-east-1.rds.amazonaws.com", database = "ACT", user = "dbadmin", password = rdspass)
rds.autocommit = True
rdscur = rds.cursor()

engine = sqlalchemy.create_engine('postgresql+psycopg2://dbadmin:'+rdspass+'@actdashboards.chego5yclvpv.us-east-1.rds.amazonaws.com:5432/ACT')
meta = sqlalchemy.MetaData(bind=engine)
app_log.info("Connections created")

games = nflgame.games(2019, week=4)
game = games[0]
game.away

#END

#TEST

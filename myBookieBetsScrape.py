import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from pyquery import PyQuery
import numpy as np
import os
import pandas as pd
import psycopg2 as ppg
import sqlalchemy
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
import configparser
import time
import sys
import re


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

def setup_selenium():
    option = webdriver.ChromeOptions()
    option.add_argument(" — incognito")
    browser = webdriver.Chrome(options=option)
    url = "https://engine.mybookie.ag/sports/open-bets"
    browser.get(url)

    input("Press enter when on bet page:")
    source = browser.page_source

    return source

def scrape_page(source):
    bet_keys = ({'date_open':['class="bet-placed"> Placed: ','</span>'], 'type':['bet_type_name text-uppercase"> ','</span>'],
                'risk':['class="bet-risk">Risk: $', '</span>'], 'win':['class="bet-win">Win: $', '</span>']})

    bets = {'id':[], 'date_open':[], 'type':[], 'risk':[], 'win':[]}

    bet_line_keys = {'game_start': ['<span>Game Date :  ', '</span>'], 'team_and_odds': ['<p>', '</p>']}

    bet_lines = {'id': [], 'line': [], 'game_start':[], 'team_and_odds': []}

    bet_search = "Bet Ticket: #"
    i = source.find(bet_search)
    while i != -1:
        id = source[i+len(bet_search):i+21]
        bets['id'].append(id)
        bet = source[i:source.find('<span class="bet-ticket">', i+100)]
        start = 0
        for key, value in bet_keys.items():
            start = bet.find(value[0], start) + len(value[0])
            end = bet.find(value[1], start)
            bets[key].append(bet[start:end])

        start = 0
        total_lines = 0
        while start > -1:
            for key, value in bet_line_keys.items():
                start = bet.find(value[0], start)
                end = bet.find(value[1], start)
                if start == -1 or end == -1:
                    pass
                else:
                    bet_lines[key].append(bet[start+len(value[0]):end])
                    line = True
                start = end
            if line:
                total_lines += 1
        bet_lines['id'].extend([id] * (total_lines-1))
        bet_lines['line'].extend(np.arange(1,total_lines))

        i = source.find(bet_search, i+ 100)
    bets = pd.DataFrame(bets)
    bet_lines = pd.DataFrame(bet_lines)

    return [bets, bet_lines]

def spread(x):
    list = ['-' + y for y in x[1:].split('-')]
    list_plus = ['+' + y for y in x[1:].split('+')]
    if len(list) == 2:
        points = list[0]
        odds = list[1]
        wager = 'spread'
    elif len(list_plus) == 2:
        points = list_plus[0]
        odds = list_plus[1]
        wager = 'spread'
    else:
        points = None
        odds = x
        wager = 'straight'
    return [points, odds, wager]

def find_game_id(x, games):
    team = x.team_id
    date = x.game_start
    try:
        id = games[(games['game_start'] == date) & ((games['home_team'] == team)|(games['away_team'] == team))].iloc[0].id
    except:
        id = None

    return id

def bet_type_parse(x):
    if x.find('PARLAY') != -1:
        type = 'parlay'
    elif x.find('STRAIGHT') != -1:
        type = 'straight'
    else:
        type = 'other'
    return type


def get_bets():
    engine, cur = setup()
    source = setup_selenium()
    bets, bet_lines = scrape_page(source)

    bets['date_open'] = pd.to_datetime(bets['date_open']).dt.date
    bets['type'] = bets['type'].apply(lambda x: bet_type_parse(x))
    values = []
    errors = 0
    bets.columns

    for index, line in bets.iterrows():
        try:
            values.append("'" + str(line.id) +"', '" + str(line.date_open) + "', '" + str(line.date_open) + "', '" + str(line.risk) + "', '" + str(line.type) + "', '" + str(line.win) + "'")
        except:
            errors += 1


    print(f"Couldn't update {errors} bet(s)")

    for value in values:
        sql = (f"""
        INSERT INTO open_bets (id, date_open, date_close, risk, type, win)
        VALUES ({value})
        ON CONFLICT DO NOTHING;
        """)
        cur.execute(sql)


    bet_lines.game_start = bet_lines.game_start.apply(lambda x: '2019 '+x)
    bet_lines.game_start = pd.to_datetime(bet_lines.game_start).dt.date
    # bet_lines.game_start = bet_lines.game_start.apply(lambda x: x.strftime('%Y-%M-%D'))
    bet_lines['team'] = bet_lines.team_and_odds.apply(lambda x: ' '.join(x.split()[1:-1]).lower().title())
    bet_lines['odds_wager'] = bet_lines.team_and_odds.apply(lambda x: x.split()[-1])

    teams = pd.read_sql("SELECT * FROM teams", engine)
    teams = teams.rename(columns = {'id':'team_id'})

    bet_lines = pd.merge(bet_lines, teams[['team_id','long_name']], how="left", left_on="team", right_on="long_name")[['game_start', 'id', 'line', 'team', 'team_id', 'odds_wager']]

    bet_lines['points'] = bet_lines.odds_wager.apply(lambda x: spread(x)[0])
    bet_lines['odds'] = bet_lines.odds_wager.apply(lambda x: spread(x)[1])
    bet_lines['wager'] = bet_lines.odds_wager.apply(lambda x: spread(x)[2])

    bet_lines = bet_lines.drop(['odds_wager'], axis=1)
    bet_lines = bet_lines.rename(columns = {'team': 'team_long', 'id': 'bet_id', 'line': 'line_number', 'wager': 'wager_type'})

    games = pd.read_sql("SELECT * FROM games", engine)
    games['game_start'] = games.game_start.dt.date

    bet_lines['game_id'] = pd.to_numeric(bet_lines.apply(lambda x: find_game_id(x, games), axis = 1), downcast = 'integer')
    bet_lines['game_id'] = pd.to_numeric(bet_lines['game_id'], downcast = 'integer')
    bet_lines['points'] = bet_lines['points'].fillna('0')
    bet_lines['odds'] = bet_lines['odds'].apply(lambda x: re.findall(r'-?\d+', x)[0])
    values = []
    errors = 0
    for index, line in bet_lines.iterrows():
        try:
            values.append("'" + str(line.bet_id) +"', '" + str(line.line_number) + "', '" + line.wager_type + "', '" + str(line.odds) + "', '" + str(int(line.game_id)) + "', '" + str(float(line.points)) + "', '" + str(line.team_id) + "', '" + str(line.team_long) + "'")
        except:
            errors += 1

    print(f"Couldn't update {errors} bet line(s)")

    for value in values:
        sql = (f"""
        INSERT INTO bet_lines (bet_id, line_number, wager_type, odds, game_id, points, team, team_long)
        VALUES ({value})
        ON CONFLICT DO NOTHING;
        """)
        cur.execute(sql)

    return "Success!"

get_bets()


#END



#TEST

test = '-1'
test.isnumeric()'

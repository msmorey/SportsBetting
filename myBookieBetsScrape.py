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
    browser = webdriver.Chrome(options=option)
    url = "https://engine.mybookie.ag/sports/open-bets"
    browser.get(url)

    return browser

def scrape_open_bets_page(source):
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

def scrape_closed_bets_page(source):
    bet_keys = ({'date_open': ['"mr-4 bold">Placed: ', '</span>'],
    'type': ["""<span class="flex-grow-1 text-uppercase">\n"""
    , """
    """], 'risk': ['Risk: $', ' <span data-result'], 'win': ["""Win: $\n"""
    , """
    """] ,'won':['<span data-result="','" class="history-result ml-3"']})

    bets = {'id':[], 'date_open':[], 'type':[], 'risk':[], 'win':[], 'won': []}

    bet_line_keys = {'team_odds_result': ['<span>[', '</span>']}

    bet_lines = {'id': [], 'line': [], 'team_odds_result': [], 'bet_date': []}

    bet_search = "Bet Ticket: #"

    i = source.find(bet_search)
    while i != -1:
        id = source[i+len(bet_search):i+21]
        if id == '\n       ':
            break
        bets['id'].append(id)
        bet = source[i:source.find('<div class="history-row">', i+100)]
        start = 0

        for key, value in bet_keys.items():
            start = bet.find(value[0], 0) + len(value[0])
            end = bet.find(value[1], start)
            bets[key].append(bet[start:end].strip())
        start = 0
        total_lines = 0
        while start > -1:
            for key, value in bet_line_keys.items():
                start = bet.find(value[0], start)
                end = bet.find(value[1], start)
                if start == -1 or end == -1:
                    line = False
                    pass
                else:
                    bet_lines[key].append(bet[start+len(value[0]):end])
                    line = True
                start = end
            if line:
                total_lines += 1
                bet_lines['bet_date'].append(bets['date_open'][len(bets['date_open'])-1])

        bet_lines['id'].extend([id] * (total_lines))
        bet_lines['line'].extend(np.arange(1,total_lines+1))

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
        wager = 'win'
    return [points, odds, wager]

def find_game_id(x, games):
    team = x.team_id
    date = x.game_start
    try:
        id = games[(games['game_start'] >= date) & ((games['home_team'] == team)|(games['away_team'] == team))].sort_values(by=['game_start']).iloc[0].id
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

def intnull(x):
    try:
        out = str(int(x))
    except:
        out = '999'
    return out

def insert_bets(cur, bets):
    values = []
    errors = 0

    bets['date_open'] = pd.to_datetime(bets['date_open']).dt.date
    bets['type'] = bets['type'].apply(lambda x: bet_type_parse(x))

    for index, line in bets.iterrows():
        try:
            values.append("'" + str(line.id) +"', '" + str(line.date_open) + "', '" + str(line.date_open) + "', '" + str(line.risk) + "', '" + str(line.type) + "', '" + str(line.win) + "'")
        except:
            errors += 1
    print(f"Couldn't update {errors} bet(s)")

    for value in values:
        sql = (f"""
        INSERT INTO open_bets AS o (id, date_open, date_close, risk, type, win)
        VALUES ({value})
        ON CONFLICT (id) DO UPDATE
        SET id = o.id
            ,date_open = EXCLUDED.date_open
            ,date_close = EXCLUDED.date_close
            ,risk = EXCLUDED.risk
            ,type = EXCLUDED.type
            ,win = EXCLUDED.win;
        """)
        cur.execute(sql)

    return "Success!"

def team_and_odds_split(x):
    spaces = x.split()
    if spaces[1] == 'TOTAL':
        team = None
        odds_wager = spaces[2]
    else:
        team = ' '.join(spaces[1:-1]).lower().title()
        odds_wager = spaces[-1]
    return [team, odds_wager]

def clean_bet_lines(engine, bet_lines):
    bet_lines.game_start = bet_lines.game_start.apply(lambda x: '2019 '+x)
    bet_lines.game_start = pd.to_datetime(bet_lines.game_start).dt.date
    # bet_lines.game_start = bet_lines.game_start.apply(lambda x: x.strftime('%Y-%M-%D'))
    bet_lines['team'] = bet_lines.team_and_odds.apply(lambda x: team_and_odds_split(x)[0])
    bet_lines['odds_wager'] = bet_lines.team_and_odds.apply(lambda x: team_and_odds_split(x)[1])

    bet_lines

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

    bet_lines
    bet_lines['game_id'] = bet_lines.game_id.apply(lambda x: intnull(x))

    return bet_lines

def insert_open_bet_lines(cur, engine, bet_lines):
    values = []
    errors = 0

    bet_lines = clean_bet_lines(engine, bet_lines)

    for index, line in bet_lines.iterrows():
       try:
            values.append("'" + str(line.bet_id) +"', '" + str(line.line_number) + "', '" + str(line.wager_type) + "', '" + str(line.odds) + "', '" + str(line.game_id) + "', '" + str(float(line.points.replace('½','.5'))) + "', '" + str(line.team_id) + "', '" + str(line.team_long) + "'")
       except:
           errors += 1

    print(f"Couldn't update {errors} bet line(s)")

    for value in values:
        sql = (f"""
        INSERT INTO bet_lines (bet_id, line_number, wager_type, odds, game_id, points, team, team_long)
        VALUES ({value})
        ON CONFLICT (bet_id, line_number) DO UPDATE
        SET bet_id = bet_lines.bet_id
            ,line_number = EXCLUDED.line_number
            ,wager_type = EXCLUDED.wager_type
            ,odds = EXCLUDED.odds
            ,game_id = EXCLUDED.game_id
            ,points = EXCLUDED.points
            ,team = EXCLUDED.team
            ,team_long = EXCLUDED.team_long
        """)
        cur.execute(sql)

    return "Success!"

def winloss(x):
    out = False
    if x == "WIN":
        out = True
    return out

def update_closed_bets(cur, engine, bets):

    bets['won'] = bets['won'].apply(lambda x: winloss(x))

    values = []
    errors = 0
    bets['date_open'] = pd.to_datetime(bets['date_open']).dt.date
    bets['type'] = bets['type'].apply(lambda x: bet_type_parse(x))

    for index, line in bets.iterrows():
        try:
            values.append("'" + str(line.id) +"', '" + str(line.date_open) + "', '" + str(line.date_open) + "', '" + str(line.risk) + "', '" + str(line.type) + "', '" + str(line.win) + "', '" + str(line.won) + "'")
        except:
            errors += 1
    print(f"Couldn't update {errors} bet(s)")

    for value in values:
        sql = (f"""
        INSERT INTO open_bets AS o (id, date_open, date_close, risk, type, win, won)
        VALUES ({value})
        ON CONFLICT (id) DO UPDATE
        SET id = o.id
            ,date_open = o.date_open
            ,date_close = o.date_close
            ,risk = o.risk
            ,type = o.type
            ,win = o.win
            ,won = EXCLUDED.won;
        """)
        try:
            cur.execute(sql)
        except:
            print(value)

def find_game_with_team_date(x):
    id = int(x.bet_id)

    return out

def clean_closed_bet_lines(engine, bet_lines):

    bet_lines['team'] = bet_lines.team_odds_result.apply(lambda x: ' '.join(x.split()[1:-2]).lower().title())
    bet_lines['won'] = bet_lines.team_odds_result.apply(lambda x: winloss(x.split()[-1]))
    bet_lines['odds_wager'] = bet_lines.team_odds_result.apply(lambda x: x.split()[-2])
    bet_lines['game_start'] = pd.to_datetime(bet_lines.bet_date).dt.date


    teams = pd.read_sql("SELECT * FROM teams", engine)
    teams = teams.rename(columns = {'id':'team_id'})
    bet_lines = pd.merge(bet_lines, teams[['team_id','long_name']], how="left", left_on="team", right_on="long_name")[['game_start', 'id', 'line', 'team', 'team_id', 'odds_wager', 'won']]

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
    bet_lines['game_id'] = bet_lines.game_id.apply(lambda x: intnull(x))
    bet_lines

    return bet_lines

def update_closed_bet_lines(cur, engine, bet_lines):
    values = []
    inserts = []
    errors = 0

    bet_lines = clean_closed_bet_lines(engine, bet_lines)
    bet_lines[bet_lines['bet_id'] =='72831806']

    values
    for index, line in bet_lines.iterrows():
       try:
            values.append([str(line.bet_id), str(line.line_number), str(line.wager_type), str(line.odds), str(line.game_id), str(float(line.points.replace('½', '.5'))), str(line.team_id), str(line.team_long), str(line.won)])
       except:
           print(line)
           errors += 1

    print(f"Couldn't parse {errors} bet line(s)")

    for value in values:
        errors =0
        exists = pd.read_sql(f"SELECT COUNT(bet_id) as count FROM bet_lines WHERE bet_id = {value[0]} and team = '{value[6]}'", engine).iloc[0]['count']
        if exists >0:
            sql = f"""
            UPDATE bet_lines
            SET won = {value[8]}
            WHERE bet_id = {int(value[0])} and team = '{value[6]}'
            """
        else:
            try:
                line = pd.read_sql(f"SELECT COALESCE(MAX(line_number), 0) AS line FROM bet_lines WHERE bet_id ={int(value[0])}", engine).iloc[0].line + 1
            except:
                line = 1
            sql = (f"""
            INSERT INTO bet_lines AS b (bet_id, line_number, wager_type, odds, game_id, points, team, team_long, won)
            VALUES ({int(value[0])}, {line}, '{value[2]}', '{value[3]}', '{value[4]}', '{value[5]}', '{value[6]}', '{value[7]}', '{value[8]}')
            """)
        try:
            cur.execute(sql)
        except:
            errors += 1

    print(f"Couldn't update {errors} bet line(s)")

    return "Success!"

def get_bets():
    engine, cur = setup()
    browser = setup_selenium()
    cont = 'y'
    while cont == 'y':
        input("Press enter when on the open bets page you wish to import:")
        source = browser.page_source
        bets, bet_lines = scrape_open_bets_page(source)
        insert_bets(cur, bets)

        insert_open_bet_lines(cur, engine, bet_lines)
        print("Finished page; navigate to next page.")
        cont = input("Scrape another page? (y/n)\n")

    print("Ok! I've got your open bets.")
    cont = input("Would you like to import closed bets?\n")
    while cont == 'y':
        # input("Press enter when on the closed bets page you wish to import:")
        source = browser.page_source
        with open('source.html', 'wb') as f:
            f.write(source.encode('utf8'))
        with open('source.html', 'r', encoding='utf8') as f:
            source = f.read()
        bets, bet_lines = scrape_closed_bets_page(source)
        update_closed_bets(cur, engine, bets)

        update_closed_bet_lines(cur, engine, bet_lines)
        print("Finished page; navigate to next page.")
        cont = input("Scrape another page? (y/n)\n")




    cur.close()
    engine.dispose()
    print("Goodbye!")

    return None


if __name__ == "__main__":
    get_bets()

#END


#TEST



#

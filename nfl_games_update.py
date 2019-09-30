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

def populate_teams_table(engine):
    teams = nflgame.teams
    for team in teams:
        if team[0] == "JAC":
            team[0], team[4] = team[4], team[0]
        elif len(team) == 4:
            team.append(None)
        team.insert(1, 'nfl')

    columns = ["id", 'league', 'location', 'short_name', 'long_name', 'alt_abr']
    teams_df = pd.DataFrame(teams, columns = columns)
    teams_df.to_sql(name = "teams", con = engine, if_exists = "append", index = False)

    return "Success!"

def create_games_rows(cur, year, week):
    games = nflgame.games(year, week=week)
    values = []

    for game in games:
        date = str(game.schedule['year']) + '-' + str(game.schedule['month']).zfill(2) + '-' + str(game.schedule['day']) + ' ' + pd.to_datetime(game.schedule['time']).strftime('%H:%M:00')
        try:
            values.append("'" + str(game.gamekey) +"', '" + game.away + "', '" + game.home + "', '" + str(date)+ "'")
        except:
            print(f"Couldn't create game number: {game.gamekey} for {game.away} @ {game.home}")

    for value in values:
        sql = (f"""
        INSERT INTO games (id, away_team, home_team, game_start)
        VALUES ({value})
        ON CONFLICT DO NOTHING;
        """)
        cur.execute(sql)

    return "Success!"

def update_games(cur, year, week):
    games = nflgame.games(year, week=week)
    values = []
    errors = 0

    for game in games:
        date = str(game.schedule['year']) + '-' + str(game.schedule['month']).zfill(2) + '-' + str(game.schedule['day']) + ' ' + pd.to_datetime(game.schedule['time']).strftime('%H:%M:00')
        try:
            values.append("'" + str(game.gamekey) +"', '" + str(game.away) +"', '" + str(game.home) +"', '" + str(date) + "', '" + str(game.score_away) + "', '" + str(game.score_home) + "', '" + str(game.togo) + "', '" + str(game.game_over()) + "'")
        except:
            errors += 1

    print(f"Couldn't update {errors} game(s)")

    for value in values:
        sql = (f"""
        INSERT INTO games (id, away_team, home_team, game_start, away_score, home_score, time_remaining, game_over)
        VALUES ({value})
        ON CONFLICT (id) DO UPDATE SET
            home_team = games.home_team
            ,away_team = games.away_team
            ,game_start = games.game_start
            ,away_score = EXCLUDED.away_score
            ,home_score = EXCLUDED.home_score
            ,time_remaining = EXCLUDED.time_remaining
            ,game_over = EXCLUDED.game_over;
        """)
        cur.execute(sql)

    return "Success!"

def run_the_script():
    engine, cur = setup()
    year = int(input("What year is it?\n"))
    week = int(input("What NFL week is it?\n"))

    print("\nSetting up connections...\n\n")
    create_games_rows(cur, year, week)
    iter = 0
    start = time.time()
    elapsed = 0
    while elapsed < 240:
        for i in range(20):       # print is Ok, and comma is needed.
            time.sleep(.25)
            if i % 4 == 0:
                p = "/   "
            elif i % 4 == 1:
                p = "|   "
            elif i % 4 == 2:
                p = "\\   "
            else:
                p = "-   "
            print(p, end = '\r')
        print("\n")
        update_games(cur, year, week)
        iter += 1
        end = time.time()
        elapsed = (end - start)/60

    engine.dispose()
    cur.close()
    print("Goodbye!")
    return None

run_the_script()
# populate_teams_table(engine)

# END




# TEST

year = 2019
week = 4

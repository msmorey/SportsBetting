import pandas as pd
import psycopg2 as ppg
import sqlalchemy
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
import nflgame
from nflgame import live
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

def create_games_rows(cur):
    year, week = live.current_year_and_week()
    games = nflgame.games(year, week=week)
    values = []

    for game in games:
        date = str(game.schedule['year']) + '-' + str(game.schedule['month']).zfill(2) + '-' + str(game.schedule['day']) + ' ' + (pd.to_datetime(game.schedule['time'] + game.schedule['meridiem']) - pd.Timedelta(hours = 1)).strftime('%H:%M:00')
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

    return [year, week]

def update_games(cur, year, week):
    games = nflgame.games(year, week=week)
    values = []
    errors = 0
    for game in games:
        date = str(game.schedule['year']) + '-' + str(game.schedule['month']).zfill(2) + '-' + str(game.schedule['day']) + ' ' + (pd.to_datetime(game.schedule['time'] + game.schedule['meridiem']) - pd.Timedelta(hours = 1)).strftime('%H:%M:00')
        if game.game_over() == True:
            quarter = 4
        else:
            try:
                quarter = int(game.time.qtr)
            except:
                quarter = 0
        try:
            minutes, seconds = [int(x) for x in game.time.clock.split(':')]
            time_remaining = (minutes *60) + seconds + ((15*(4-quarter)) * 60)
        except:
            time_remaining = game.time.clock
        try:
            yardline = str(game.data['yl'])
        except:
            yardline = ''
        try:
            posteam = str(game.data['posteam'])
        except:
            posteam = ''

        try:
            (values.append("'" + str(game.gamekey) +"', '" + str(game.away) +"', '" + \
            str(game.home) +"', '" + str(date) + "', '" + str(game.score_away) \
            + "', '" + str(game.score_home) + "', '" + str(time_remaining) + "', '" + \
            str(game.game_over())+ "', '" + str(quarter) + "','" + posteam\
             + "','" + yardline +"'"))
        except:
            errors += 1
    if errors == 0:
        print("All games updated!")
    else:
        print(f"Couldn't update {errors} game(s)")

    for value in values:
        sql = (f"""
        INSERT INTO games   (id, away_team, home_team, game_start, away_score, home_score,
                            time_remaining, game_over, quarter, posteam, yardline)
        VALUES ({value})
        ON CONFLICT (id) DO UPDATE SET
            home_team = games.home_team
            ,away_team = games.away_team
            ,game_start = EXCLUDED.game_start
            ,away_score = EXCLUDED.away_score
            ,home_score = EXCLUDED.home_score
            ,time_remaining = EXCLUDED.time_remaining
            ,game_over = EXCLUDED.game_over
            ,quarter = EXCLUDED.quarter
            ,posteam = EXCLUDED.posteam
            ,yardline = EXCLUDED.yardline;
        """)
        cur.execute(sql)
    cur.execute("UPDATE games SET posteam = NULL WHERE posteam = ''; UPDATE games SET yardline = NULL WHERE yardline = ''")

    return None


if __name__ == '__main__':
    print('Not a standalone script')

    # populate_teams_table(engine)
    engine, cur = setup()
    update_games(cur, 2019, 6)


# TEST

import sys, select, os

os.chdir("/Users/acttech/Documents/PersonalProjects/SportsBetting")

import myBookieBetsScrape as myBookie
import dashboard
import nfl_games_update as nfl
import pandas as pd
import time
from tabulate import tabulate

def loop_scores(cur, engine, year, week):
    start = time.time()
    elapsed = 0
    old_dfs = []

    while elapsed < 240:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Press 'enter' to end script or update bets\n\n")
        while len(old_dfs) >5:
            del old_dfs[0]
        if len(old_dfs) > 0:
            old_df = old_dfs[0]
        else:
            old_df = None
        df = dashboard.retrieve_dash(engine, old_df)
        old_dfs.append(df[['delta', 'team', 'points']].copy())
        df = df.rename(columns = {'delta': 'n_delta', 'f_delta':'delta', 'time_remaining':'time'})
        try:
            print(tabulate(df[['score', 'time', 'yardline', 'team', 'points', 'delta', 'closeness']]\
            .sort_values(by = ['closeness'], ascending = False), headers = 'keys', showindex = False, tablefmt = 'fancy_grid'))
        except KeyError:
            print('No games found!')
        for i in range(20):
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
        # nfl.update_games(cur, year, week)
        time.sleep(.5)
        if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
            line = input()
            break

def meat_and_potatoes():
    engine, cur = nfl.setup()
    year = int(input("What year is it?\n"))
    week = int(input("What NFL week is it?\n"))

    print("\nSetting up games...\n\n")
    nfl.create_games_rows(cur, year, week)

    print('Gathering bets:\n')
    browser = myBookie.setup_selenium()
    myBookie.get_bets(engine, cur, browser)
    print('Updating scores: press control c to update bets\n')
    cont = 'y'
    while cont == 'y':
        loop_scores(cur, engine, year, week)
        bets = input("Would you like to update your bets? y/n\n")
        if bets == 'y':
            myBookie.get_bets(engine, cur, browser)
        cont = input("Would you like to keep updating scores? y/n\n")


    cur.close()
    engine.dispose()
    print("Thanks for using Sean's bet tracker!")

if __name__ == '__main__':
    meat_and_potatoes()

# END
# TEST
tabulate(df[['score', 'time_remaining', 'team', 'points', 'delta', 'closeness']]\
.sort_values(by = ['closeness'], ascending = False), headers = 'keys', showindex = False, tablefmt = 'grid').split('\n')









# END TEST

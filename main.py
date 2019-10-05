import os

os.chdir("/Users/acttech/Documents/PersonalProjects/SportsBetting")

import myBookieBetsScrape as myBookie
import nfl_games_update as nfl

if __name__ == '__main__':
    print("Grabbing current games")
    nfl.update_games()
    print('Gathering bets:\n')
    myBookie.get_bets()
    print('Updating scores\n')
    nfl.update_games()

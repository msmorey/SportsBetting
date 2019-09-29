import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from pyquery import PyQuery


option = webdriver.ChromeOptions()
option.add_argument(" — incognito")

browser = webdriver.Chrome(options=option)

url = "https://michaelsean@me.com:13214ortho@engine.mybookie.ag/sports/open-bets"

browser.get(url)

text = browser.page_source

bets = []
search = {'bet-placed"> ':'</span>', 'bet_type_name text-uppercase"> ':'</span>', '<p>[': '</p', '<p>[': '</p'}
lines = text.split("\n")
index = 0
j = 0
for line in lines:
    i = line.find("Bet Ticket: #")
    if i == -1:
        pass
    else:
        bets.append([])
        id = line[i+13:i+21]
        bets[index].append(id)
        bet = ''.join(elem for elem in lines[j:j+56])
        start = 0
        for key, value in search.items():
            start = bet.find(key, start) + len(key)
            end = bet.find(value, start)
            bets[index].append(bet[start:end])
        index += 1
    j+=1



bets
t = "test"

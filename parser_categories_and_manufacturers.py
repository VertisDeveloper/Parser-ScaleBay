import json
import random
import time
import csv

import requests

from apscheduler.schedulers.background import BackgroundScheduler
from categories_dict import CATEGORIES
from list_all_manufacturers import ALL_MANUFACTURERS
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from requests import Session


class Parser:
    user_agent_lst = ['Mozilla/5.0 (Windows NT 10.0; rv:122.0) Gecko/20100101 Firefox/122.0',
                      'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:122.0) Gecko/20100101 Firefox/122.0',
                      'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
                      'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
                      'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:121.0) Gecko/20100101 Firefox/121.0']
    headers = {
        'User-Agent': '',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'}

    def __init__(self):
        self.user_agent: str = random.choice(self.user_agent_lst)
        self.headers['User-Agent'] = self.user_agent
        self.config: dict = self.read_traders_links
        self.all_manufacturers = ALL_MANUFACTURERS
        self.lst_matches = self.read_lst_matches

    @property
    def read_lst_matches(self):
        with open('соответствия.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            matches_dct = {i[0].strip(): i[1].strip() for i in reader}
        return matches_dct

    @property
    def read_traders_links(self) -> dict:
        setting = {"parsing_all_site": "False",
                   'parsing_time': {"everyday": {"hour": 19, "minute": 0},
                                    "interval": {"hour": 2}},
                   'links': {},
                   'all_manufacturers': {}}
        with open('links_sellers.txt', 'r', encoding='utf-8') as file:
            for link in file:
                setting.get('links').setdefault(link.strip(), {'time_trade': 24,
                                                               'markup': 0,
                                                               'price_delivery': 0,
                                                               'condition': ['Новый', 'Б/у'],
                                                               'categories': {},
                                                               'unnecessary_manufacturers': {}})
        return setting

    @property
    def new_session(self) -> Session:
        session = requests.Session()
        session.cookies.update(self.cookies)
        session.headers.update(self.headers)
        return session

    @property
    def new_cookies(self) -> dict:
        with sync_playwright() as playwright:
            browser = playwright.firefox.launch(headless=True)
            context = browser.new_context(viewport={"width": 1920, "height": 1080},
                                          user_agent=self.user_agent)
            page = context.new_page()
            for link in self.config.get('links').keys():
                page.goto(url=link, timeout=30000)
                time.sleep(1)
                self.parsing_manufacturers(page)
                time.sleep(0.5)
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies('https://scalebay.ru')}
            browser.close()
        return cookies

    def parsing_manufacturers(self, page):
        if page.query_selector('div.lsf__show-all'):
            page.query_selector('div.lsf__show-all').click()
        time.sleep(1)
        link_manufacturers = set(
            i.text_content().strip() for i in page.query_selector_all('div.lsf--manufacturers a.lsf__option__link'))
        for manufacturer in link_manufacturers:
            if self.lst_matches.get(manufacturer.strip(), False):
                self.config.get('all_manufacturers').setdefault(manufacturer.strip(), self.lst_matches.get(manufacturer.strip()))
            elif manufacturer.strip() in self.all_manufacturers:
                self.config.get('all_manufacturers').setdefault(manufacturer.strip(), manufacturer.strip())
            else:
                self.config.get('all_manufacturers').setdefault(manufacturer.strip(), 'Другой')

    def parsing_categories(self):
        for link in self.config.get('links').keys():
            response = self.session.get(url=link, timeout=(5, 10))
            soup = BeautifulSoup(response.text, 'lxml')
            if soup:
                next_page = True
                n = 1
                while next_page:
                    if n != 1:
                        response = self.session.get(url=f'{link}&page={n}', timeout=(5, 10))
                        soup = BeautifulSoup(response.text, 'lxml')
                    all_cards = soup.select('div.lbr__listings>div')
                    for card in all_cards:
                        links_categories = [i.get('href').strip() for i in card.select('span.list__attribute>a') if i]
                        [self.config.get('links').get(link).get('categories').setdefault(CATEGORIES.get(i), i) for i in
                         links_categories if links_categories]
                    n += 1
                    next_page = soup.select_one('a.pager__page--next')

    def parsing_start(self):
        print('Парсинг категорий и производителей начался...')
        self.cookies: dict = self.new_cookies
        self.session: Session = self.new_session
        self.parsing_categories()
        self.write_config()
        print('Парсинг категорий и производителей завершился.')

    def write_config(self):
        with open(f'config.json', 'w', encoding='utf-8') as file:
            json.dump(self.config, file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    pars = Parser()
    scheduler = BackgroundScheduler()
    scheduler.add_job(pars.parsing_start, 'cron', hour=22, minute=9)   # Здесь задаем время парсинга
    scheduler.start()
    try:
        while True:
            time.sleep(1)
    finally:
        scheduler.shutdown(wait=False)

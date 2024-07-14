import asyncio
import csv
import gc
import json
import random
import time
import re

import aiohttp
import openpyxl
import requests
from datetime import datetime

from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup
from requests import Session
from loguru import logger
from tqdm import tqdm

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

logger.add('log_app.log', format='{time} {level} {message}', level='ERROR')

class ParserScaleBaySync:
    def __init__(self):
        self.pattern: str = r'(\d+)д.*?(\d+)ч'
        self.setting_parsing: dict = self.read_config
        self.file_name: str = f'ScaleBay_sellers_{datetime.now().date()}'
        self.links_manufacturers: dict = self.setting_parsing.get('all_manufacturers')

        self.user_agent: str = random.choice(user_agent_lst)
        headers['User-Agent'] = self.user_agent

        with open(f'{self.file_name}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(('Название', 'Ссылки на картинки', 'Номер лота', 'Блиц цена',
                             'Цена с доставкой и наценкой', 'Марка', 'Производитель', 'Масштаб',
                             'Материал', 'Артикул', 'Состояние', 'Описание', 'Колличество',
                             'Ссылка на продавца', 'Ник продавца'))


    @property
    def new_session(self) -> Session:
        session = requests.Session()
        session.cookies.update(self.cookies)
        session.headers.update(headers)
        return session

    @property
    def new_cookies(self) -> dict:
        with sync_playwright() as playwright:
            browser = playwright.firefox.launch(headless=True)
            context = browser.new_context(viewport={"width": 1920, "height": 1080},
                                          user_agent=self.user_agent)
            page = context.new_page()
            page.goto(url='https://scalebay.ru', timeout=60000)
            time.sleep(1)
            cookies = {cookie['name']: cookie['value'] for cookie in context.cookies('https://scalebay.ru')}
            time.sleep(0.5)
            browser.close()
            time.sleep(0.5)
        return cookies

    @property
    def read_config(self) -> dict:
        with open('final_config.json', 'r', encoding='utf-8') as file:
            json_config = json.load(file)
        return json_config

    @logger.catch()
    def count_trade_time(self, card: BeautifulSoup) -> bool:
        time_card = card.select_one('div.ends').text
        match = re.search(self.pattern, time_card)
        result = 0
        if match:
            digit_before_d = match.group(1)
            if digit_before_d:
                result += int(digit_before_d) * 24
            digit_before_ch = match.group(2)
            if digit_before_ch:
                result += int(digit_before_ch)
        if result >= self.trade_timer:
            return True
        else:
            return False

    @logger.catch()
    def find_link_categories(self, card: BeautifulSoup) -> bool:
        link_categories = [i.get('href') for i in card.select('span.list__attribute>a') if i]
        for i in link_categories:
            if i in self.categories:
                return True

        return False

    @logger.catch()
    def find_link_manufacturer(self, card: BeautifulSoup) -> bool:
        manufacturer = [i.text.strip().replace('\xa0', '') for i in card.select('a.list__attribute') if i]
        for i in manufacturer:
            if i not in self.manufacturer.keys():
                return True

        return False

    @logger.catch()
    def parsing_pages(self):
        for link in self.user_link:
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
                        hours = self.count_trade_time(card)
                        categories = self.find_link_categories(card)
                        manufacturer = self.find_link_manufacturer(card)
                        if all([hours, categories, manufacturer]):
                            link = card.select_one('div.list__description-top>h3>a').get('href')
                            self.all_links.add(link)
                    n += 1
                    next_page = soup.select_one('a.pager__page--next')

    @logger.catch()
    def search_product_characteristics(self, soup: BeautifulSoup) -> str:
        characteristics: dict = {"Марка": "-", "Производитель": "-", "Масштаб": "-",
                                 "Материал": "-", "Артикул": "-", "Состояние": '-'}
        all_characteristics: list = soup.select('dl.product-characteristics>div>div')
        for i in all_characteristics:
            key = i.select_one('dt').text.strip().replace('\xa0', '').replace('\n', '').replace('\t', '')
            if key == 'Производитель':
                value = self.links_manufacturers.get(i.select_one('dd').text.strip().replace('\n', '').
                                 replace('\t', ''), None)
            else:
                value = i.select_one('dd').text.strip().replace('#', '').replace("’", '')
            characteristics[key] = value
        return characteristics

    @logger.catch()
    def search_all_characteristics(self, soup: BeautifulSoup) -> bool:
        price = soup.select_one('span#product-price>[itemprop="price"]')
        if not price:
            return False
        self.price: int = int(price.text)
        self.new_price: float = round((self.price + self.price_delivery) * (1 + (self.markup / 100)), 1)
        name = soup.select_one('h1.title>span:nth-child(2)')
        self.name: str = name.text.strip().replace('#', '').replace("’", '') if name else '-'
        image_links = soup.select('a.thumbnail')
        self.image_links: str = ','.join(
            [i.get('href') for n, i in enumerate(image_links, 1) if n != 1]) if image_links else '-'
        identifier = soup.select_one('span[itemprop="identifier"]')
        self.identifier: str = identifier.text.strip() if identifier else '-'
        self.product_characteristics: dict = self.search_product_characteristics(soup)
        description = soup.select_one('div#desc')
        self.description: str = (description.text.strip().replace('\xa0', ' ').replace('\n', ' ').
                                 replace('\t', ' ').replace('#', '').replace("’", '').
                                 replace(";", ':')) \
            if description else '-'
        quantity_available = soup.select_one('#quantity-available')
        self.quantity_available: str or int = quantity_available.text.strip().replace('\n', '').replace('\t', '') \
            if quantity_available and quantity_available.text.strip().isdigit() else 1
        return True

    @logger.catch()
    def write_csv_file(self, link: str):
        with open(f'{self.file_name}.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow((self.name, self.image_links, self.identifier, self.price, self.new_price,
                             *self.product_characteristics.values(), self.description, self.quantity_available,
                             f'https://scalebay.ru/display-feedback/-/{link.split("/")[-1]}', link.split("/")[-2]))

    @logger.catch()
    def parsing_start(self):
        print(f'{datetime.now()} - Парсинг продавцов начался...')
        self.cookies: dict = self.new_cookies
        self.session: Session = self.new_session
        progress = 0
        for link, setting in self.setting_parsing.get('links').items():
            self.all_links: set = set()
            self.user_link: list = list(
                map(lambda i: f"{link}?cnd=new" if i == 'Новый' else f"{link}?cnd=old", setting.get('condition')))
            self.trade_timer: int = int(setting.get('time_trade'))
            self.markup: int = int(setting.get('markup'))
            self.price_delivery: int = int(setting.get('price_delivery'))
            self.categories: list = setting.get('categories')
            self.manufacturer: dict = setting.get('unnecessary_manufacturers')
            self.parsing_pages()
            for url in tqdm(self.all_links, desc=f'Собираю товары продавца - {link}'):
                for _ in range(3):
                    try:
                        response = self.session.get(url=url, timeout=(5, 5))
                        break
                    except:
                        response = False

                if response:
                    soup = BeautifulSoup(response.text, 'lxml')
                    progress += 1
                    if self.search_all_characteristics(soup):
                        self.write_csv_file(link)
        gc.collect()
        print(f'{datetime.now()} - Парсинг продавцов завершен.')


class ParserScaleBayAsync:
    def __init__(self):
        self.pattern: str = r'(\d+)д.*?(\d+)ч'
        self.file_name: str = f'ScaleBay_all_goods_{datetime.now().date()}'

        self.user_agent: str = random.choice(user_agent_lst)
        headers['User-Agent'] = self.user_agent
        self.all_links = set()

        with open(f'{self.file_name}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(('Название', 'Ссылки на картинки', 'Номер лота', 'Блиц цена', 'Марка',
                             'Производитель', 'Масштаб', 'Материал', 'Артикул', 'Состояние', 'Описание',
                             'Колличество', 'Ссылка на продавца', 'Ник продавца'))

    @property
    async def new_session(self) -> Session:
        session = requests.Session()
        session.cookies.update(self.cookies)
        session.headers.update(headers)
        return session

    @property
    async def new_cookies(self) -> dict:
        async with async_playwright() as playwright:
            browser = await playwright.firefox.launch(headless=True)
            context = await browser.new_context(viewport={"width": 1920, "height": 1080},
                                                user_agent=self.user_agent)
            page = await context.new_page()
            await page.goto(url='https://scalebay.ru', timeout=60000)
            time.sleep(1)
            cook = await context.cookies('https://scalebay.ru')
            cookies = {cookie['name']: cookie['value'] for cookie in cook}
            time.sleep(0.5)
            await browser.close()
            time.sleep(0.5)
        return cookies

    @logger.catch()
    async def count_trade_time(self, card: BeautifulSoup) -> bool:
        time_card = card.select_one('div.ends').text
        match = re.search(self.pattern, time_card)
        result = 0
        if match:
            digit_before_d = match.group(1)
            if digit_before_d:
                result += int(digit_before_d) * 24
            digit_before_ch = match.group(2)
            if digit_before_ch:
                result += int(digit_before_ch)
        if result >= 1:
            return True
        else:
            return False

    @logger.catch()
    async def response_get(self, url='https://scalebay.ru/browse/'):
        soup = None
        for _ in range(2):
            try:
                response = self.session.get(url=url, timeout=(5, 15))
                soup = BeautifulSoup(response.text, 'lxml')
                break
            except:
                pass

        return soup

    @logger.catch()
    async def search_link(self, soup):
        if not soup:
            return
        all_cards = soup.select('div.lbr__listings>div')
        for card in all_cards:
            hours = await self.count_trade_time(card)
            if hours:
                link = card.select_one('div.list__description-top>h3>a').get('href')
                self.all_links.add(link)

    @logger.catch()
    async def parsing_pages(self):
        soup = await self.response_get()
        if not soup:
            return
        count_page = int(soup.select_one('div.pager__page--total-count').text.strip())
        for n in tqdm(range(2, count_page + 1), desc='Собираю ссылки на все товары: '):
            await self.search_link(soup)
            soup = await self.response_get(url=f'https://scalebay.ru/browse?page={n}')

    @logger.catch()
    async def go_coroutine(self):
        semaphore = asyncio.Semaphore(5)
        progress_bar = tqdm(total=len(self.all_links), desc="Собираю все данные", unit=' link')
        timeout = aiohttp.ClientTimeout(
            total=25,
            connect=5,
            sock_connect=5,
            sock_read=15
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [self.parsing_link(url=url, progress_bar=progress_bar, session=session, semaphore=semaphore) for url in self.all_links]
            await asyncio.gather(*tasks)

        progress_bar.close()

    @logger.catch()
    async def parsing_link(self, **kwargs):
        try:
            async with kwargs.get('semaphore'):
                kwargs.get('progress_bar').update(1)
                async with kwargs.get('session').get(url=kwargs.get('url'), headers=headers, cookies=self.cookies) as response:
                    if response.status == 200:
                        content = await response.text(encoding='utf-8', errors='replace')
                        soup = BeautifulSoup(content, 'lxml')
                        if await self.search_all_characteristics(soup):
                            await self.write_xlsx_file()
        except Exception as ex:
            print(ex)

    @logger.catch()
    async def search_product_characteristics(self, soup: BeautifulSoup) -> str:
        characteristics: dict = {"Марка": "-", "Производитель": "-", "Масштаб": "-",
                                 "Материал": "-", "Артикул": "-", "Состояние": '-'}
        all_characteristics: list = soup.select('dl.product-characteristics>div>div')
        if all_characteristics:
            for i in all_characteristics:
                key = i.select_one('dt').text.strip().replace('\xa0', '').replace('\n', '').replace('\t', '')
                value = i.select_one('dd').text.strip().replace('#', '').replace("’", '')
                characteristics[key] = value
        return characteristics

    @logger.catch()
    async def search_all_characteristics(self, soup: BeautifulSoup) -> bool:
        price = soup.select_one('span#product-price>[itemprop="price"]')
        if not price:
            return False
        self.price: int = int(price.text)
        name: BeautifulSoup = soup.select_one('h1.title>span:nth-child(2)')
        self.name: str = name.text.strip().replace('#', '').replace("’", '') if name else '-'
        image_links: list[BeautifulSoup] = soup.select('a.thumbnail')
        self.image_links: str = ','.join(
            [i.get('href') for n, i in enumerate(image_links, 1) if n != 1]) if image_links else '-'
        identifier: BeautifulSoup = soup.select_one('span[itemprop="identifier"]')
        self.identifier: str = identifier.text.strip() if identifier else '-'
        self.product_characteristics: dict = await self.search_product_characteristics(soup)
        description: str = soup.select_one('div#desc').text.strip() if soup.select_one('div#desc') else '-'
        self.description: str = ''.join([description.replace(i, ' ') for i in {'\xa0', '\n', '\t', '#', '’', ';'}])
        quantity_available: BeautifulSoup = soup.select_one('#quantity-available')
        self.quantity_available: int = quantity_available.text.strip().replace('\n', '').replace('\t', '') \
            if quantity_available and quantity_available.text.strip().isdigit() else 1
        seller_link: BeautifulSoup = soup.select_one('span.seller-details>a')
        self.seller_link: str = seller_link.get('href').strip() if seller_link else '-'
        seller_name: BeautifulSoup = soup.select_one('span.seller-details>a')
        self.seller_name: str = seller_name.text.strip() if seller_name else '-'
        return True

    @logger.catch()
    async def write_xlsx_file(self):
        with open(f'{self.file_name}.csv', 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow((self.name, self.image_links, self.identifier, self.price, *self.product_characteristics.values(),
                             self.description, self.quantity_available, self.seller_link, self.seller_name))

    @logger.catch()
    async def parsing_start(self):
        print(f'{datetime.now()} - Парсинг всего сайта начался...')
        self.cookies: dict = await self.new_cookies
        self.session: Session = await self.new_session
        await self.parsing_pages()
        await self.go_coroutine()

        with open(f'{self.file_name}.csv', 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            data = [i for i in reader]

        workbook = openpyxl.Workbook()
        sheet = workbook.active
        for i in data:
            sheet.append(i)
        workbook.save(f'{self.file_name}.xlsx')
        workbook.close()
        gc.collect()
        print(f'{datetime.now()} - Парсинг всего сайта завершен.')


@logger.catch()
def go_parser_sync(json_config):
    parser = ParserScaleBaySync()
    scheduler = BackgroundScheduler()
    if json_config.get('parsing_time').get('everyday', False):
        hour = json_config.get('parsing_time').get('everyday').get('hour')
        minute = json_config.get('parsing_time').get('everyday').get('minute')
        scheduler.add_job(parser.parsing_start, 'cron', hour=hour, minute=minute)
        print(f'Парсинг будет запускаться ежедневно в {hour}:{f"0{minute}" if minute < 10 else minute}')

    elif json_config.get('parsing_time').get('interval', False):
        hour = json_config.get('parsing_time').get('interval').get('hour')
        scheduler.add_job(parser.parsing_start, 'interval', hours=hour)
        print(f'Парсинг будет запускаться каждые {hour * 60} мин.')

    scheduler.start()
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("Программа завершена пользователем.")
    finally:
        scheduler.shutdown(wait=False)

@logger.catch()
async def go_parser_async(json_config):
    parser = ParserScaleBayAsync()
    scheduler = AsyncIOScheduler()
    if json_config.get('parsing_time').get('everyday', False):
        hour = json_config.get('parsing_time').get('everyday').get('hour')
        minute = json_config.get('parsing_time').get('everyday').get('minute')
        scheduler.add_job(parser.parsing_start, 'cron', hour=hour, minute=minute)
        print(f'Парсинг будет запускаться ежедневно в {hour}:{f"0{minute}" if minute < 10 else minute}')

    elif json_config.get('parsing_time').get('interval', False):
        hour = json_config.get('parsing_time').get('interval').get('hour')
        scheduler.add_job(parser.parsing_start, 'interval', hours=hour)
        print(f'Парсинг будет запускаться каждые {hour * 60} мин.')

    scheduler.start()
    await asyncio.Event().wait()

@logger.catch()
def read_config():
    with open('final_config.json', 'r', encoding='utf-8') as file:
        json_config = json.load(file)

    if json_config.get('parsing_all_site') == 'False':
        go_parser_sync(json_config)
    else:
        asyncio.run(go_parser_async(json_config))


if __name__ == "__main__":
    read_config()

import pandas as pd
from aiohttp import ClientSession, TCPConnector
from pypeln import asyncio_task as aio
from bs4 import BeautifulSoup
import json
import aiofiles
import csv
import re
import math


# Read Post Code File
post_c = pd.read_csv('../data/Post Codes.csv', names=['postcode'])

# Convert Post Code to generator
post_i = iter(post_c['postcode'].tolist())

# Create Url generator using Post Code
urls = ("https://www.realestate.com.au/rent/in-nsw+{}/list-1?includeSurrounding=false".format(i) for i in post_i)

limit = 20
list_data = []
multi_pages = []
list_data = []


async def fetch(url, session):
    print("Scraping: " + url)
    async with session.get(url) as response:
        raw_data = await response.read()
        if response.status == 200:
            await parse(url, raw_data)
        print(multi_pages)
        with open('data.csv', 'w+',encoding='UTF-8') as file:
            writer = csv.DictWriter(file, fieldnames=["Address", "details", "price", "url"], delimiter=';')
            writer.writeheader()
            for row in list_data:
                writer.writerow(row)


async def parse(url, html):
    if str(html) != "None":
        print("Parsing Html!"+url)
        soup = BeautifulSoup(html, "html.parser")

        # Getting number of pages
        max_pages=0
        pages_h = soup.find(class_='resultsInfo')
        if pages_h is None:
            max_pages=0
        else:
            pages_l = re.findall('\d+',pages_h.get_text())
            max_pages = math.ceil(int(pages_l[2])/int(pages_l[1]))
        # print((max_pages))

        sub_pages_url = [n for n in range(2, max_pages)]
        n_url = url.replace("/list-1", "/list-{}")
        nurls = list(n_url.format(i) for i in sub_pages_url)
        await append(nurls)

        #getting lising information
        listings=soup.select('.listingInfo')
        for listing in listings:
            price = listing.find(class_='priceText')
            if price is None:
                price = 0
            else:
                price=price.get_text()
            address = listing.find(class_='rui-truncate').get_text()
            url = "https://www.realestate.com.au/" + listing.find(class_='rui-truncate').find(class_='name').get('href')
            details = listing.find(class_='rui-property-features').get_text()
            new_data = {"Address": address, "details": details, "price": price,"url": url}
            list_data.append(new_data)

async def append(l):
    multi_pages.append(l)

aio.each(
    fetch,
    urls,
    workers=limit,
    on_start=lambda: ClientSession(connector=TCPConnector(limit=None, verify_ssl=False)),
    on_done=lambda _status, session: session.close(),
    run=True,
)

flat_multi_pages = [item for sublist in multi_pages for item in sublist]
multi_p_url = iter(flat_multi_pages)

aio.each(
    fetch,
    multi_p_url,
    workers=limit,
    on_start=lambda: ClientSession(connector=TCPConnector(limit=None, verify_ssl=False)),
    on_done=lambda _status, session: session.close(),
    run=True,
)
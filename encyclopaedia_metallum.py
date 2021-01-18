"""Downloads data from Encyclopaedia Metallum"""

import time
import json
import re
import sys

import requests
import pandas as pd
import bs4 as bs

from queue import PriorityQueue
from threading import Thread

ALPHABET = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'NBR', '~']

METAL_ARCHIVES_ROOT = 'www.metal-archives.com'
USER_AGENT_STR = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) '
                  + 'Gecko/20100101 Firefox/78.0')

BATCH_SIZE = 500
MAX_ATTEMPTS = 3


def _create_metallum_api_endpoint(letter, offset):
    '''returns an API endpoint for retrieving a segment of bands
    beginnging with the given letter'''

    endpoint = f'browse/ajax-letter/l/{letter}/json'
    query_string = \
        f'sEcho=1&iDisplayStart={offset}&iDisplayLength={BATCH_SIZE}'

    return f'https://{METAL_ARCHIVES_ROOT}/{endpoint}?{query_string}'


def _get_metallum_records_by_letter(letter):
    '''Returns a list containing metal bands beginning with the given letter'''

    print(f'[{letter}]: starting download')

    headers = {
        'Accept': ('text/html,'
                   + 'application/xhtml+xml,'
                   + 'application/xml;q=0.9,'
                   + 'image/webp,*/*;q=0.8'),
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Host': METAL_ARCHIVES_ROOT,
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': USER_AGENT_STR
    }

    offset = 0

    endpoint = _create_metallum_api_endpoint(letter, offset)
    band_data = requests.get(endpoint, headers=headers)
    band_json = json.loads(band_data.text)

    total_records = int(band_json['iTotalRecords'])
    records = list(band_json['aaData'])

    while offset < total_records:
        time.sleep(2)

        record_range = f'{offset}-{min(offset + BATCH_SIZE, total_records)}'
        print(f'[{letter}]: downloading {record_range} of {total_records}...')

        offset += BATCH_SIZE
        attempts = 0
        endpoint = _create_metallum_api_endpoint(letter, offset)
        band_data = requests.get(endpoint, headers=headers)

        if band_data.status_code == 200:
            band_json = json.loads(band_data.text)
        elif attempts < MAX_ATTEMPTS:
            print('ERROR - reattempting')
            attempts += 1
            offset -= BATCH_SIZE
        else:
            print(f'{band_json.status_code} error at {record_range}')

        records = records + band_json['aaData']

    print(f'[{letter}]: download complete')

    return records


def download_all_bands():
    """Get every band from Encyclopaedia Metallum"""
    bands = []

    def _download_by_letter_concurrently():
        while True:
            letter = q.get()
            bands_data = _get_metallum_records_by_letter(letter)
            for datum in bands_data:
                bands.append(datum)

            q.task_done()

    q = PriorityQueue(16 * 2)
    for _ in range(16):
        t = Thread(target=_download_by_letter_concurrently)
        t.daemon = True
        t.start()

    try:
        for letter in ALPHABET:
            q.put(letter)

        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

    bands_columns = ('band', 'country', 'genre', 'status')
    bands_df = pd.DataFrame(bands, columns=bands_columns)
    bands_df.to_csv('bands_raw.csv', index=False)


def clean_band_data():
    """Clean"""
    bands_df = pd.read_csv('bands_raw.csv')
    bands = bands_df.to_records()

    clean_records = []

    for _, link, country, genre, status in bands:
        link_match = re.match(r'^<a href=\'(.+)\'>(.+)<\/a>$', link)
        status_match = re.match(r'^<span class=".+">(.+)<\/span>$', status)
        url = link_match.group(1)
        name = link_match.group(2)
        band_status = status_match.group(1)

        band_id = url.split('/')[-1]

        clean_records.append((band_id, name, genre, country, band_status, url))

    band_columns = ('band_id', 'name', 'genre', 'country', 'status', 'url')
    clean_df = pd.DataFrame(clean_records, columns=band_columns)
    clean_df.to_csv('bands.csv', index=False)


def download_band_details():
    bands_df = pd.read_csv('bands.csv')
    urls = bands_df['url']
    album_data = []

    discography_urls = []

    for url in urls:
        band_id = url.split('/')[-1]
        band_name = url.split('/')[-2]
        endpoint = f'band/discography/id/{band_id}/tab/all'
        discography_url = f'https://www.metal-archives.com/{endpoint}'
        discography_urls.append(discography_url)

    processed_urls = []

    def _get_albumns_concurrently():
        while True:
            discography_url = q.get()
            band_webpage = None
            max_attempts = 3
            attempts = 0
            while attempts < max_attempts:
                try:
                    band_webpage = requests.get(discography_url)
                except Exception:
                    if attempts < max_attempts:
                        time.sleep(120)
                        attempts += 1
                        continue
                    print(f'\nerror at {discography_url}')
                    continue
                break

            band_soup = bs.BeautifulSoup(band_webpage.text, 'html.parser')
            band_disco_soup = band_soup.find_all('tr')[1:]

            for table_row in band_disco_soup:
                table_data = table_row.find_all('td')

                album_link = table_data[0].a
                if not album_link:
                    break

                album_url = album_link['href']
                album_name = album_link.text
                album_id = album_url.split('/')[-1]

                album_type = table_data[1].text

                year = table_data[2].text

                review = table_data[3].text.strip()
                record = (band_id, band_name, album_id, album_name, album_type,
                          year, review, album_url)
                album_data.append(record)

            processed_urls.append(discography_url)
            processed_percent = \
                (len(processed_urls) / len(discography_urls)) * 100
            print(f'\r{len(album_data)} records processed ' +
                  f'({round(processed_percent, 2)}%)', end='')
            q.task_done()

    q = PriorityQueue(16 * 2)
    for _ in range(16):
        t = Thread(target=_get_albumns_concurrently)
        t.daemon = True
        t.start()

    try:
        for discography_url in discography_urls:
            q.put(discography_url)

        q.join()
        print()
    except KeyboardInterrupt:
        sys.exit(1)

    print('albums downloaded')
    album_headers = ('band_id', 'band_name', 'album_id', 'album_name',
                     'album_type', 'year', 'review', 'album_url')
    albums_df = pd.DataFrame(album_data, columns=album_headers)
    albums_df.to_csv('albums.csv', index=False)


def download_data():
    print('downloading bands...')
    download_all_bands()
    print('cleaning band data...')
    clean_band_data()
    print('downloading albums...')
    download_band_details()


if __name__ == '__main__':
    download_data()

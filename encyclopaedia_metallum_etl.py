"""Downloads data from Encyclopaedia Metallum"""

import time
import json
import re
import sys
import copy
import curses

import requests
import pandas as pd
import bs4 as bs
import multiprocessing

from queue import Queue, PriorityQueue
from threading import Thread


ALPHABET = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'NBR', '~']  # the alphabet, according to metal archives

METAL_ARCHIVES_ROOT = 'www.metal-archives.com'
USER_AGENT_STR = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) '
                  + 'Gecko/20100101 Firefox/78.0')

BATCH_SIZE = 500
MAX_ATTEMPTS = 3

NUMBER_OF_THREADS = multiprocessing.cpu_count()


def _create_metallum_api_endpoint(letter, offset):
    """Returns an API endpoint for retrieving a segment of bands
    beginnging with the given letter"""

    endpoint = f'browse/ajax-letter/l/{letter}/json'
    query_string = \
        f'sEcho=1&iDisplayStart={offset}&iDisplayLength={BATCH_SIZE}'

    return f'https://{METAL_ARCHIVES_ROOT}/{endpoint}?{query_string}'


def download_all_bands():
    """Get every band from Encyclopaedia Metallum using the website API"""
    bands = list()
    bands_view = dict()
    bands_screen = curses.initscr()
    threading = True

    def _update_view(letter, message):
        """Textually output progress"""
        bands_view[letter] = message
        bands_screen.clear()
        for index, (key, value) in enumerate(bands_view.items()):
            bands_screen.addstr(index, 0, f'[{key}]: {value}')

        bands_screen.refresh()

    def _get_metallum_records_by_letter(letter):
        """Returns metal bands beginning with the given letter"""

        # the HTML headers that metal archives demands
        headers = {
            'Accept': ('text/html,' +
                       'application/xhtml+xml,' +
                       'application/xml;q=0.9,' +
                       'image/webp,*/*;q=0.8'),
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Host': METAL_ARCHIVES_ROOT,
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': USER_AGENT_STR
        }

        offset = 0

        # retrieve the first batch
        endpoint = _create_metallum_api_endpoint(letter, offset)
        band_data = requests.get(endpoint, headers=headers)
        band_json = json.loads(band_data.text)

        # determine total records to be determined
        total_records = int(band_json['iTotalRecords'])
        records = list(band_json['aaData'])

        # load the remainder of data until the records loaded
        # matches the total number of records specified in the
        # first call
        while offset < total_records:
            time.sleep(1)  # wait a second, to be courteous

            # update view with progress
            record_range = \
                f'{offset}-{min(offset + BATCH_SIZE, total_records)}'
            message = f'downloading {record_range} of {total_records}...'
            _update_view(letter, message)

            # get the next batch
            offset += BATCH_SIZE
            attempts = 0
            endpoint = _create_metallum_api_endpoint(letter, offset)

            # what does metal archives' web server know?
            # does it know things?
            # lets find out
            band_data = requests.get(endpoint, headers=headers)
            if band_data.status_code == 200:
                # it does know things
                band_json = json.loads(band_data.text)
            elif attempts < MAX_ATTEMPTS:
                attempts += 1
                offset -= BATCH_SIZE
            else:
                # it does NOT know things
                raise Exception(f'{band_json.status_code} error')

            records = records + band_json['aaData']

        return records

    # threadable funtion that reads letters from a queue (q) and then
    # retrieves the metal bands beginning with each returned letter
    def _download_by_letter_concurrently():
        while threading:
            letter = q.get()
            if letter:
                _update_view(letter, 'starting download...')
                bands_data = _get_metallum_records_by_letter(letter)
                for datum in bands_data:
                    bands.append(datum)
                _update_view(letter, 'download complete')
            q.task_done()

    q = PriorityQueue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = Thread(target=_download_by_letter_concurrently)
        t.daemon = True
        t.start()

    try:
        # add each letter to the queue
        for letter in ALPHABET:
            q.put(letter)

        # block main thread until downloads are complete
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

    # set threading to false and push empty values into queue
    # this will cause the thread loops to exit, closing each thread
    threading = False
    for _ in range(NUMBER_OF_THREADS):
        q.put(0)  # priority queues require a sortable value

    # dump data to a CSV
    bands_columns = ('band', 'country', 'genre', 'status')
    bands_df = pd.DataFrame(bands, columns=bands_columns)
    bands_df.to_csv('bands_raw.csv', index=False)

    # close cureses window
    curses.endwin()

    print('bands downloaded')


def clean_band_data():
    """Scrub HTML from band data"""
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
    """Retrieves discographies for the bands in bands.csv"""
    bands_df = pd.read_csv('bands.csv')
    album_data = []

    # construct endpoints for each bands discography
    discography_urls = []
    for _, band_id, band_name in bands_df[['band_id', 'name']]:
        endpoint = f'band/discography/id/{band_id}/tab/all'
        discography_url = f'https://www.metal-archives.com/{endpoint}'
        discography_urls.append((band_id, band_name, discography_url))

    processed_urls = []
    total_urls = len(discography_urls)

    threading = True

    def _update_view():
        print(f'\rprocessed {len(processed_urls)} of {total_urls}', end='')

    def _download_band_discography(band_id, band_name, discography_url):
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
                continue
            break

        band_soup = bs.BeautifulSoup(band_webpage.text, 'html.parser')
        band_disco_soup = band_soup.find_all('tr')[1:]

        album_records = []
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
            record = (band_id, band_name, album_id, album_name,
                      album_type, year, review, album_url)
            album_records.append(record)

        return album_records

    def _get_albumns_concurrently():
        while threading:
            band_data = q.get()
            if band_data:
                _, _, discography_url = band_data
                album_records = _download_band_discography(*band_data)

                for record in album_records:
                    album_data.append(record)

                processed_urls.append(discography_url)
                _update_view()
            q.task_done()

    q = Queue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = Thread(target=_get_albumns_concurrently)
        t.daemon = True
        t.start()

    try:
        for band_data in discography_urls:
            q.put(band_data)

        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

    threading = False
    for _ in range(NUMBER_OF_THREADS):
        q.put(None)

    album_headers = ('band_id', 'band_name', 'album_id', 'album_name',
                     'album_type', 'year', 'review', 'album_url')
    albums_df = pd.DataFrame(album_data, columns=album_headers)
    albums_df.to_csv('albums.csv', index=False)


def download_all_tracks():
    albums_df = pd.read_csv('albums.csv')
    selection = ['band_id', 'band_name', 'album_id', 'album_name', 'album_url']
    album_data = albums_df[selection]

    threading = True

    downloaded_tracks = []
    urls_processed = []
    failed_urls = []

    # check to see if any data has already been downloaded
    # if so, continue off of that
    try:
        processed_tracks_df = pd.read_csv('tracks.csv')
        urls_processed = list(processed_tracks_df['album_url'].unique())
        processed_track_records = processed_tracks_df.to_records(index=False)
        downloaded_tracks = [tuple(n) for n in processed_track_records]
        del processed_tracks_df
    except FileNotFoundError:
        pass

    try:
        failed_urls_df = pd.read_csv('failed_album_urls.csv')
        failed_urls = [tuple(n) for n in failed_urls_df.to_records()]
        del failed_urls_df
    except FileNotFoundError:
        pass

    def _update_view(close=False):
        """Textual output"""
        if close:
            print('\ntracks downloaded')
        else:
            percentage = (len(urls_processed) / len(album_data)) * 100
            estimated = (len(downloaded_tracks) / percentage) * 100
            message = f'{len(downloaded_tracks)} tracks downloaded'
            message += f' for {len(urls_processed)} albums'
            message += \
                f' ({percentage:.2f}% of an estimated {estimated:.1e} records)'
            print('\r' + message, end='')

    def _save_records():
        """Save data and record which bands caused errors"""
        tracks = copy.deepcopy(downloaded_tracks)
        tracks_df = pd.DataFrame(tracks)
        tracks_df.columns = ('band_id', 'band_name', 'album_id', 'album_name',
                             'album_url', 'track_name', 'track_number',
                             'track_length')

        failed_records = copy.deepcopy(failed_urls)
        failed_records_df = pd.DataFrame(failed_records)
        failed_records_df.columns = ('band_id', 'band_name', 'album_id',
                                     'album_name', 'album_url')

        tracks_df.to_csv('tracks.csv', index=False)
        failed_records_df.to_csv('failed_album_urls.csv', index=False)

    # the next two functions use this special error to handle issues
    # with parsing album data. albums that throw this error get stored
    # for further analysis.
    class AlbumParseException(Exception):
        """A special error"""

    def _get_album_tracks(album):
        url = album['album_url']
        album_webpage = requests.get(url)

        if album_webpage.status_code == 520:
            time.sleep(15)
            album_webpage = requests.get(url)

        try:
            album_soup = bs.BeautifulSoup(album_webpage.text, 'html.parser')

            track_table_attributes = {'class': 'display table_lyrics'}
            track_attributes = {'class': ['even', 'odd']}

            album_track_soup = album_soup.find('table', track_table_attributes)
            track_soup = album_track_soup.find_all('tr', track_attributes)

            track_records = [[td.text.strip() for td in tr.find_all('td')]
                             for tr in track_soup]
        except Exception:
            raise AlbumParseException()

        tracks = list(map(lambda n: n[1].replace('\n', ' '), track_records))
        track_numbers = \
            map(lambda n: re.sub(r'(.+)\.', r'\g<1>', n[0]), track_records)
        track_lengths = map(lambda n: n[2], track_records)

        band_id = [album['band_id']] * len(tracks)
        band_name = [album['band_name']] * len(tracks)
        album_id = [album['album_id']] * len(tracks)
        album_name = [album['album_name']] * len(tracks)
        album_url = [url] * len(tracks)

        dataset = [band_id, band_name, album_id, album_name, album_url,
                   tracks, track_numbers, track_lengths]

        return list(zip(*dataset))

    def _get_album_tracks_concurrently():
        while threading:
            album_data = q.get()
            if album_data is not None:
                album_url = album_data['album_url']
                if album_url not in urls_processed:
                    time.sleep(1)  # wait a sec, to be courteous
                    try:
                        tracks = _get_album_tracks(album_data)
                    except AlbumParseException:
                        failed_urls.append(album_data)
                    else:
                        for track in tracks:
                            downloaded_tracks.append(track)
                        urls_processed.append(album_url)
                _update_view()
            q.task_done()

    q = Queue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = Thread(target=_get_album_tracks_concurrently)
        t.daemon = True
        t.start()

    try:
        for _, album_record in album_data.iterrows():
            q.put(album_record)

        q.join()
    except KeyboardInterrupt:
        _save_records()
        sys.exit(1)
    except Exception:
        _save_records()
        raise

    _update_view(close=True)

    _save_records()


def download_data():
    # download_all_bands()
    # clean_band_data()
    # download_band_details()
    download_all_tracks()


if __name__ == '__main__':
    download_data()

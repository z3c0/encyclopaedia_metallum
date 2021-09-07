"""Downloads data from Encyclopaedia Metallum"""

import time
import json
import re
import sys
import copy
import os

import requests
import pandas as pd
import bs4 as bs
import datetime as dt

import queue as q
import threading as thr


ALPHABET = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
            'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'NBR', '~']  # the alphabet, according to metal archives

METAL_ARCHIVES_ROOT = 'www.metal-archives.com'
USER_AGENT_STR = ('Python-3.9')

BATCH_SIZE = 500
MAX_ATTEMPTS = 3

NUMBER_OF_THREADS = 32

METALLUM_LOG = 'metallum.log'


class LogComponent:
    '''A thread-safe class for logging info to stdout or a specified file'''

    def __init__(self, stdout=True, path=None):
        if not stdout and path is None:
            print('[-]: a path is required when stdout is False')
            stdout = True

        if stdout and path is None:
            # only write to stdout
            def _print_wrapper(*values, **kwargs):
                print(*values, **kwargs)

        elif stdout and path is not None:
            # write to log and stdout
            def _print_wrapper(*values, **kwargs):
                with open(path, 'a') as log_file:
                    print(*values, **kwargs, file=log_file)
                print(*values, **kwargs)

        else:
            # only write to log
            def _print_wrapper(*values, **kwargs):
                with open(path, 'a') as log_file:
                    print(*values, **kwargs, file=log_file)

        self._write_func = _print_wrapper

        self._is_enabled = True
        self._print_lock = thr.Lock()

    def message(self, text):
        lines = text.split('\n')

        if self._is_enabled:
            with self._print_lock:
                for text in lines:
                    self._write_func(f'[{dt.datetime.now()}]: {text}')

    def disable(self):
        self._is_enabled = False


class Output:
    log = LogComponent(path=METALLUM_LOG)


def _create_metallum_api_endpoint(letter, offset):
    """Returns an API endpoint for retrieving a segment of bands
    beginning with the given letter"""

    endpoint = f'browse/ajax-letter/l/{letter}/json'
    query_string = \
        f'sEcho=1&iDisplayStart={offset}&iDisplayLength={BATCH_SIZE}'

    return f'https://{METAL_ARCHIVES_ROOT}/{endpoint}?{query_string}'


def _get_metallum_records_by_letter(letter: str):
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


def _write_band_data_to_csv(band_records: list):
    # dump raw data to a CSV
    bands_columns = ('band', 'country', 'genre', 'status')
    bands_df = pd.DataFrame(band_records, columns=bands_columns)
    bands_df.to_csv('out/bands_raw.csv', index=False)

    # clean band data
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

    band_columns = ('metallum_band_id', 'name', 'genre', 'country', 'status',
                    'url')
    clean_df = pd.DataFrame(clean_records, columns=band_columns)
    clean_df.to_csv('out/bands.csv', index=False)


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
            raise
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


def _create_discography_urls(bands_df: pd.DataFrame) -> list:
    # construct endpoints for each bands discography

    Output.log.message('creating discography URLs')
    discography_urls = []
    band_records = bands_df[['metallum_band_id', 'name']].iterrows()
    for _, (band_id, band_name) in band_records:
        endpoint = f'band/discography/id/{band_id}/tab/all'
        discography_url = f'https://www.metal-archives.com/{endpoint}'
        discography_urls.append((band_id, band_name, discography_url))

    return discography_urls


class AlbumParseException(Exception):
    """Album data could not be parsed"""


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

    band_id = [album['metallum_band_id']] * len(tracks)
    band_name = [album['band_name']] * len(tracks)
    album_id = [album['metallum_album_id']] * len(tracks)
    album_name = [album['album_name']] * len(tracks)
    album_url = [url] * len(tracks)

    dataset = [band_id, band_name, album_id, album_name, album_url,
               tracks, track_numbers, track_lengths]

    return list(zip(*dataset))


def download_all_bands():
    """Get every band from Encyclopaedia Metallum using the website API"""
    bands = list()

    # threadable function that reads letters from a queue (q) and then
    # retrieves the metal bands beginning with each returned letter
    def _download_by_letter_concurrently():
        halting = False
        while not halting:
            letter = queue.get()

            halting = letter == 0

            if not halting:
                display_letter = '#' if letter == 'NBR' else letter
                msg = f'bands | {display_letter} | starting download'
                Output.log.message(msg)
                bands_data = _get_metallum_records_by_letter(letter)
                for datum in bands_data:
                    bands.append(datum)
                msg = f'bands | {display_letter} | download complete'
                Output.log.message(msg)
            else:
                current_thread = thr.current_thread()
                thread_msg = (f'closing {current_thread.name} '
                              f'(ID: {current_thread.native_id})')
                Output.log.message(thread_msg)

            queue.task_done()

    queue = q.PriorityQueue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = thr.Thread(target=_download_by_letter_concurrently)
        t.daemon = True
        t.start()

    try:
        # add each letter to the queue
        for letter in ALPHABET:
            queue.put(letter)

        # block main thread until downloads are complete
        queue.join()
    except KeyboardInterrupt:
        sys.exit(1)

    # set threading to false and push empty values into queue
    # this will cause the thread loops to exit, closing each thread
    for _ in range(NUMBER_OF_THREADS):
        queue.put(0)  # priority queues require a sortable value

    _write_band_data_to_csv(bands)


def download_band_details():
    """Retrieves discographies for the bands in bands.csv"""
    bands_df = pd.read_csv('out/bands.csv')
    album_data = []
    processed_urls = []
    discography_urls = _create_discography_urls(bands_df)

    del bands_df

    def _get_albums_concurrently():
        halting = False
        while not halting:
            band_data = queue.get()

            halting = band_data is None

            if not halting:
                _, _, discography_url = band_data
                album_records = _download_band_discography(*band_data)

                for record in album_records:
                    album_data.append(record)

                processed_urls.append(discography_url)

                if len(album_data) % 100 == 0:
                    msg = (f'{len(album_data)} albums downloaded')
                    Output.log.message(msg)
            else:
                current_thread = thr.current_thread()
                thread_msg = (f'closing {current_thread.name} '
                              f'(ID: {current_thread.native_id})')
                Output.log.message(thread_msg)

            queue.task_done()

    Output.log.message('starting threads')

    queue = q.Queue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = thr.Thread(target=_get_albums_concurrently)
        t.daemon = True
        t.start()

    try:
        for band_data in discography_urls:
            queue.put(band_data)

        queue.join()
    except KeyboardInterrupt:
        sys.exit(1)

    for _ in range(NUMBER_OF_THREADS):
        queue.put(None)

    album_headers = ('metallum_band_id', 'band_name', 'metallum_album_id',
                     'album_name', 'album_type', 'year', 'review', 'album_url')
    albums_df = pd.DataFrame(album_data, columns=album_headers)
    albums_df.to_csv('out/albums.csv', index=False)


def download_all_tracks():
    albums_df = pd.read_csv('out/albums.csv')
    selection = ['metallum_band_id', 'band_name', 'metallum_album_id',
                 'album_name', 'album_url']
    album_data = albums_df[selection]

    downloaded_tracks = []
    urls_processed = []
    failed_urls = []

    # check to see if any data has already been downloaded
    # if so, continue off of that
    try:
        processed_tracks_df = pd.read_csv('out/tracks.csv')
        urls_processed = list(processed_tracks_df['album_url'].unique())
        processed_track_records = processed_tracks_df.to_records(index=False)
        downloaded_tracks = [tuple(n) for n in processed_track_records]
        del processed_tracks_df
    except FileNotFoundError:
        pass

    try:
        failed_urls_df = pd.read_csv('out/failed_album_urls.csv')
        failed_urls = [tuple(n) for n in failed_urls_df.to_records()]
        del failed_urls_df
    except FileNotFoundError:
        pass

    def _update_view():
        """Textual output"""
        percentage = (len(urls_processed) / len(album_data)) * 100
        estimated = (len(downloaded_tracks) / percentage) * 100
        message = f'{len(downloaded_tracks)} tracks downloaded'
        message += \
            f' ({percentage:.2f}% of an estimated {estimated:.1e} records)'

        Output.log.message(message)

    def _save_records():
        """Save data and record which bands caused errors"""
        tracks = copy.deepcopy(downloaded_tracks)
        tracks_df = pd.DataFrame(tracks)
        tracks_df.columns = ('metallum_band_id', 'band_name',
                             'metallum_album_id', 'album_name',
                             'album_url', 'track_name', 'track_number',
                             'track_length')

        tracks_df.to_csv('out/tracks.csv', index=False)

        failed_records = copy.deepcopy(failed_urls)
        if len(failed_records) > 0:
            failed_records_df = pd.DataFrame(failed_records)
            failed_records_df.columns = ('metallum_band_id', 'band_name',
                                         'album_id', 'album_name', 'album_url')

            failed_records_df.to_csv('out/failed_album_urls.csv', index=False)

    def _get_album_tracks_concurrently():
        halting = False
        while not halting:
            album_data = queue.get()

            halting = album_data is None

            if not halting:
                album_url = album_data['album_url']
                if album_url not in urls_processed:
                    try:
                        tracks = _get_album_tracks(album_data)
                    except AlbumParseException:
                        failed_urls.append(album_data)
                    else:
                        for track in tracks:
                            downloaded_tracks.append(track)
                        urls_processed.append(album_url)

                if len(urls_processed) % 1000 == 0:
                    _update_view()

            else:
                current_thread = thr.current_thread()
                thread_msg = (f'closing {current_thread.name} '
                              f'(ID: {current_thread.native_id})')
                Output.log.message(thread_msg)

            queue.task_done()

    queue = q.Queue(NUMBER_OF_THREADS * 2)
    for _ in range(NUMBER_OF_THREADS):
        t = thr.Thread(target=_get_album_tracks_concurrently)
        t.daemon = True
        t.start()

    try:
        for _, album_record in album_data.iterrows():
            queue.put(album_record)

        queue.join()
    except KeyboardInterrupt:
        _save_records()
        sys.exit(1)
    except Exception:
        _save_records()
        raise

    Output.log.message('tracks downloaded - saving data')

    _save_records()

    for _ in range(NUMBER_OF_THREADS):
        queue.put(None)
    queue.join()


def download_data(bands=True, albums=False, tracks=False):
    try:
        os.mkdir('out')
    except FileExistsError:
        pass

    if bands:
        Output.log.message('downloading bands')
        download_all_bands()

    if albums:
        Output.log.message('downloading band details')
        download_band_details()

    if tracks:
        Output.log.message('downloading tracks')
        download_all_tracks()


if __name__ == '__main__':
    Output.log.disable()
    download_data(False, True, False)

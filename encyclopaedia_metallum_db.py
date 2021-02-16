import re
import pandas as pd
import itertools as it

from sqlalchemy import create_engine
from decouple import config

USER = config('USER')
PASSWORD = config('PASSWORD')
IP_ADDRESS = config('IP_ADDRESS')
DATABASE = 'metallum'

conn_str = f'mysql+pymysql://{USER}:{PASSWORD}@{IP_ADDRESS}/{DATABASE}'
mysql_conn = create_engine(conn_str)


def load_bands():
    bands_df = pd.read_csv('bands.csv')
    bands_df.columns = ('metallum_band_id', 'band_name', 'genre', 'country',
                        'band_status', 'band_url')

    bands_df.index.name = 'band_id'
    bands_df = bands_df.drop_duplicates()
    bands_df = bands_df.convert_dtypes()
    bands_df.to_sql('bands', mysql_conn, if_exists='replace')


def load_albums():
    albums_df = pd.read_csv('albums.csv')
    albums_df.columns = ('metallum_band_id', 'band_name', 'metallum_album_id',
                         'album_name', 'album_type', 'year', 'review',
                         'album_url')

    albums_df.index.name = 'album_id'
    albums_df = albums_df.drop_duplicates()
    albums_df = albums_df.convert_dtypes()
    albums_df.to_sql('albums', mysql_conn, if_exists='replace')


def load_countries():
    bands_df = pd.read_sql('bands', mysql_conn)
    unique_countries = bands_df['country'].unique()
    unique_countries.sort()
    country_df = pd.DataFrame(unique_countries, columns=['country_name'])
    country_df.index.name = 'country_id'
    country_df.to_sql('countries', mysql_conn, if_exists='replace')


def load_genres():
    band_genres_df = pd.read_sql('band_genres', mysql_conn)
    unique_genres = band_genres_df['genre_name'].unique()
    unique_genres.sort()
    genre_df = pd.DataFrame(unique_genres, columns=['genre_name'])
    genre_df.index.name = 'genre_id'
    genre_df.to_sql('genres', mysql_conn, if_exists='replace')


def load_tracks():
    def track_generator(page_size):
        file = open('tracks.csv', 'r', encoding='utf-8')

        line = file.readline()
        columns = line.strip().split(',')

        row_count = 0
        for _ in file:
            row_count += 1

        file.close()

        max_index = 0

        while max_index < row_count:
            kwargs = {'skiprows': max_index + 1, 'nrows': page_size}
            page_df = pd.read_csv('tracks.csv', **kwargs)
            if page_df.empty:
                break

            page_df.columns = columns

            page_df.index.name = 'track_id'

            page_df.index = page_df.index + max_index

            page_df['metallum_band_id'] = \
                page_df['metallum_band_id'].astype('int64')

            page_df['metallum_album_id'] = \
                page_df['metallum_album_id'].astype('int64')

            page_df['track_number'] = \
                page_df['track_number'].astype('int32')

            yield page_df

            max_index = max(page_df.index) + 1

    track_gen = track_generator(100000)

    initial_load = next(track_gen)
    initial_load.to_sql('tracks', mysql_conn, if_exists='replace')

    for track_page in track_gen:
        track_page.to_sql('tracks', mysql_conn, if_exists='append')


def process_band_genres():
    bands_df = pd.read_sql('bands', mysql_conn)
    selection = ['band_id', 'genre']
    genre_df = bands_df[selection]
    genre_df.columns = ('band_id', 'genre')
    genre_df = genre_df.set_index('band_id')

    # (first pass)
    # split genres by commas that aren't contained
    # with parentheses. At the same time,
    # scrub anomalous details that causes genres
    # to fall out of common patterns or break grouping
    unpivoted_genre_phases = list()
    for band_index, row in genre_df.iterrows():
        genres = re.split(r'(?!\B\([^\)]*),(?![^\(]*\)\B)', row['genre'])

        for genre in genres:
            genre = genre.lower()

            genre = re.sub(r'\u200b', '', genre)

            # somebody left a cyrillic c in one of the RAC entries
            genre = re.sub(chr(1089), chr(99), genre)
            genre = re.sub(r'(\w)\(', r'\g<1> (', genre)
            genre = re.sub(r'\)\/? ', r'); ', genre)
            genre = re.sub(r' \- ', ' ', genre)
            phases = genre.split(';')
            for phase in phases:
                record = (band_index, phase)
                unpivoted_genre_phases.append(record)

    # (second pass)
    # parse phases into a separate column
    # e.g. thrash metal (early) power metal (later)
    desired_phases = ('later', 'early', 'mid')
    processed_genre_phases = list()
    for band_index, p in unpivoted_genre_phases:
        phase_match = re.search(r' \(.+\)$', p)
        if phase_match:
            phase_text = phase_match.group(0)
            genre = p.replace(phase_text, '')
            cleaned_phases = phase_text.lstrip()[1:-1]
            split_phases = re.split(r'[,\/\\]', cleaned_phases)

            records = set()
            for phase in split_phases:
                phase = phase.strip()

                # filter phases that don't adhere to the discrete
                # categories 'early', 'mid', and 'later'
                phase = phase if phase in desired_phases else None
                records.add((band_index, genre, phase))

            records = list(records)

        else:
            genre = p
            records = [(band_index, genre, None)]

        processed_genre_phases += records

    # (third pass)
    # fix data issues
    cleaned_records = list()
    junk = ('', 'metal', 'elements', 'influences', 'music')
    for band_index, genre, phase in processed_genre_phases:
        genre = genre.strip()

        # normalize and removes spaces from common patterns
        genre = re.sub(r' \'?n\'? ', '\'n\'', genre)
        genre = re.sub(r'nu ', 'nu-', genre)
        genre = re.sub(r'new ', 'new-', genre)
        genre = re.sub(r'hard ', 'hard-', genre)
        genre = re.sub(r'free ', 'free-', genre)
        genre = re.sub(r'post ', 'post-', genre)
        genre = re.sub(r'jazzy', 'jazz', genre)
        genre = re.sub(r'pop rock', 'pop-rock', genre)
        genre = re.sub(r'trip hop', 'trip-hop', genre)
        genre = re.sub(r'cloud rap', 'cloud-rap', genre)
        genre = re.sub(r'a cappella', 'a-cappela', genre)
        genre = re.sub(r'bossa nova', 'bossa-nova', genre)
        genre = re.sub(r'spoken word', 'spoken-word', genre)
        genre = re.sub(r'film score', 'film-score', genre)
        genre = re.sub(r'world music', 'world-music', genre)
        genre = re.sub(r'middle eastern', 'middle-eastern', genre)
        genre = re.sub(r'ethnic music', 'ethnic-music', genre)
        genre = re.sub(r'game music', 'game-music', genre)
        genre = re.sub(r'drum and bass', 'drum-and-bass', genre)
        genre = re.sub(r'psychedellic rock', 'psychedellic-rock', genre)
        genre = re.sub(r'power electronics', 'power-electronics', genre)
        genre = re.sub(r'neue deutsche härte', 'neue-deutsche-härte', genre)

        # turn "genre'n'roll" into "genre rock'n'roll"
        genre = re.sub(r'(\S+)\'n\'roll', r"\g<1> rock'n'roll", genre)

        genres = genre.split(' with ')
        genres = list(set().union(*[g.split(' and ') for g in genres]))
        genres = list(set().union(*[g.split('/') for g in genres]))

        # finally, spaces
        genres = list(set().union(*[g.split(' ') for g in genres]))

        # final clean-up
        for genre in genres:
            genre = genre.strip()

            if genre.endswith('-'):
                genre += 'metal'

            # lump oddballs into their closest relatives
            if genre == 'post':
                genre = 'post-metal'

            if genre == 'hard':
                genre = 'hard-rock'

            if genre == 'soft':
                genre = 'soft-rock'

            if genre == 'electronics':
                genre = 'power-electronics'

            if genre == 'atmoshpheric':
                genre = 'atmospheric'

            if genre == 'world':
                genre = 'world-music'

            if genre in junk:
                continue

            cleaned_record = (band_index, genre, phase)
            cleaned_records.append(cleaned_record)

    cleaned_genres_df = pd.DataFrame(cleaned_records)
    cleaned_genres_df.columns = ('band_id', 'genre_name', 'phase_name')
    cleaned_genres_df.index.name = 'band_genre_id'
    cleaned_genres_df = cleaned_genres_df.drop_duplicates()
    cleaned_genres_df = cleaned_genres_df.convert_dtypes()
    cleaned_genres_df.to_sql('band_genres', mysql_conn, if_exists='replace')


def process_band_genre_changes():
    genre_df = pd.read_sql('band_genres', mysql_conn)

    genre_phase_records = []

    band_ids = genre_df['band_id'].unique()
    for band_id in band_ids:
        band_records = genre_df[genre_df['band_id'] == band_id]

        has_null_phases = pd.isnull(band_records['phase_name'])
        core_genre_records = band_records[has_null_phases]
        core_genres = list(core_genre_records['genre_name'].unique())

        is_early_phase = band_records['phase_name'] == 'early'
        is_mid_phase = band_records['phase_name'] == 'mid'
        is_later_phase = band_records['phase_name'] == 'later'

        early_genres = \
            list((band_records[is_early_phase])['genre_name'].unique())
        mid_genres = \
            list((band_records[is_mid_phase])['genre_name'].unique())
        later_genres = \
            list((band_records[is_later_phase])['genre_name'].unique())

        record = (band_id, core_genres + early_genres,
                  core_genres + mid_genres, core_genres + later_genres)

        genre_phase_records.append(record)

    genre_phase_df = pd.DataFrame(genre_phase_records)
    genre_phase_df = genre_phase_df.drop_duplicates()
    genre_phase_df.columns = ('band_id', 'early_phase', 'mid_phase',
                              'later_phase')


def process_genre_relationships():
    band_genres_df = pd.read_sql('SELECT * FROM band_genres_vw', mysql_conn)

    genre_permutations = list()
    band_ids = band_genres_df['band_id'].unique()
    for band_id in band_ids:
        genres_df = band_genres_df[band_genres_df['band_id'] == band_id]
        genre_relationships = list(it.permutations(genres_df['genre_id'], 2))
        genre_permutations += genre_relationships

    genre_permutations_cols = ('genre_id', 'related_genre_id')
    genre_permutations_df = \
        pd.DataFrame(genre_permutations, columns=genre_permutations_cols)
    genre_permutations_df.index.name = 'genre_permutation_id'

    genre_combos = list()
    band_ids = band_genres_df['band_id'].unique()
    for band_id in band_ids:
        genres_df = band_genres_df[band_genres_df['band_id'] == band_id]
        genre_relationships = list(it.combinations(genres_df['genre_id'], 2))
        genre_combos += genre_relationships

    genre_combos_cols = ('genre_id', 'related_genre_id')
    genre_combos_df = \
        pd.DataFrame(genre_combos, columns=genre_combos_cols)
    genre_combos_df.index.name = 'genre_combination_id'

    genre_combos_df.to_sql('genre_combos', mysql_conn,
                           if_exists='replace')
    genre_permutations_df.to_sql('genre_permutations', mysql_conn,
                                 if_exists='replace')


if __name__ == '__main__':
    # print('loading bands...')
    # load_bands()

    # print('loading albums...')
    # load_albums()

    # print('loading tracks...')
    # load_tracks()

    # print('processing band genres...')
    # process_band_genres()

    # print('processing band genre changes...')
    # process_band_genre_changes()

    # print('loading genres...')
    # load_genres()

    print('processing genre relationships...')
    process_genre_relationships()

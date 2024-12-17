import requests
import sqlite3
import os
import re
from bs4 import BeautifulSoup
import lyricsgenius
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from time import sleep
import time

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, db_name)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    return cur, conn

def create_artists_table():
    cur, conn = set_up_database("final.db")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')
    conn.commit()
    conn.close()

def create_songs_table():
    cur, conn = set_up_database("final.db")
    cur.execute('''
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            artist TEXT,
            word TEXT,
            duration TEXT,
            wpm INTEGER,
            popularity INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def normalize_string(string):
    """Remove special characters and standardize formatting, including quotes."""
    string = re.sub(r'[“”]', '', string)
    string = re.sub(r"[\"'\x92\x93\x94]", '', string)  
    string = re.sub(r'\(.*?\)', '', string)
    string = re.sub(r'feat\.|featuring', '', string, flags=re.IGNORECASE)
    string = re.sub(r'[^\w\s]', '', string)
    return string.lower().strip()

def scrape_from_static():
    end_artists = []
    end_songs = []

    with open('RS100.html', 'r', encoding='utf-8') as file:
        static_html = file.read()

    soup = BeautifulSoup(static_html, 'html.parser')

    found_titles = soup.find_all("article", class_="pmc-fallback-list-item")

    if found_titles:
        for x in found_titles:
            h2tag = x.find("h2")
            if h2tag:
                title = h2tag.get_text().strip()
                if "," in title:
                    artist, song = title.split(",", 1)
                    end_artists.append(artist.strip())
                    end_songs.append(song.strip())
    
    end_artists.reverse()
    end_songs.reverse()

    return end_artists, end_songs

def scrape_from_live_url():
    artists = []
    songs = []
    url = "https://www.rollingstone.com/music/music-lists/the-100-best-songs-of-the-2010s-917532/dj-snake-lil-jon-turn-down-for-what-917594/"

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        found_articles = soup.find_all('article', class_='pmc-fallback-list-item')

        for article in found_articles:
            h2_tag = article.find('h2')
            if h2_tag:
                title = h2_tag.get_text().strip()

                if "," in title:
                    artist, song = title.split(",", 1)
                    artists.append(artist.strip())
                    songs.append(song.strip())
    
    artists.reverse()
    songs.reverse()
    
    return artists, songs

def combine_and_copy_data(cur, sp):
    cur.execute('SELECT COUNT(*) FROM songs')
    row_count = cur.fetchone()[0]

    if row_count >= 100:
        print("The 'songs' table already has 100 rows. Skipping new data insertion.")
        
        # Fetch the existing songs and artists from the database
        cur.execute('''
            SELECT s.title, s.artist FROM songs s ORDER BY rowid ASC
        ''')
        rows = cur.fetchall()
        artists = [row[1] for row in rows]
        songs = [row[0] for row in rows]
        
        return artists, songs

    artists_50_1, songs_50_1 = scrape_from_live_url()
    artists_100_51, songs_100_51 = scrape_from_static()

    artists_combined = artists_50_1 + artists_100_51
    songs_combined = songs_50_1 + songs_100_51

    # Insert data in batches of 25
    batch_size = 25
    start_index = row_count
    end_index = min(start_index + batch_size, 100)

    for i, (artist, song) in enumerate(list(zip(artists_combined, songs_combined))[start_index:end_index], start=start_index + 1):
        cur.execute('''
            SELECT name FROM artists WHERE name = ?
        ''', (artist,))
        artist_name = cur.fetchone()

        if artist_name is None:
            # Insert the artist if not already present
            cur.execute('''
                INSERT INTO artists (name) VALUES (?)
            ''', (artist,))
        
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        if spotify_data:
            duration = spotify_data['duration']
            popularity = spotify_data['popularity']
        else:
            duration = "Unknown"
            popularity = None

        cur.execute('''
            INSERT INTO songs (title, artist, popularity, word, duration, wpm)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (song, artist, popularity, "", duration, ""))

    cur.connection.commit()

    copy_columns_data('songs.db', 'final.db', 'lyrics', 'songs', ['word', 'wpm'])

    return artists_combined, songs_combined

def copy_columns_data(source_db, target_db, source_table, target_table, column_names):
    try:
        source_conn = sqlite3.connect(source_db)
        source_cursor = source_conn.cursor()

        source_cursor.execute(f"PRAGMA table_info({source_table})")
        columns = [col[1] for col in source_cursor.fetchall()]
        
        for column_name in column_names:
            if column_name not in columns:
                raise ValueError(f"Column '{column_name}' does not exist in source table '{source_table}'")

        placeholders = ', '.join(column_names)
        source_cursor.execute(f"SELECT {placeholders} FROM {source_table}")
        data = source_cursor.fetchall()

        target_conn = sqlite3.connect(target_db)
        target_cursor = target_conn.cursor()

        batch_size = 25
        total_rows = len(data)
        num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size != 0 else 0)

        for batch_num in range(num_batches):
            start_index = batch_num * batch_size
            end_index = min((batch_num + 1) * batch_size, total_rows)
            current_batch = data[start_index:end_index]

            for idx, row in enumerate(current_batch, start=start_index + 1):
                target_cursor.execute(f"UPDATE {target_table} SET word = ?, wpm = ? WHERE rowid = ?", 
                                      (row[0], row[1], idx))

            target_conn.commit()
            print(f"Inserted rows {start_index + 1} to {end_index} into target table.")

        print(f"Data from columns {column_names} in table '{source_table}' has been successfully copied to the target database.")

    except sqlite3.Error as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Value Error: {e}")
    finally:
       
        if source_conn:
            source_conn.close()
        if target_conn:
            target_conn.close()


source_db = 'songs.db'  # Path to the source database
target_db = 'final.db'  # Path to the target database
source_table = 'lyrics'  # Name of the source table
target_table = 'songs'  # Name of the target table
column_names = ['word', 'wpm']  # List of columns to copy

def spotify_authenticate():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id='e1995ad727784f5abd056cc7e77d272c',
        client_secret='fa4a74248a304f39b1a7007c70f671d0',
        redirect_uri='http://localhost:8888/callback',
        scope='user-library-read'
    ))
    return sp

def get_spotify_data_with_fallback(sp, title, artist):
    retries = 3  

    for attempt in range(retries):
        sleep(0.5)  
        normalized_title = normalize_string(title)
        normalized_artist = normalize_string(artist)

      
        query = f"track:{normalized_title} artist:{normalized_artist}"
        print(f"Searching for {title} by {artist} using query: {query}")  
        results = sp.search(q=query, type='track', limit=1)

        if results['tracks']['items']:
            track = results['tracks']['items'][0]
            duration_ms = track.get('duration_ms', None)  

            if duration_ms:
                duration_sec = duration_ms / 1000  # Convert ms to seconds
                minutes = int(duration_sec // 60)
                seconds = int(duration_sec % 60)
                duration = f"{minutes}:{seconds:02d}"
                print(f"Found track: {title} by {artist}, duration: {duration}")  # Log successful fetch
                return {
                    'spotify_id': track['id'],
                    'popularity': track['popularity'],
                    'album': track['album']['name'],
                    'duration': duration
                }
            else:
                print(f"Warning: No duration found for track: {title} by {artist}")
        else:
            print(f"Warning: No track found for query: {query}")
                
            # If the first query failed, retry with only the song title (ignoring artist)
            print(f"Retrying with title only: {title}")
            query = f"track:{normalized_title}"
            results = sp.search(q=query, type='track', limit=1)

            if results['tracks']['items']:
                track = results['tracks']['items'][0]
                duration_ms = track.get('duration_ms', None)

                if duration_ms:
                    duration_sec = duration_ms / 1000  
                    minutes = int(duration_sec // 60)
                    seconds = int(duration_sec % 60)
                    duration = f"{minutes}:{seconds:02d}"
                    print(f"Found track (title only): {title}, duration: {duration}") 
                    return {
                        'spotify_id': track['id'],
                        'popularity': track['popularity'],
                        'album': track['album']['name'],
                        'duration': duration
                    }
                else:
                    print(f"Warning: No duration found for track (title only): {title}")
            else:
                print(f"Warning: No track found for title-only query: {title}")
    return None  

def update_artist_ids_in_songs(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    sql_query = '''
        UPDATE songs
        SET artist = (
            SELECT artists.id
            FROM artists
            WHERE artists.name = songs.artist
        )
        WHERE EXISTS (
            SELECT 1
            FROM artists
            WHERE artists.name = songs.artist
        );
    '''

    cur.execute(sql_query)

    conn.commit()
    conn.close()

def fetch_lyrics_and_count_words(genius, song, artist):
    retries = 3  
    delay = 5    

    for attempt in range(retries):
        
        song_info = genius.search_song(song, artist)
        if song_info and song_info.lyrics:
            lyrics = song_info.lyrics
            
            word_count = len(lyrics.split())
            print(f"Fetched {word_count} words for {song} by {artist}")
            return word_count
        else:
            print(f"No lyrics found for {song} by {artist}")
            return 0  # Return 0 if no lyrics are found
    return 0  # Return 0 after retries if still no success

def update_songs_with_lyrics_and_word_count(db_name, genius, sp):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute('SELECT title, artist FROM songs')
    songs = cur.fetchall()

    for song, artist in songs:
       
        word_count = fetch_lyrics_and_count_words(genius, song, artist)
        
       
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        duration = spotify_data['duration'] if spotify_data else "Unknown"
        
      
        if spotify_data and spotify_data['duration'] != "Unknown":
            # Duration in "mm:ss" format, so we need to extract the minutes and seconds
            minutes, seconds = map(int, duration.split(":"))
            total_seconds = minutes * 60 + seconds
            if total_seconds > 0:
                wpm = word_count / (total_seconds / 60)  # Words per minute
            else:
                wpm = 0
        else:
            wpm = 0

        
        cur.execute('''
            UPDATE songs
            SET word = ?, duration = ?, wpm = ?
            WHERE title = ? AND artist = ?
        ''', (word_count, duration, int(wpm), song, artist))

    conn.commit()
    conn.close()

def populate_lyrics_table_with_duration(cur, conn, artists, songs, sp):
    """Populate the lyrics table with song lyrics, word counts, and duration."""

    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"  
    genius = lyricsgenius.Genius(token, timeout=10)

    for i, (artist, song) in enumerate(zip(artists, songs)):
        print(f"Fetching lyrics for {song} by {artist}")
        
        # Get Spotify data (duration)
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        
        if spotify_data:
            duration = spotify_data['duration']
        else:
            duration = "Unknown"  
     
        song_info = fetch_lyrics_and_count_words(genius, song, artist)
        
        if song_info and song_info.lyrics:
       
            word_count = len(song_info.lyrics.split())

            
            cur.execute('''
                INSERT OR IGNORE INTO lyrics (id, title, artist, word, duration)
                VALUES (?, ?, ?, ?, ?)
            ''', (i + 1, song, artist, word_count, duration))
        else:
            print(f"Lyrics not found for {song} by {artist}")
        
    conn.commit()

def main():
   
    create_artists_table()
    create_songs_table()

    sp = spotify_authenticate()

    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"
    genius = lyricsgenius.Genius(token, timeout=10)

    cur, conn = set_up_database("final.db")

    artists, songs = combine_and_copy_data(cur, sp)

    copy_columns_data(source_db, target_db, source_table, target_table, column_names)

    update_songs_with_lyrics_and_word_count('final.db', genius, sp)

    update_artist_ids_in_songs('final.db')
    
    conn.close()

if __name__ == "__main__":
    main()
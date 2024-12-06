import requests
import sqlite3
import os
import re
from bs4 import BeautifulSoup
import lyricsgenius
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from time import sleep

def fetch_lyrics_with_retries(genius, song, artist, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            song_info = genius.search_song(song, artist)
            if song_info:
                return song_info
        except Exception as e:
            print(f"Error fetching lyrics for {song} by {artist}: {e}")
            retries += 1
            print(f"Retrying ({retries}/{max_retries})...")
            sleep(2)  
    return None  

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, db_name)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    return cur, conn

def first_table():
    cur, conn = set_up_database("songs.db")
    cur.execute('''CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        title TEXT, 
        artist TEXT,
        popularity INTEGER,
        UNIQUE(title, artist)  -- Add a unique constraint to prevent duplicate songs
    )''')
    conn.commit()
    conn.close()

def remove_columns_from_songs(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS temp_songs AS
        SELECT id, title, artist, popularity
        FROM songs
    ''')
    cur.connection.commit()

    cur.execute('DROP TABLE IF EXISTS songs')
    cur.connection.commit()

    cur.execute('ALTER TABLE temp_songs RENAME TO songs')
    cur.connection.commit()

    print("'spotify_id' column removed from 'songs' table.")

def ensure_lyrics_table_structure(cur, conn):
    cur.execute('PRAGMA table_info(lyrics);')  
    columns = [column[1] for column in cur.fetchall()]

    if 'duration' not in columns:
        cur.execute('''
            ALTER TABLE lyrics ADD COLUMN duration TEXT;
        ''')
        conn.commit()
        print("'duration' column added to 'lyrics' table.")

    if 'wpm' not in columns:
        cur.execute('''
            ALTER TABLE lyrics ADD COLUMN wpm REAL;
        ''')
        conn.commit()
        print("'wpm' column added to 'lyrics' table.")

def normalize_string(string):
    """Remove special characters and standardize formatting."""
    string = re.sub(r'[“”"\']', '', string).strip()  
    string = re.sub(r'\(.*?\)', '', string)  
    string = re.sub(r'feat\.|featuring', '', string, flags=re.IGNORECASE)  
    return string.lower()

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

def combine_and_order(cur):
    cur.execute('SELECT COUNT(*) FROM songs')
    row_count = cur.fetchone()[0]
    
    if row_count >= 100:
        print("The 'songs' table already has 100 rows. Skipping new data insertion.")
        return [], []  

    artists_50_1, songs_50_1 = scrape_from_live_url()
    artists_100_51, songs_100_51 = scrape_from_static()

    artists_combined = artists_50_1 + artists_100_51
    songs_combined = songs_50_1 + songs_100_51

    remaining_count = 100 - row_count
    songs_to_insert = zip(artists_combined, songs_combined)[:remaining_count]

    for i, (artist, song) in enumerate(songs_to_insert, row_count + 1):
        cur.execute('''
            SELECT id FROM songs WHERE title = ? AND artist = ?
        ''', (song, artist))
        existing_song = cur.fetchone()

        if existing_song is None:
            cur.execute('''
                INSERT INTO songs (id, title, artist)
                VALUES (?, ?, ?)
            ''', (i, song, artist))

    cur.connection.commit()
    return artists_combined, songs_combined

def clean_up_songs_table(cur):
    cur.execute('''
        DELETE FROM songs WHERE id > 100
    ''')
    cur.connection.commit()
    print("Deleted rows beyond ID 100 from 'songs' table.")

def spotify_authenticate():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id='e1995ad727784f5abd056cc7e77d272c',
        client_secret='fa4a74248a304f39b1a7007c70f671d0',
        redirect_uri='http://localhost:8888/callback',
        scope='user-library-read'
    ))
    return sp

def get_spotify_data_with_fallback(sp, title, artist):
    sleep(0.5) 
    query = f"track:{normalize_string(title)} artist:{normalize_string(artist)}"
    results = sp.search(q=query, type='track', limit=1)

    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        duration_ms = track.get('duration_ms', None)  
        if duration_ms:
            duration_sec = duration_ms / 1000  
            minutes = int(duration_sec // 60)
            seconds = int(duration_sec % 60)
            duration = f"{minutes}:{seconds:02d}" 
            return {
                'spotify_id': track['id'],
                'popularity': track['popularity'],
                'album': track['album']['name'],
                'duration': duration  
            }
        else:
            print(f"Warning: No duration found for track: {title} by {artist}")
    else:
        print(f"Warning: No track found for query: {title} by {artist}")

    print(f"Retrying with title only: {title}")
    query = f"track:{normalize_string(title)}"
    results = sp.search(q=query, type='track', limit=1)
    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        duration_ms = track.get('duration_ms', None)
        if duration_ms:
            duration_sec = duration_ms / 1000
            minutes = int(duration_sec // 60)
            seconds = int(duration_sec % 60)
            duration = f"{minutes}:{seconds:02d}"
            return {
                'spotify_id': track['id'],
                'popularity': track['popularity'],
                'album': track['album']['name'],
                'duration': duration
            }
        else:
            print(f"Warning: No duration found for track (title only): {title}")

    return None
    
def populate_lyrics_table_with_duration(cur, conn, artists, songs, sp):
    """Populate the lyrics table with song lyrics, word counts, and duration."""
    
    ensure_lyrics_table_structure(cur, conn)

    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"  
    genius = lyricsgenius.Genius(token, timeout=10)

    for i, (artist, song) in enumerate(zip(artists, songs)):
        print(f"Fetching lyrics for {song} by {artist}")
        
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        
        if spotify_data:
            duration = spotify_data['duration']
        else:
            duration = "Unknown"  
        
        song_info = fetch_lyrics_with_retries(genius, song, artist)
        
        if song_info and song_info.lyrics:
            word_count = len(song_info.lyrics.split())

            cur.execute('''
                INSERT OR IGNORE INTO lyrics (id, title, artist, word, duration)
                VALUES (?, ?, ?, ?, ?)
            ''', (i + 1, song, artist, word_count, duration))
        else:
            print(f"Lyrics not found for {song} by {artist}")
        
    conn.commit()

def update_duration_and_wpm(cur, sp):
    """Update the 'duration' and 'wpm' columns for songs with null values."""
    
    cur.execute("SELECT id, title, artist, word FROM lyrics WHERE duration IS NULL")
    rows = cur.fetchall()

    for row in rows:
        song_id, title, artist, word_count = row
        print(f"Updating song: {title} by {artist}")

        spotify_data = get_spotify_data_with_fallback(sp, title, artist)
        
        if spotify_data:
            duration_str = spotify_data['duration']
            if duration_str != "Unknown":
                cur.execute('''
                    UPDATE lyrics
                    SET duration = ?
                    WHERE id = ?
                ''', (duration_str, song_id))
                print(f"Updated duration for {title}: {duration_str}")

                if word_count > 0:
                    duration_parts = duration_str.split(":")
                    minutes = int(duration_parts[0])
                    seconds = int(duration_parts[1])
                    total_duration_in_minutes = minutes + seconds / 60.0
                    wpm = word_count / total_duration_in_minutes if total_duration_in_minutes > 0 else 0

                    cur.execute('''
                        UPDATE lyrics
                        SET wpm = ?
                        WHERE id = ?
                    ''', (wpm, song_id))
                    print(f"Updated wpm for {title}: {wpm:.2f}")
                else:
                    print(f"Warning: No word count for {title}, skipping wpm calculation.")
            else:
                print(f"Could not update duration for {title}. Duration is 'Unknown'.")
        else:
            print(f"Warning: No Spotify data found for {title} by {artist}.")

    cur.connection.commit()

def delete_extra_rows(cur):
    cur.execute("DELETE FROM songs WHERE id > 100")
    cur.connection.commit()
    print("Deleted rows beyond ID 100 from 'songs' table.")

def reset_song_index(cur):
    cur.execute('''
        CREATE TABLE temp_songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            artist TEXT,
            popularity INTEGER
        );
    ''')
    cur.connection.commit()

    cur.execute('''
        INSERT INTO temp_songs (title, artist, popularity)
        SELECT title, artist, popularity FROM songs;
    ''')
    cur.connection.commit()

    cur.execute('DROP TABLE IF EXISTS songs')
    cur.connection.commit()

    cur.execute('ALTER TABLE temp_songs RENAME TO songs')
    cur.connection.commit()

    print("Reset index and updated 'songs' table.")

def main():
    cur, conn = set_up_database("songs.db")
    first_table()

    remove_columns_from_songs(cur)  
    
    artists, songs = combine_and_order(cur)  
    
    if not artists:  
        print("All 100 songs have already been added. No new songs to process.")
        conn.close()
        return

    reset_song_index(cur)

    sp = spotify_authenticate()  
    populate_lyrics_table_with_duration(cur, conn, artists, songs, sp)  
    
    update_duration_and_wpm(cur, sp)  

    conn.close()

if __name__ == "__main__":
    main()
import requests
import sqlite3
import os
import re
from bs4 import BeautifulSoup
import lyricsgenius
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from time import sleep

# Database setup
def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, db_name)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    return cur, conn

# Ensure the songs table exists (without 'spotify_id' and 'duration' columns)
def first_table():
    cur, conn = set_up_database("songs.db")
    cur.execute('''CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        title TEXT, 
        artist TEXT,
        popularity INTEGER
    )''')
    conn.commit()
    conn.close()

# Function to remove 'spotify_id' from the 'songs' table
def remove_columns_from_songs(cur):
    # Create a new temporary table without 'spotify_id' column
    cur.execute('''
        CREATE TABLE IF NOT EXISTS temp_songs AS
        SELECT id, title, artist, popularity
        FROM songs
    ''')
    cur.connection.commit()

    # Drop the old 'songs' table
    cur.execute('DROP TABLE IF EXISTS songs')
    cur.connection.commit()

    # Rename the 'temp_songs' table to 'songs'
    cur.execute('ALTER TABLE temp_songs RENAME TO songs')
    cur.connection.commit()

    print("'spotify_id' column removed from 'songs' table.")

def ensure_lyrics_table_structure(cur, conn):
    cur.execute('PRAGMA table_info(lyrics);')  # Get the structure of the lyrics table
    columns = [column[1] for column in cur.fetchall()]

    # Check if the 'duration' column exists, if not, add it
    if 'duration' not in columns:
        cur.execute('''
            ALTER TABLE lyrics ADD COLUMN duration TEXT;
        ''')
        conn.commit()
        print("'duration' column added to 'lyrics' table.")

# Normalize strings
def normalize_string(string):
    """Remove special characters and standardize formatting."""
    string = re.sub(r'[“”"\']', '', string).strip()  # Remove quotes
    string = re.sub(r'\(.*?\)', '', string)  # Remove anything in parentheses
    string = re.sub(r'feat\.|featuring', '', string, flags=re.IGNORECASE)  # Handle "feat." and "featuring"
    return string.lower()

# Scrape songs 51-100 from a static HTML file
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

# Scrape songs 1-50 from a live URL
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

# Combine songs and artists from both sources
def combine_and_order(cur):
    artists_50_1, songs_50_1 = scrape_from_live_url()
    artists_100_51, songs_100_51 = scrape_from_static()

    artists_combined = artists_50_1 + artists_100_51
    songs_combined = songs_50_1 + songs_100_51

    for i, (artist, song) in enumerate(zip(artists_combined, songs_combined), 1):
        # Check if the song already exists
        cur.execute('''
            SELECT id FROM songs WHERE title = ? AND artist = ?
        ''', (song, artist))
        existing_song = cur.fetchone()

        # If the song does not exist, insert it
        if existing_song is None:
            cur.execute('''
                INSERT INTO songs (id, title, artist)
                VALUES (?, ?, ?)
            ''', (i, song, artist))

    cur.connection.commit()
    return artists_combined, songs_combined

def clean_up_songs_table(cur):
    # Delete songs with IDs greater than 100 (keeping only the first 100 rows)
    cur.execute('''
        DELETE FROM songs WHERE id > 100
    ''')
    cur.connection.commit()
    print("Deleted rows beyond ID 100 from 'songs' table.")

# Spotify authentication
def spotify_authenticate():
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id='e1995ad727784f5abd056cc7e77d272c',
        client_secret='fa4a74248a304f39b1a7007c70f671d0',
        redirect_uri='http://localhost:8888/callback',
        scope='user-library-read'
    ))
    return sp

# Fetch Spotify data with a fallback mechanism
def get_spotify_data_with_fallback(sp, title, artist):
    sleep(0.5)  # Delay to avoid hitting rate limits
    query = f"track:{normalize_string(title)} artist:{normalize_string(artist)}"
    results = sp.search(q=query, type='track', limit=1)

    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        duration_ms = track.get('duration_ms', None)  # Safely get the duration_ms field
        if duration_ms:
            duration_sec = duration_ms / 1000  # Convert milliseconds to seconds
            minutes = int(duration_sec // 60)
            seconds = int(duration_sec % 60)
            duration = f"{minutes}:{seconds:02d}"  # Format as "m:ss"
            return {
                'spotify_id': track['id'],
                'popularity': track['popularity'],
                'album': track['album']['name'],
                'duration': duration  # Return the duration
            }
        else:
            print(f"Warning: No duration found for track: {title} by {artist}")
    else:
        print(f"Warning: No track found for query: {title} by {artist}")

    # Retry with title only if no results found with artist
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
    
# Populate the lyrics table with Spotify data and duration
def populate_lyrics_table_with_duration(cur, conn, artists, songs, sp):
    """Populate the lyrics table with song lyrics, word counts, and duration."""
    
    # Ensure that the 'lyrics' table has the 'duration' column
    ensure_lyrics_table_structure(cur, conn)
    
    # Genius API initialization
    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"  # Replace with your Genius API token
    genius = lyricsgenius.Genius(token, timeout=10)

    for i, (artist, song) in enumerate(zip(artists, songs)):
        print(f"Fetching lyrics for {song} by {artist}")
        
        # Fetch the Spotify data
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        
        if spotify_data:
            duration = spotify_data['duration']
        else:
            duration = "Unknown"  # If Spotify data is not found, default to "Unknown"
        
        # Fetch lyrics from Genius
        song_info = fetch_lyrics_with_retries(genius, song, artist)
        
        if song_info and song_info.lyrics:
            word_count = len(song_info.lyrics.split())

            # Insert the data into the 'lyrics' table
            cur.execute('''
                INSERT OR IGNORE INTO lyrics (id, title, artist, word, duration)
                VALUES (?, ?, ?, ?, ?)
            ''', (i + 1, song, artist, word_count, duration))
        else:
            print(f"Lyrics not found for {song} by {artist}")
        
    conn.commit()

def delete_extra_rows(cur):
    cur.execute("DELETE FROM songs WHERE id > 100")
    cur.connection.commit()
    print("Deleted rows beyond ID 100 from 'songs' table.")


# Main function to update the database
def main():
    cur, conn = set_up_database("songs.db")
    first_table()
    
    remove_columns_from_songs(cur)  # Remove 'spotify_id' from 'songs'
    
    delete_extra_rows(cur)  # Delete rows beyond ID 100
    
    artists, songs = combine_and_order(cur)  # Combine songs from both sources
    
    sp = spotify_authenticate()  # Authenticate with Spotify
    populate_lyrics_table_with_duration(cur, conn, artists, songs, sp)  # Populate lyrics and duration

    conn.close()

def fetch_lyrics_with_retries(genius, song, artist, max_retries=3):
    """Fetch lyrics from Genius with retries in case of failure."""
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
            sleep(2)  # Wait before retrying
    return None  # Return None if we exhaust retries

if __name__ == "__main__":
    main()

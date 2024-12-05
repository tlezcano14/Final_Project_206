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

# Ensure the songs table exists
def first_table():
    cur, conn = set_up_database("songs.db")
    cur.execute('''CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        title TEXT, 
        artist TEXT,
        spotify_id TEXT,
        popularity INTEGER,
        album TEXT
    )''')
    conn.commit()
    conn.close()

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
        cur.execute('''
            INSERT OR IGNORE INTO songs (id, title, artist)
            VALUES (?, ?, ?)
        ''', (i, song, artist))

    cur.connection.commit()

    return artists_combined, songs_combined

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
        return {
            'spotify_id': track['id'],
            'popularity': track['popularity'],
            'album': track['album']['name']
        }

    # Retry with title only
    print(f"Retrying with title only: {title}")
    query = f"track:{normalize_string(title)}"
    results = sp.search(q=query, type='track', limit=1)
    if results['tracks']['items']:
        track = results['tracks']['items'][0]
        return {
            'spotify_id': track['id'],
            'popularity': track['popularity'],
            'album': track['album']['name']
        }

    return None

# Populate Spotify data
def populate_spotify_data(cur, sp, artists, songs):
    for i, (artist, song) in enumerate(zip(artists, songs)):
        print(f"Fetching Spotify data for: {song} by {artist}")
        spotify_data = get_spotify_data_with_fallback(sp, song, artist)
        if spotify_data:
            cur.execute('''
                UPDATE songs
                SET spotify_id = ?, popularity = ?, album = ?
                WHERE id = ?
            ''', (spotify_data['spotify_id'], spotify_data['popularity'], spotify_data['album'], i + 1))
        else:
            print(f"Spotify data not found for: {song} by {artist}")
    cur.connection.commit()

# Fetch lyrics with retries
def fetch_lyrics_with_retries(genius, title, artist, max_retries=3):
    """Fetch lyrics with retries in case of timeouts or API failures."""
    retries = 0
    while retries < max_retries:
        try:
            song_info = genius.search_song(title, artist)
            return song_info
        except requests.exceptions.Timeout:
            retries += 1
            print(f"Retrying lyrics for {title} by {artist} (Attempt {retries}/{max_retries})...")
            sleep(2)
        except Exception as e:
            print(f"Error fetching lyrics for {title} by {artist}: {e}")
            return None
    print(f"Failed to fetch lyrics for {title} by {artist} after {max_retries} retries.")
    return None

# Populate the lyrics table
def populate_lyrics_table(cur, conn, artists, songs):
    """Populate the lyrics table with song lyrics and word counts."""
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lyrics (
            id INTEGER PRIMARY KEY,
            title TEXT,
            artist TEXT,
            word INTEGER
        )
    ''')
    conn.commit()

    # Genius API initialization
    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"  # Replace with your Genius API token
    genius = lyricsgenius.Genius(token, timeout=10)

    for i, (artist, song) in enumerate(zip(artists, songs)):
        print(f"Fetching lyrics for {song} by {artist}")
        song_info = fetch_lyrics_with_retries(genius, song, artist)
        if song_info and song_info.lyrics:
            word_count = len(song_info.lyrics.split())
            cur.execute('''
                INSERT OR IGNORE INTO lyrics (id, title, artist, word)
                VALUES (?, ?, ?, ?)
            ''', (i + 1, song, artist, word_count))
        else:
            print(f"Lyrics not found for {song} by {artist}")

    conn.commit()

# Main execution
if __name__ == "__main__":
    first_table()
    cur, conn = set_up_database("songs.db")

    # Combine songs from both static and live sources
    artists_combined, songs_combined = combine_and_order(cur)

    # Spotify API Authentication and Data Population
    sp = spotify_authenticate()
    populate_spotify_data(cur, sp, artists_combined, songs_combined)

    # Populate lyrics data
    populate_lyrics_table(cur, conn, artists_combined, songs_combined)

    conn.close()

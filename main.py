# Gabriel Oliveira, Thomas Lezcano, Renard Richmond
# Final Project 

import requests
import sqlite3
import json
import os
from bs4 import BeautifulSoup
import lyricsgenius


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
        artist TEXT)
    ''')

    conn.commit()
    conn.close()

first_table()

def scrape_from_static():
    d = {}
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
                    end_artists.append(artist)
                    end_songs.append(song)
    
    end_artists.reverse()
    end_songs.reverse()
# hay bitches
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
                    artists.append(artist)
                    songs.append(song)
    
    artists.reverse()
    songs.reverse()
    
    return artists, songs

def combine_and_order(cur):
    artists_50_1, songs_50_1 = scrape_from_live_url()
    artists_100_51, songs_100_51 = scrape_from_static()

    artists_combined = artists_50_1 + artists_100_51
    songs_combined = songs_50_1 + songs_100_51

    batch_size = 25
    num_batches = 4  

    for batch in range(num_batches):
        start_index = batch * batch_size
        end_index = start_index + batch_size

        artists_batch = artists_combined[start_index:end_index]
        songs_batch = songs_combined[start_index:end_index]

        for i in range(len(artists_batch)):
            title = songs_batch[i]
            artist = artists_batch[i]
            id = start_index + i + 1  

            cur.execute('''
                INSERT OR IGNORE INTO songs (id, title, artist)
                VALUES (?, ?, ?)
            ''', (id, title, artist))

        cur.connection.commit()

    return artists_combined, songs_combined

def lyrics(songs, artists): 
    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"
    genius = lyricsgenius.Genius(token)

    for x in range(len(songs)):
        song_info = genius.search_song(songs[x], artists[x])
        if song_info:
            print(f"Lyrics for {songs[x]} by {artists[x]}:\n")
            print(song_info.lyrics)
        else:
            print(f"Song '{songs[x]}' by {artists[x]} not found.")
            print()

cur, conn = set_up_database("songs.db")
artists_combined, songs_combined = combine_and_order(cur)
lyrics(songs_combined, artists_combined)


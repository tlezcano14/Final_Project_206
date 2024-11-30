# Gabriel Oliveira, Thomas Lezcano
# Final Project 

import requests
import json
from bs4 import BeautifulSoup
import lyricsgenius

def scrape_from_static():
    d = {}
    end_artists = []  # To store artist names
    end_songs = []    # To store song titles

    # Open the static HTML file that was saved earlier
    with open('static_page.html', 'r', encoding='utf-8') as file:
        static_html = file.read()

    # Parse the static HTML content using BeautifulSoup
    soup = BeautifulSoup(static_html, 'html.parser')

    # Find the titles in the saved static HTML (adjust this part based on the actual content structure)
    found_titles = soup.find_all("article", class_="pmc-fallback-list-item")

    # Print the number of titles found for debugging
    print("Number of titles found:", len(found_titles))

    # If titles are found, extract and print them
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

def combine_and_order():
    artists_50_1, songs_50_1 = scrape_from_live_url()
    artists_100_51, songs_100_51 = scrape_from_static()

    artists_combined = artists_50_1 + artists_100_51
    songs_combined = songs_50_1 + songs_100_51

    print("Songs ranked 1 to 100:")
    for i in range(len(artists_combined)):
        print(f"Rank {i+1}: {artists_combined[i]} - {songs_combined[i]}")

combine_and_order()


def lyrics(): 
    songs = {}
    token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"
    genius = lyricsgenius.Genius(token)

    for artist, song in d.items():  # Iterate through the dictionary items
        song_info = genius.search_song(artist, song)  # Pass song and artist to search_song

        if song_info:
            print(f"Lyrics for {song} by {artist}:\n")
            print(song_info.lyrics)
        else:
            print(f"Song '{song}' by {artist} not found.")
            print()

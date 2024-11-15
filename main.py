# Gabriel Oliveira, Thomas Lezcano
# Final Project 

import requests
import json
import lyricsgenius
token = "-SPqPS1Wq_0zYs_L-31AVu0N_7Bx2-UcEmFQZAYs0CQ5zEeQKq083QV-VK0zLNHt"
genius = lyricsgenius.Genius(token)

song = genius.search_song("Sicko Mode", "Travis Scott")

# Print the song's lyrics
if song:
    print(song.lyrics)
else:
    print("Song not found.")
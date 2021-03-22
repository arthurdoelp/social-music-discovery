import sys
from dotenv import load_dotenv
from pathlib import Path  # Python 3.6+ only
env_path = Path('../config/') / 'config.env'
load_dotenv(dotenv_path=env_path)
import json
import datetime
import os
from google.cloud import bigquery
from TikTokApi import TikTokApi
# import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

# print(os.environ.get("CHROMEDRIVER_PATH"))


def document_initialised(driver):
    return driver.execute_script("return initialised")

chrome_options = Options()
chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN") # Have to comment out this line if testing locally
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
# from selenium.webdriver import Safari
# Have to change the CHROMEDRIVER_PATH environmental variable to the local full path if testing locally
driver = webdriver.Chrome(
    options=chrome_options, executable_path=os.environ.get("CHROMEDRIVER_PATH"))

verifyFp = os.environ.get("VERIFYFP")

api = TikTokApi.get_instance(
    custom_verifyFp=verifyFp, use_test_endpoints=True, use_selenium=True)

client = bigquery.Client()

auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

print("Script is starting! Let's do this!!!")
# Pre-processing Part
# I need to delete all of the rows from the hashtag_tiktok_music_ids table before running the rest of the script.
# The reason I cannot do it right away at the end of each script is any data that was processed in the last 90 minutes
# or so cannot be changed until the period is complete (after 90 minutes). So I will run the delete query at the
# beginning of the script each day so it will be wellll beyond 90 minutes so I should not have an issue deleting the rows.

# delete_job = client.query(
#     """
# DELETE FROM social-music-discovery-307622.social_music_discovery.hashtag_tiktok_music_ids
# WHERE true """
# )

# delete_results = delete_job.result()

# print(delete_results)

# -----------------------------------------------------------------------
print("____________________________________________________________")
print("                                                            ")
# PART 1
print("PART 1 Beginning...")
print("                                                            ")
print("-------------------Hashtags and Music_IDs-------------------")
# create list of hashtags
# hashtags = ["newmusic", "music", "hiphop", "rap", "artist", "spotify", "producer", "rapper", "soundcloud", "musician", "beats", "singer", "musicproducer", "youtube", "trap", "dj",
#             "hiphopmusic", "unsignedartist", "songwriter", "musicvideo", "rapmusic", "rappers", "newmusicalert", "instamusic", "applemusic", "singersongwriter", "trapmusic", "newartist", "song",
#             "livemusic", "hiphopculture", "rock", "independentartist", "studio", "indiemusic", "newsong", "upcomingartist", "undergroundhiphop", "beatmaker", "pop", "musica", "newsingle", "indie",
#             "freestyle", "guitar", "newalbum", "musicproduction", "musicislife"]
# hashtags_short = ["newmusic", "musician", "unsignedartist", "songwriter", "newmusicalert", "singersongwriter", "newartist", "song", "independentartist", "upcomingartist", "newsingle"]
hashtags = ["newmusic", "newsingle"]

print("grabbing trending tiktoks")
tikkkytoks = api.trending()

for tiktok in tikkkytoks:
    print(tiktok['author']['uniqueId'])

print("grabbed trending tiktoks")

# Define the song ID List
hashtag_tiktok_music_ids = []

print("Grabbing tiktoks based on hashtags")
# find tiktoks based on the hashtags from the list
for hashtag in hashtags:
    hashtag_tiktoks = api.by_hashtag(hashtag, count=1, offset=0)

# grab only the song IDs from each of those tiktoks
    for hashtag_tiktok in hashtag_tiktoks:
        # create a list of song IDs
        hashtag_tiktok_music_ids += [
            {u"tiktok_music_id": hashtag_tiktok["music"]["id"]}]

# print(hashtag_tiktok_music_ids)
print("Grabbed the music ids from the tiktoks from each of the hashtags.")

# Add all ids to a table in Bigquery
# TODO(developer): Set table_id to the ID of table to append to.
table_id = "social-music-discovery-307622.social_music_discovery.hashtag_tiktok_music_ids"

# Make an API request.
errors = client.insert_rows_json(table_id, hashtag_tiktok_music_ids)
if errors == []:
    print("New rows have been added to the music_ids table in bigquery.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))

# merge and check to see which ids are new and add those new ones to the main list
# check the table in bigquery to see if the IDs in the new list match any of the IDs in the table
# If any IDs match, dont include the ID
# else add the new IDs to the table
# store the list of NEW song IDs into a table in bigquery
query_job = client.query(
    """
MERGE social-music-discovery-307622.social_music_discovery.hashtag_tiktok_music_main_ids T
USING social-music-discovery-307622.social_music_discovery.hashtag_tiktok_music_ids S
ON T.tiktok_music_id = S.tiktok_music_id
WHEN NOT MATCHED THEN
INSERT (tiktok_music_id) VALUES (tiktok_music_id)"""
)

results = query_job.result()  # Waits for job to complete.

if results != []:
    print("Tables have successfully merged and any new rows have been added to the main music_ids table in bigquery.")
else:
    print("Encountered errors while merging and inserting rows: {}".format(results))

print("PART 1 Completed!")
print("                                                            ")
print("____________________________________________________________")
print("                                                            ")
# ---------------------------------------------------------------------

# # PART 2
# # grab the full music object of each of those songs from the song ID table
# # query to get song IDs
print("PART 2 Beginning...")
print("                                                            ")
print("-------------------Creating Music Objects-------------------")

main_music_id_table_id = "social-music-discovery-307622.social_music_discovery.hashtag_tiktok_music_main_ids"
main_music_ids = []

table = client.get_table(main_music_id_table_id)  # Make an API request.
fields = table.schema[:1]  # First two columns.
rows = client.list_rows(main_music_id_table_id, selected_fields=fields)
format_string = "{!s:<16} " * len(rows.schema)
field_names = [field.name for field in rows.schema]
for row in rows:
    main_music_ids += [format_string.format(*row).strip()]

# print(main_music_ids)
print("Successfully grabbed the main music_ids from the table in bigquery.")

music_objects = []
music_object_list = []

# grab music objects based ont he list of music ids
for main_music_id in main_music_ids:
    music_object_dict = api.get_music_object_full(main_music_id)
    music_object_dict_copy = music_object_dict.copy()
    music_objects.append(music_object_dict_copy)

# loop function through list of music objects to grab song objects and grab only necessary data store in list of dictionaries
    for music_object in music_objects:
        music_object_list += [{
            "music_id": music_object["music"]['id'],
            "music_title": music_object["music"]["title"],
            "music_play_url": music_object["music"]["playUrl"],
            "music_author_name": music_object["music"]["authorName"],
            "music_duration": music_object["music"]["duration"],
            "music_album": music_object["music"]["album"],
            "music_user_id": music_object["author"]["id"],
            "music_user_name": music_object["author"]["uniqueId"],
            "music_video_count": music_object["stats"]["videoCount"],
            "spotify_music_play_count": 0,
            "spotify_music_song_name": "",
            "spotify_music_artist_name": "",
            "spotify_music_artist_url": "",
            "spotify_music_popularity": 0
        }]

print("Successfully stored all of the music_object data for each music_id into a list of dictionaries.")
# print(music_object_list)

print("PART 2 Completed!")
print("____________________________________________________________")
print("                                                            ")
# ---------------------------------------------------------------------

# PART 3
print("PART 3 Beginning...")
print("                                                            ")
print("-----------------Collecting Spotify Data--------------------")
# Go to Spotify api and grab song data
# append the data to the data from part 2
# playlists = sp.user_playlists('spotify')

# music_object_list = [{
#     "music_title": "billie jean",
#     "music_author_name": "Michael Jackson"
# }]

spotify_artist_url = ""
spotify_artist_popularity = ""
spotify_artist_results = ""
play_count = ""
song_name = ""
spotify_artist = ""
spotify_artist_url = ""

# data = {
#     'url': 'https://sf16-ies-music-va.tiktokcdn.com/obj/musically-maliva-obj/1631003807503397.mp3',
#     'return': 'spotify',
#     'api_token': 'b55e662c0a4b20a1029f21a7d2249fb1'
# }
# result = requests.post('https://api.audd.io/', data=data)
# print(result.text)

print("Beginning Spotify API requests and web scraping...")
for music_object in music_object_list:
    if music_object["music_title"] != "original sound":
        formatted_music_title = music_object["music_title"].replace(
            " ", "+")
        search = sp.search(q=formatted_music_title,
                            limit=1, offset=0, type="track")
        if len(search["tracks"]["items"][0]["album"]["artists"]) > 0:
            spotify_artist_popularity = search["tracks"]["items"][0]["popularity"]
            spotify_artist_results = search["tracks"]["items"][0]["album"]["artists"]
            for spotify_artist_result in spotify_artist_results:
                if music_object["music_author_name"] in spotify_artist_result["name"]:
                    spotify_artist = spotify_artist_result["name"]
                    spotify_artist_url = spotify_artist_result["external_urls"]["spotify"]
                    driver.get(spotify_artist_url)
                    WebDriverWait(driver, timeout=3).until(lambda d: d.find_element_by_class_name(
                        "d47b790d001ed769adcd9ddfc0e83acc-scss.f3fc214b257ae2f1d43d4c594a94497f-scss"))
                    play_count_element = driver.find_element_by_class_name(
                        'd47b790d001ed769adcd9ddfc0e83acc-scss.f3fc214b257ae2f1d43d4c594a94497f-scss')
                    play_count_text = play_count_element.get_attribute(
                        'innerText')
                    song_name_element = driver.find_element_by_class_name(
                        'da0bc4060bb1bdb4abb8e402916af32e-scss.standalone-ellipsis-one-line._8a9c5cc886805907de5073b8ebc3acd8-scss')
                    song_name = song_name_element.get_attribute(
                        'innerText')
                    driver.close()
                    play_count = play_count_text.replace(',', '')
                    # print(play_count)
                    # print(song_name)
                    music_object.update({
                        "spotify_music_play_count": play_count,
                        "spotify_music_song_name": song_name,
                        "spotify_music_artist_name": spotify_artist,
                        "spotify_music_artist_url": spotify_artist_url,
                        "spotify_music_popularity": spotify_artist_popularity
                    })
                    # print(music_object)
# print(music_object_list)
print("Spotify API and web scraping completed!")
print("                                                            ")
print("PART 3 Completed!")
print("____________________________________________________________")
print("                                                            ")
print(spotify_artist_url)
# ---------------------------------------------------------------------

# PART 4
print("PART 4 Beginning...")
print("                                                            ")
print("-------------Collecting Tiktok Music_ID Data----------------")
# get a list of 10 tiktoks associated with each of the song IDs
# use same list from above to search for tiktoks

music_tiktok_ids = []

print("Querying Tiktok api to collect Tiktok objects based on sound IDs...")
print("Filtering out just the tiktok_video_ids from each of the Tiktok objects.")
for main_music_id in main_music_ids:
    music_tiktoks = api.by_sound(main_music_id, count=1)

# loop through list to grab tiktok ids
# grab only the video IDs from each of those tiktoks
    for music_tiktok in music_tiktoks:
        # create a list of song IDs
        music_tiktok_ids += [{u"tiktok_id": music_tiktok["id"]}]

# create a list of tiktok ids
# print(music_tiktok_ids)
print("Created a list of Tiktok IDs from each of the Tiktok objects.")

# Add all ids to a table in Bigquery
music_tiktok_ref_table_id = "social-music-discovery-307622.social_music_discovery.music_tiktok_ref_ids"

print("Inserting rows of Tiktok IDs into the Tiktok ID Reference Table...")
# Make an API request.
music_tiktok_errors = client.insert_rows_json(
    music_tiktok_ref_table_id, music_tiktok_ids)
if music_tiktok_errors == []:
    print("New rows have been added to the Tiktok ID Reference Table in Bigquery.")
else:
    print("Encountered errors while inserting rows: {}".format(
        music_tiktok_errors))

# merge and check to see which ids are new and add those new ones to the main list
# Check the table in bigquery to see if the IDs in the new list match any of the IDs in the table
# If any IDs match, dont include the ID
# else add the new IDs tot he table
# store the list of NEW tiktok ids into a table in bigquery
print("Merging Tiktok ID Reference and Main Tables...")
merge_music_tiktoks_job = client.query(
    """
MERGE social-music-discovery-307622.social_music_discovery.music_tiktok_main_ids T
USING social-music-discovery-307622.social_music_discovery.music_tiktok_ref_ids S
ON T.tiktok_id = S.tiktok_id
WHEN NOT MATCHED THEN
INSERT (tiktok_id) VALUES (tiktok_id)"""
)

# Waits for job to complete.
music_tiktok_results = merge_music_tiktoks_job.result()

if music_tiktok_results != []:
    print("Tables have successfully merged and any new rows have been added to the Main Tiktok IDs Table in Bigquery.")
else:
    print("Encountered errors while merging and inserting rows: {}".format(
        music_tiktok_results))

print("                                                            ")
print("PART 4 Completed!")
print("____________________________________________________________")
print("                                                            ")
# ---------------------------------------------------------------------

# PART 5
print("PART 5 Beginning...")
print("                                                            ")
print("-------------------Compiling Tiktok Data--------------------")
# get the tiktok ids stored in the tiktok id table from bigquery
main_tiktok_id_table_id = "social-music-discovery-307622.social_music_discovery.music_tiktok_main_ids"
main_tiktok_ids = []

print("Grabbing Main Tiktok IDs from the table in Bigquery.")
# Make an API request.
tiktok_id_table = client.get_table(main_tiktok_id_table_id)
tiktok_id_fields = tiktok_id_table.schema[:1]  # First two columns.
tiktok_id_rows = client.list_rows(
    main_tiktok_id_table_id, selected_fields=tiktok_id_fields)
tiktok_id_format_string = "{!s:<16} " * len(tiktok_id_rows.schema)
tiktok_id_field_names = [field.name for field in tiktok_id_rows.schema]
for row in tiktok_id_rows:
    main_tiktok_ids += [tiktok_id_format_string.format(*row).strip()]

print("Successfully grabbed Main Tiktok IDs from table in Bigquery!")
# print(main_tiktok_ids)

# Using the list of tiktok ids, loop through the list and find all of the tiktok objects of those ids

tiktok_objects = []
tiktok_object_list = []

# grab tiktok objects based on the list of tiktok ids
# store all of the tiktok objects into a list of dictionaries
print("Beginning process of querying Tiktok API to collect Tiktok object data. This might take some time...")
for main_tiktok_id in main_tiktok_ids:
    tiktok_object_dict = api.get_tiktok_by_id(main_tiktok_id)
    tiktok_object_dict_copy = tiktok_object_dict.copy()
    tiktok_objects.append(tiktok_object_dict_copy)

# print(tiktok_objects)
# loop through list of tiktok objects to grab all necessary data and store in a list of dictionaries
    for tiktok_object in tiktok_objects:
        challenge_hashtags = []
        ref_hashtag_string = ""
        challenge_hashtags_count = 0
        challenge_hashtags_string = ""
        ref_hashtag_video_count = 0
        ref_hashtag_view_count = 0
        try:
            tiktok_object["itemInfo"]["itemStruct"]["challenges"]
            hashtag_objects = tiktok_object["itemInfo"]["itemStruct"]["challenges"]
            for hashtag_object in hashtag_objects:
                challenge_hashtags.append(hashtag_object["title"])
            ref_hashtag = set(challenge_hashtags) & set(hashtags)
            ref_hashtag_string = ''.join(ref_hashtag)
            challenge_hashtags_count = len(challenge_hashtags)
            challenge_hashtags_string = ", ".join(challenge_hashtags)
            ref_hashtag_object = api.get_hashtag_object(ref_hashtag_string)
            ref_hashtag_video_count = ref_hashtag_object["challengeInfo"]["stats"]["videoCount"]
            ref_hashtag_view_count = ref_hashtag_object["challengeInfo"]["stats"]["viewCount"]
        except KeyError:
            pass
        tiktok_object_music_id_list = []
        tiktok_object_music_id_list.append(
            tiktok_object["itemInfo"]["itemStruct"]["music"]["id"])
        ref_music_id = set(main_music_ids) & set(
            tiktok_object_music_id_list)
        ref_music_id_string = ''.join(ref_music_id)
        ref_music_object = []

        for item in music_object_list:
            if item["music_id"] == ref_music_id_string:
                ref_music_object += [item]
                break

        # print(ref_music_object)

# Then loop through the list of tiktok objects to grab all of the necessary tiktok data
# append the music object data to the respective tiktok records
        tiktok_object_list += [{
            "tiktok_id": tiktok_object["itemInfo"]["itemStruct"]["id"],
            "tiktok_video_description": tiktok_object["itemInfo"]["itemStruct"]["desc"],
            "tiktok_video_created_at": tiktok_object["itemInfo"]["itemStruct"]["createTime"],
            "tiktok_video_duration": tiktok_object["itemInfo"]["itemStruct"]["video"]["duration"],
            "tiktok_video_cover_image": tiktok_object["itemInfo"]["itemStruct"]["video"]["cover"],
            "tiktok_video_user_id": tiktok_object["itemInfo"]["itemStruct"]["author"]["id"],
            "tiktok_video_user_name": tiktok_object["itemInfo"]["itemStruct"]["author"]["uniqueId"],
            "tiktok_video_likes": tiktok_object["itemInfo"]["itemStruct"]["stats"]["diggCount"],
            "tiktok_video_shares": tiktok_object["itemInfo"]["itemStruct"]["stats"]["shareCount"],
            "tiktok_video_comments": tiktok_object["itemInfo"]["itemStruct"]["stats"]["commentCount"],
            "tiktok_video_views": tiktok_object["itemInfo"]["itemStruct"]["stats"]["playCount"],
            "tiktok_video_user_total_following": tiktok_object["itemInfo"]["itemStruct"]["authorStats"]["followingCount"],
            "tiktok_video_user_total_followers": tiktok_object["itemInfo"]["itemStruct"]["authorStats"]["followerCount"],
            "tiktok_video_user_total_likes": tiktok_object["itemInfo"]["itemStruct"]["authorStats"]["heartCount"],
            "tiktok_video_user_total_videos": tiktok_object["itemInfo"]["itemStruct"]["authorStats"]["videoCount"],
            "tiktok_video_ref_hashtag": ref_hashtag_string,
            "tiktok_video_hashtags_count": challenge_hashtags_count,
            "tiktok_video_hashtags": challenge_hashtags_string,
            "tiktok_video_ref_hashtag_total_videos": ref_hashtag_video_count,
            "tiktok_video_ref_hashtag_total_views": ref_hashtag_view_count,
            "tiktok_video_music_id": ref_music_object[0]["music_id"],
            "tiktok_video_music_title": ref_music_object[0]["music_title"],
            "tiktok_video_music_play_url": ref_music_object[0]["music_play_url"],
            "tiktok_video_music_author_name": ref_music_object[0]["music_author_name"],
            "tiktok_video_music_duration": ref_music_object[0]["music_duration"],
            "tiktok_video_music_album": ref_music_object[0]["music_album"],
            "tiktok_video_music_user_id": ref_music_object[0]["music_user_id"],
            "tiktok_video_music_user_name": ref_music_object[0]["music_user_name"],
            "tiktok_video_music_video_count": ref_music_object[0]["music_video_count"],
            "spotify_music_play_count": ref_music_object[0]["spotify_music_play_count"],
            "spotify_music_song_name": ref_music_object[0]["spotify_music_song_name"],
            "spotify_music_artist_name": ref_music_object[0]["spotify_music_artist_name"],
            "spotify_music_artist_url": ref_music_object[0]["spotify_music_artist_url"],
            "spotify_music_popularity": ref_music_object[0]["spotify_music_popularity"],
            "record_created_at": datetime.datetime.now().strftime('%Y-%m-%d')
        }]

# Now I should have one big list of aggrigated tiktok data on each video
# print("Here is the list of records:")
# print(tiktok_object_list)
print("The records have been successfully compiled!!! All that is left is to add the records to Bigquery!")
# store all of the tiktok data into a big table in bigquery
# Need to create the table in bigquery with all of the fields
main_social_media_data_table_id = "social-music-discovery-307622.social_music_discovery.raw_social_media_records"

print("Creating the records in Bigquery...")
# Make an API request.
# finished_records = client.insert_rows_json(
#     main_social_media_data_table_id, tiktok_object_list)
# print(finished_records)
# if finished_records != []:
#     print("All of the new rows have been added to the main table in Bigquery.")
# else:
#     print("Encountered errors while inserting rows: {}".format(finished_records))

print("____________________________________________________________")
print("                   Process Completed!                       ")
print("____________________________________________________________")

# TESTING TIKTOK API
# tiktoks = api.trending()

# for tiktok in tiktoks:
#     print(tiktok['author']['uniqueId'])

# tiktoks = api.by_hashtag("newmusic", count=1, offset=0)

# music = api.get_music_object_full("6867020900907158277")
# print(music)

# hashtag_object = api.get_hashtag_object("newsingle")
# print(hashtag_object)

sys.stdout.flush()

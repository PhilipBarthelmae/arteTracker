import requests
import argparse
from datetime import datetime, timedelta
import time
import csv
from zoneinfo import ZoneInfo
import os
import subprocess as sp
import shutil
from tqdm import tqdm
import isodate
import numpy as np
from logger import log

parser = argparse.ArgumentParser()
parser.add_argument("--api_key", help="api key for youtube api v3", required=True)
parser.add_argument("--channelID", help = "unique id of youtbe channel, provided by YT", required = True)
parser.add_argument("--mode", help = "Type of download, snapshot or viewTracker", required = True)
parser.add_argument("--trackingDuration", default = 21,  help = "time in days for which view_count, comment_count etc is tracked hourly", required=False)

args = parser.parse_args()

api_key = args.api_key
channelID = args.channelID
mode = args.mode
trackingDuration = timedelta(days = args.trackingDuration)

baseURL = "https://www.googleapis.com/youtube/v3"

def makeGetRequest(url, retries = 3, delay = 1, timeout = 5):

    for attempt in range(1,retries + 1):
        try:
            res = requests.get(url, timeout = timeout)
            res.raise_for_status()
            return res.json()
        
        except requests.exceptions.HTTPError as httperr:
            statusCode = httperr.response.status_code
            if attempt < retries:
                log.warning(f"Failed http request. http error {statusCode} \n Trying again in {delay}s.")
            else:
                log.error(f"Failed http request. http error {statusCode} \n Aborting request.")

        except requests.exceptions.RequestException as err:
            if attempt < retries:
                log.warning(f"Failed http request: \n {err} \n Trying again in {delay}s.")
                time.sleep(delay)
            else:
                log.error(f"Failed final attempt ({attempt}): \n {err}. \n Aborting request.")
                exit()
    

def get_uploads_playlist_id(channel_id):
    url = f'{baseURL}/channels?part=contentDetails&id={channelID}&key={api_key}'
    res = makeGetRequest(url)
    return res['items'][0]['contentDetails']['relatedPlaylists']['uploads']


def get_all_video_ids(playlist_id):
    video_ids = []
    url = f'{baseURL}/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&key={api_key}'
    
    while url:
        res = makeGetRequest(url)

        for item in res['items']:
            video_ids.append(item['contentDetails']['videoId'])

        token = res.get('nextPageToken')
        if token:
            url = f'{baseURL}/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&pageToken={token}&key={api_key}'
        else:
            break

    return video_ids

def get_video_metadata(video_ids):
    metadata = []
    now = datetime.now()
    timestamp = now.replace(microsecond=0).isoformat()

    for i in tqdm(range(0, len(video_ids), 50)):
        ids = ','.join(video_ids[i:i+50])
        url = f'{baseURL}/videos?part=snippet,statistics,contentDetails&id={ids}&key={api_key}'
        res = requests.get(url).json()
        
        for video in res['items']:
            snippet = video['snippet']
            stats = video.get('statistics', {})
            details = video.get('contentDetails',{})

            publishDate = snippet.get("publishedAt")
            #transform to german time
            dt_utc = datetime.strptime(publishDate, "%Y-%m-%dT%H:%M:%SZ")
            dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
            dt_berlin = dt_utc.astimezone(ZoneInfo("Europe/Berlin"))

            duration = details.get('duration')
            #parse to readale format in minutes
            duration = np.round(isodate.parse_duration(duration).total_seconds()/60,2)

            metadata.append({
                'datetime': timestamp,
                'title': snippet.get('title'),
                'videoID': video['id'],
                'url': f"https://www.youtube.com/watch?v={video['id']}",
                'publishedOn': dt_berlin,
                'description': snippet.get('description', ''),
                'viewCount': stats.get('viewCount'),
                'likeCount': stats.get('likeCount'),
                'commentCount': stats.get('commentCount'),
                'duration': duration
            })
            
        time.sleep(0.1)  # to avoid quota issues

    return metadata


def save_to_csv(metadata, filename, targetDir):
    keys = metadata[0].keys()
    csv_filename = os.path.join(targetDir, f"{filename}.csv")

    # Write the CSV file
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        writer.writerows(metadata)


def takeSnapshot():

    log.debug("Looking for Uploads playlist ID")
    playlist_id = get_uploads_playlist_id(channel_id=channelID)
    log.info(f"Found uploads playlist ID: {playlist_id}")

    log.debug(f"Looking for videos in playlist")
    video_ids = get_all_video_ids(playlist_id)
    log.info(f"Found {len(video_ids)} videos on ARTEde.")

    log.debug(f"Downloading metadata for videos")
    metadata = get_video_metadata(video_ids)
    log.info(f"Downloaded metadata for {len(metadata)} videos.")

    log.debug(f"Saving metadata")
    targetDir = "Data/Snapshots/"
    save_to_csv(metadata, f'{date}', targetDir)
    log.info(f"Saved metadata to {date}.csv")

def logSection(title):
    width = shutil.get_terminal_size().columns
    line = '-' * width
    print(f"\n{line}\n{title.center(width)}\n{line}\n")



def get_video_trackerdata(video_ids):

    now = datetime.now()
    timestamp = now.replace(microsecond=0).isoformat()
    now = now.replace(tzinfo=ZoneInfo("Europe/Berlin"))

    metadata = []
    stop = False

    for i in range(0, len(video_ids), 50):
        ids = ','.join(video_ids[i:i+50])
        url = f'{baseURL}/videos?part=statistics,snippet&id={ids}&key={api_key}'
        res = makeGetRequest(url)
        
        for video in res['items']:
            snippet = video['snippet']
            stats = video.get('statistics', {})

            #check if vid more than trackingDuration Old
            publishDate = snippet.get("publishedAt")
            #transform to german time
            dt_utc = datetime.strptime(publishDate, "%Y-%m-%dT%H:%M:%SZ")
            dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
            dt_berlin = dt_utc.astimezone(ZoneInfo("Europe/Berlin"))
            if dt_berlin < now - trackingDuration:
                stop = True
                break

            metadata.append({
                'datetime': timestamp,
                'videoID': video['id'],
                'viewCount': stats.get('viewCount'),
                'likeCount': stats.get('likeCount'),
                'commentCount': stats.get('commentCount')
            })

            #terminate loop if last video is over trackingDuration old
            #if video == res['items'][-1]:
                #publishDate = snippet.get("publishedAt")
                #transform to german time
                #dt_utc = datetime.strptime(publishDate, "%Y-%m-%dT%H:%M:%SZ")
                #dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
                #dt_berlin = dt_utc.astimezone(ZoneInfo("Europe/Berlin"))
                #if dt_berlin < now - trackingDuration:
                    #stop = True
        
        if stop: 
            break
        
        time.sleep(0.1)  # to avoid quota issues

    return metadata


def trackStats():

    log.debug("Looking for Uploads playlist ID")
    playlist_id = get_uploads_playlist_id(channel_id=channelID) 
    log.info(f"Found uploads playlist ID: {playlist_id}")

    log.debug(f"Looking for videos in playlist")
    video_ids = get_all_video_ids(playlist_id)
    log.info(f"Found {len(video_ids)} videos on ARTEde.")

    log.debug(f"Downloading metadata for videos")
    metadata = get_video_trackerdata(video_ids)
    log.info(f"Downloaded metadata for {len(metadata)} videos.")

    log.debug(f"Saving metadata")
    filename = "trackerUpdate_" + str(dt)
    targetDir = "Data/TrackerUpdates/"
    save_to_csv(metadata, filename, targetDir)
    log.info(f"Saved metadata to {filename}.csv")
    return filename



if __name__ == '__main__':

    date = datetime.today().date()
    dt = datetime.today().strftime("%d-%m-%Y_%H-%M-%S")

    if mode == "snapshot":

        logSection("Taking snapshot")
        takeSnapshot()
        sp.run(["python", "scripts/librarian.py", "--filename", str(date)], text=True)
        logSection("Snapshot Complete")

        
    elif mode == "tracker":
        logSection("Tracking stats")
        updateFilename = trackStats()
        sp.run(["python", "scripts/librarian.py", "--filename", updateFilename], text=True)
        logSection("Tracking Complete")
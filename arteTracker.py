import subprocess as sp
import pandas as pd
import time
from scripts.logger import log
import os
import schedule
from scripts.sizeTracker import trackSize

apiKey = "YOUR-YOUTUBE-DATA-API-V3-KEY-HERE"

with open("key.txt") as f:
    apiKey = f.read()

channelID = "UCLLibJTCy3sXjHLVaDimnpQ" #ARTEde


def takeSnaphot(apiKey, channelID):
    log.info("Taking snapshot")
    sp.run(["python", "scripts/downloader.py", "--api_key", apiKey, "--channelID", channelID, "--mode", "snapshot"], text=True)

def track(apiKey, channelID):
    log.info("Starting tracker")
    sp.run(["python", "scripts/downloader.py", "--api_key", apiKey, "--channelID", channelID, "--mode", "tracker"], text=True)

if __name__ == '__main__':
    #create folder strucutre

    log.info("arteTracker started successfully")

    if not os.path.exists("Data/Snapshots"):
        os.makedirs("Data/Snapshots", exist_ok = True)
        log.warning("Created Data/Snapshots directory")

    if not os.path.exists("Data/TrackerUpdates"):
        os.makedirs("Data/TrackerUpdates", exist_ok = True)
        log.warning("Created Data/TrackerUpdates directory")

    if not os.path.exists("Data/Snapshots/Archive"):
        os.makedirs("Data/Snapshots/Archive", exist_ok= True)
        log.warning("Created Data/Snapshots/Archive directory")

    #for debugging
    # Show all columns (no horizontal cut-off)
    pd.set_option('display.max_columns', None)
    # Prevent wrapping or truncating based on screen width
    pd.set_option('display.width', None)

    #schedule Tasks
    schedule.every().day.at("23:00").do(takeSnaphot, apiKey, channelID)
    schedule.every().day.at("23:30").do(trackSize)
    schedule.every().hour.at(":00").do(track, apiKey, channelID)
    
    while True:
        schedule.run_pending()
        time.sleep(60)
    

    

    
    





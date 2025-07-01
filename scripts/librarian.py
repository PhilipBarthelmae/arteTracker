import argparse
import csv
import pandas as pd
import os
from logger import log

parser = argparse.ArgumentParser()
parser.add_argument("--filename", help="name of file to be processed", required=True)

args = parser.parse_args()
filename = args.filename


def appendLibrary(df):

    df = df.drop(columns = ["datetime","viewCount", "likeCount","commentCount"])
    #check if library file exists
    if not os.path.exists("Data/library.csv"):
        df.to_csv("Data/library.csv", index = False)

    else:
        currentLib = pd.read_csv("Data/library.csv", parse_dates=["publishedOn"])
        newEntries = df[~df["videoID"].isin(currentLib["videoID"])]
        currentLib = pd.concat([newEntries, currentLib], ignore_index=True)
        currentLib.to_csv("Data/library.csv", index = False)

def appendTracker(update):
    tracker = pd.read_csv("tracker.zip")
    tracker = pd.concat([update, tracker], ignore_index= True)
    tracker.to_csv("tracker.zip", index=False)



if __name__ == "__main__":
    #for debugging
    # Show all columns (no horizontal cut-off)
    pd.set_option('display.max_columns', None)
    # Prevent wrapping or truncating based on screen width
    pd.set_option('display.width', None)

    #manage snapshot
    if os.path.exists(f"Data/Snapshots/{filename}.csv"):
        log.debug(f"Loading Snapshot")
        snapshot = pd.read_csv(f"Data/Snapshots/{filename}.csv", parse_dates=["publishedOn"])
        log.info(f"Loaded Snapshot")

        log.debug(f"Appending Library")
        appendLibrary(snapshot)
        snapshot.to_csv(f"Data/Snapshots/{filename}.zip", index = False)
        #os.remove(f"Data/Snapshots/{filename}.csv")
        log.info(f"Appended Library")

        trackerUpdate = snapshot.drop(columns = ["title", "url", "publishedOn", "description", "duration"])
        updateSize = len(trackerUpdate.index)

        #if tracker file does not exist
        if not os.path.exists("tracker.zip"):
            log.warning("Creating new tracker file")
            trackerUpdate.to_csv("tracker.zip", index = False)
            sizeTrackerUpdate = len(trackerUpdate.index)
            log.info(f"Created Tracker file with {sizeTrackerUpdate} videos")
        else:
            appendTracker(trackerUpdate)
            log.info(f"Appended {updateSize} videos to tracker with snapshot update")

        
        
    #manage tracking Update
    #if file is tracker update
    if os.path.exists(f"Data/TrackerUpdates/{filename}.csv"):
        file = f"Data/TrackerUpdates/{filename}.csv"

        #if tracker file does not exist
        if not os.path.exists("tracker.zip"):
            log.warning("Creating new tracker file")
            df = pd.read_csv(file)
            df.to_csv("tracker.zip", index = False)

        else:
            update = pd.read_csv(file)
            updateSize = len(update.index)
            appendTracker(update)
            log.info(f"Appended {updateSize} videos to tracker with update")
            #os.remove(file)



    



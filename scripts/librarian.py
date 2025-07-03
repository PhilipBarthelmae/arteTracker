import argparse
import pandas as pd
import os
from logger import log
from datetime import datetime, timedelta
import zipfile

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


def cleanupSnapshots():
    dir = "Data/Snapshots"
    archiveDir = os.path.join(dir, "Archive")
    today = datetime.today().date()

    filenames = os.listdir(dir)
    for f in filenames:
        if not f.endswith(".csv"):
            log.warning(f"Skipping non csv file in Data/Snapshots: \n {f}")
            continue
        try:
            date = datetime.strptime(f.replace(".csv",""), "%d-%m-%Y").date()
        except:
            log.warning(f"Skipping file with unexpected name format: \n {f}")
            continue

        if today - date > timedelta(days = 30):
            if date.day == 1: #archive
                try:
                    cwd = os.getcwd()
                    source = os.path.join(cwd, "Data/Snapshots/", f )
                    destination = os.path.join(cwd, archiveDir,f.replace(".csv",".zip"))
                    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.write(source, arcname = f)
                    os.remove(source)
                    log.info(f"Archivef file {f}.")
                except:
                    log.error(f"Error encountered in archiving {f}.")
                    continue
            else:
                try:
                    os.remove(os.path.join(dir,f))
                    log.info(f"Deleted old snapshot {f}")
                except Exception as e:
                    log.error(f"Failed to delete snapshot {f}: \n {e}")


def cleanupTrackerUpdates():
    dir = "Data/TrackerUpdates/"
    keepLatestN = 50

    files = []

    for filename in os.listdir(dir):
        if filename.startswith("trackerUpdate") and filename.endswith(".csv"):
            try:
                ts_str = "_".join(filename.split("_")[1:]).replace(".csv","")
                ts = datetime.strptime(ts_str,"%d-%m-%Y_%H-%M-%S")
                files.append((ts,filename))
            except:
                log.warning(f"Encountered trackerUpdate with unusual name: \n {filename}")
                continue #skip files with non standart names

    files.sort()

    if len(files) < keepLatestN:
        for ts, filename in files[:-keepLatestN]:
            os.remove(os.path.join(dir, filename))
            log.info(f"Removed {filename} as part of trackerUpdate cleanup")



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
        #snapshot.to_csv(f"Data/Snapshots/{filename}.zip", index = False)
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




    



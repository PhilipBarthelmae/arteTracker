import os
from datetime import datetime
import pandas as pd
import numpy as np

try:
    from scripts.logger import log
except:
    from logger import log
    

def getDirSize(path = '.'):
    totalSize = 0
    if os.path.isdir(path):
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    totalSize += os.path.getsize(fp)

    return totalSize

def formatSize(n):
    return format(np.round(n, 2), ".2f")

def trackSize():
    if os.path.exists(os.path.join("Data", "tracker.zip")):
        sizeTracker = os.path.getsize(os.path.join("Data","tracker.zip")) /1048576. #in MiB
    else: sizeTracker = 0
    sizeData = formatSize(getDirSize("Data") /1048576. )
    sizeSnapshots = formatSize(getDirSize(os.path.join("Data","Snapshots")) /1048576.)
    sizeTrackerUpdates = formatSize(getDirSize(os.path.join("Data","TrackerUpdates")) /1048576.)
    sizeArchive = formatSize(getDirSize(os.path.join("Data","Snapshots","Archive")) /1048576.)

    now = datetime.now()
    date = now.replace(microsecond=0).strftime("%d-%m-%Y")

    columns = ["date", "tracker","TrackerUpdates", "Data", "Snapshots", "Archive"]
    values =  [date, sizeTracker, sizeTrackerUpdates, sizeData, sizeSnapshots, sizeArchive]

    dic = dict(zip(columns, values))
    df = pd.DataFrame([dic])

    if not os.path.exists("Data/size.zip"):
        df.to_csv("Data/size.zip", index=False)
        log.warning("Creating size.zip")
    else:
        currentSize = pd.read_csv("Data/size.zip")
        currentSize = pd.concat([df,currentSize], ignore_index= True)
        currentSize.to_csv("Data/size.zip", index = False)
        log.info("Updated size of project")



if __name__ == "__main__":
    log.debug("Tracking size of project")
    trackSize()
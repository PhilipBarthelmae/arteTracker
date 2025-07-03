## Tracks the (sadly non persistent) library of the ARTEde YouTube channel.

Intended to be run on servers / 100% uptime.

Youtube Data API V3 key required.

### Creates Library.csv:
Contains title, videoID, url, publishedOn, description, duration of all uploaded vieos.


### Creates tracker.zip 
Zipped CSV that contains timestamp of data rerieval, videoID, viewCount, likeCount and commentCount of videos. 
Data is collected hourly for videos less than three weeks old. Daily after.

import os
import urllib
import hashlib
import sys
from pyspark.sql import SparkSession
""""
 Spark example
 digest release json file by using Spark SQL then extract tracks and generate tracks info like :
 
 track id, track title, genre
 
 all records are sorted by genre
"""

def main(fileNames):
    sparkSession = SparkSession.builder.appName('ExtractTracksFromRelease').master('local[3]').getOrCreate()
    for fileName in fileNames :
        base=os.path.basename(fileName)
        # print(base)
        filename_pre = os.path.splitext(base)[0]
        print("processing " + fileName)
        albums = sparkSession.read.json(fileName)
        albums.printSchema()
        albums.createOrReplaceTempView("albums")
        albumRecords = sparkSession.sql("SELECT title, artists.name, community.rating.count,  community.rating.average, tracklist.title as track_title, genres, artists.id, released, country, tracklist.duration, id FROM albums")
        albumRecords.show()
        # trackRdd = albumRecords.rdd.flatMap(extractTrack)
        trackRdd = albumRecords.rdd.flatMap(extractTrack)
        trackRdd.distinct().sortBy(lambda a: a[1]).map(lambda b: b[0]).coalesce(1).saveAsTextFile('/Users/Liang/project/Analytics2/data/tracks/'+filename_pre)
    # sparkSession.stop

def extractTrack(row) :
    message = hashlib.md5()
    tracks = []
    title = row.title
    # if row.name is not None:
    artistsName = ';'.join(row.name)
    genres = row.genres
    artistInfo = urllib.parse.quote_plus(artistsName)
    # print(artistsName)
    if row.track_title is not None:
        track_titles = row.track_title

        for track_title in track_titles :
            track_info = artistInfo + '|' + urllib.parse.quote_plus(track_title)
            if genres is not None:
                for genre in genres :
                    result = []
                    rowkey = '00-01-00100_00-01-00100_'+ track_info
                    # print(rowkey)
                    message.update(rowkey.encode('utf-8'))
                    rowkey_hashed = message.hexdigest()
                    # print(rowkey_hashed)
                    result.append(rowkey_hashed)
                    result.append(urllib.parse.quote_plus(track_title))
                    result.append(genre)
                    tuple = (','.join(result), genre)
                    # print(tuple)
                    tracks.append(tuple)
    return tracks

if __name__ == '__main__':

    main(sys.argv[1:])
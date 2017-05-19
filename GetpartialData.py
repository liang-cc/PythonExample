import sys
import urllib
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
"""
Demostrate how ot use spark to get partial data from text file
"""

def main(fileNames=None):

    genres = ['Classical','Electronic','Folk', 'Jazz', 'Pop', 'Blues', 'Hip Hop', 'Reggae', 'Latin', 'Stage & Screen', 'Rock', 'Funk / Soul']
    ss = SparkSession.builder.appName('GetparitalData').master('local[3]').getOrCreate()
    rdd = ss.sparkContext.textFile('/Users/Liang/project/Analytics2/data/tracks/54_67_108_219_validRelease/*')

    for genre in genres:
        genreRdd = rdd.filter(lambda line: genre == line.split(",")[2])
        sampleRdd = genreRdd.take(20001)
        with open('/Users/Liang/project/Analytics2/data/final_track/' + urllib.parse.quote_plus(genre)+ '.txt', 'w') as file_handler:
            for item in sampleRdd:
                file_handler.write("%s\n" % item.encode('utf-8'))
            file_handler.close()
    ss.stop
if __name__ == '__main__':

    main()
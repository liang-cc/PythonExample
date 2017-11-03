import requests
import subprocess
from bs4 import BeautifulSoup
import sys
from itertools import islice
import re
import time
import redis
import discovery_constants
import json
import logging


logFormatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                                 datefmt='%Y-%m-%d,%H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
server_log = logging.FileHandler('./log/updating_trending.log')
server_log.setFormatter(logFormatter)
logger.addHandler(server_log)


def downloadTrending() :
    result = {}
    r = requests.get(discovery_constants.url, stream=True,auth=(discovery_constants.username,discovery_constants.password))
    soup = BeautifulSoup(r.content, 'html.parser')
    for link in soup.find_all('a'):
        # print(link.get('href'))
        if "popularity" in link.get("href") or "itunes" in link.get("href") :
            print(link.get('href'))
            logger.info((link.get('href')))
            if "popularity" in link.get("href") :
                result["popularity"] = link.get('href')
            else :
                result["itunes"] = link.get('href')
    for component in result.values() :
        if "popularity" in component :
            popularity_url = discovery_constants.popularity_url.format(component)
            r = requests.get(popularity_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
            local_filename = popularity_url.split('/')[-1]
            with open(local_filename, "wb") as f:
                print("Downloading %s" % local_filename )
                logger.info("Downloading %s" % local_filename )
                total_length = r.headers.get('content-length')

                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in r.iter_content(chunk_size=4096*1024):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                        sys.stdout.flush()
                    print("Download " + local_filename + " done\n")
                    logger("Download " + local_filename + " done\n")
        if "itunes" in component:
            song_url = discovery_constants.song_url.format(component)
            r = requests.get(song_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
            local_filename = song_url.split('/')[-1]
            with open(local_filename, "wb") as f:
                print("Downloading %s" % local_filename)
                logger.info("Downloading %s" % local_filename)
                total_length = r.headers.get('content-length')

                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in r.iter_content(chunk_size=4096*1024*10):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                        sys.stdout.flush()
                    print("Download " + local_filename + " done\n")
                    logger.info("Download " + local_filename + " done\n")
            genre_url = discovery_constants.genre_url.format(component)
            r = requests.get(genre_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
            local_filename = genre_url.split('/')[-1]
            with open(local_filename, "wb") as f:
                print("Downloading %s" % local_filename)
                logger.info("Downloading %s" % local_filename)
                total_length = r.headers.get('content-length')

                if total_length is None:  # no content length header
                    f.write(r.content)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in r.iter_content(chunk_size=4096*1024):
                        dl += len(data)
                        f.write(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                        sys.stdout.flush()
                    print("Download " + local_filename + " done\n")
    return result

def unzipFile(filename) :
    cmd =  ['tar', '-xjf', filename]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in p.stdout:
        print(line)
    p.wait()
    print(p.returncode)


def loadData(popularity_track_meta_dict) :
    popularity_path = popularity_track_meta_dict["popularity"]
    itunes_path = popularity_track_meta_dict["itunes"]

    genreDict = loadGenre(itunes_path)
    populiarity_dict,track_genre_dic = loadPopularity(popularity_path, genreDict)

    return loadTrackMetaFromSong(itunes_path,populiarity_dict,track_genre_dic)

def loadGenre(itunes_path) :
    genreDict = {}
    counterGenre = 0
    with open("./"+itunes_path+'genre', 'rb') as f:
        content = islice(f, 34, None)

        for s in content:
            counterGenre += 1
            record_delimiter = chr(2) + '\n'
            s = str(s,'utf-8')
            s = re.sub(record_delimiter, '', s)
            columns = s.split(chr(1))
            # print(columns[1] + ':' + columns[3])
            if len(columns) == 4:
                if columns[3] in discovery_constants.genres_condidates:
                    genreDict.update({columns[1]: columns[3]})
    return genreDict

def loadPopularity(popularity_path,genreDict) :
    result = {}
    track_genre_dic = {}
    with open("./"+popularity_path+'song_popularity_per_genre') as f:
        for line in f:
            try:
                columns = line.split(chr(1))

                if len(columns) == 5 and discovery_constants.location == columns[1]:
                    genre = genreDict.get(columns[2])
                    if genre in discovery_constants.genres_condidates :
                        if (genre not in result.keys()) or (genre in result.keys() and len(result[genre]) < discovery_constants.max_tracks_per_genre):
                            result.setdefault(genre, {}).update(
                                {columns[3]: {'provider': 'iTunes', 'trackId': columns[3], 'genre': genre}})
                            if (columns[3] not in track_genre_dic.keys()) :
                                track_genre_dic.update({columns[3]:genre})
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    return result, track_genre_dic

def loadTrackMetaFromSong(itunes_path, populiarity_dict,track_genre_dic) :
    counter = 0
    with open("./" + itunes_path + 'song', 'rb') as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0 :
                    logger.info("{} records are read".format(counter))
                line = str(line, 'utf-8')
                columns = line.split(chr(1))
                if len(columns) == 16:
                    track_id = columns[1]
                    track_title = columns[2]
                    artist = columns[6]
                    album = columns[7]

                    if track_id in track_genre_dic:
                        genre = track_genre_dic[track_id]
                        track_dic = populiarity_dict[genre]
                        track_dic[track_id].update({'title': track_title, 'artist':artist, 'collection':album})
            except Exception:
                print(columns)
    logger.info("{} records are read".format(counter))
    return populiarity_dict


def write_to_redis(populiarity_dict):
    r = redis.Redis(
        host=discovery_constants.redis_host,
        port=discovery_constants.redis_port)
    for genre in discovery_constants.genres_condidates:
        if genre in populiarity_dict.keys() :
            track_ids_per_genre = []
            track_dic = populiarity_dict[genre]
            for track_id in track_dic.keys():
                track_ids_per_genre.append(track_id)
                #push track info to redis
                if not r.exists(discovery_constants.redis_itunes_track_prefix+":"+track_id) :
                    r.set(discovery_constants.redis_itunes_track_prefix+":"+track_id, json.dumps(track_dic[track_id]))
            #push er genre track ids to redis
            r.set(discovery_constants.redis_itunes_treding_prefix+":"+genre,",".join(track_ids_per_genre))

    # a = {"provider":"iTunes","trackId":"1262126920","genre":"Rock","title":"Rx","artist":"Theory of a Deadman","collection":"Wake Up Call"}
    # r.set(discovery_constants.redis_itunes_track_prefix+":xxxx", json.dumps(a))
    # print(json.dumps(a))
    # print(str(a))
    i = 0

def main():
    start_time = time.time()
    result  = downloadTrending()
    print("--- {} seconds --- for downloading the lastest files".format(time.time() - start_time))
    logger.info("--- {} seconds --- for downloading the lastest files".format(time.time() - start_time))
    # # result = {}
    # # result.update({"itunes": "itunes20171026/"})
    # # result.update({"popularity":"popularity20171026/"})


    print("starting unzipping song_popularity_per_genre.tbz")
    logger.info("starting unzipping song_popularity_per_genre.tbz")
    start_time = time.time()
    print(unzipFile("./song_popularity_per_genre.tbz"))
    print("--- {} seconds --- for unzipping song_popularity_per_genre.tbz".format(time.time() - start_time))
    logger.info("--- {} seconds --- for unzipping song_popularity_per_genre.tbz".format(time.time() - start_time))

    print("starting unzipping genre.tbz")
    logger.info("starting unzipping genre.tbz")
    start_time = time.time()
    print(unzipFile("./genre.tbz"))
    print("--- {} seconds --- for unzipping genre.tbz".format(time.time() - start_time))
    logger.info("--- {} seconds --- for unzipping genre.tbz".format(time.time() - start_time))

    print("starting unzipping song.tbz")
    logger.info("starting unzipping song.tbz")
    start_time = time.time()
    print(unzipFile("./song.tbz"))
    print("--- {} seconds --- for unzipping song.tbz".format(time.time() - start_time))
    logger.info("--- {} seconds --- for unzipping song.tbz".format(time.time() - start_time))


    # # {genre:{trackid:{trackinfo}}}
    print("parsing data:")
    logger.info("parsing data")
    populiarity_dict = loadData(result)

    print("pushing data to redis")
    logger.info("pushing data to redis")
    start_time = time.time()
    write_to_redis(populiarity_dict)
    print("--- {} seconds --- for pushing data to redis".format(time.time() - start_time))
    logger.info("--- {} seconds --- for pushing data to redis".format(time.time() - start_time))
    i  = 0
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    logger.info("---total %s seconds to finish all jobs---" % (time.time() - start_time))
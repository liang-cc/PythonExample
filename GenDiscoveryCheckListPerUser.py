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
    # for component in result.values() :
    #     if "popularity" in component :
    #         popularity_url = discovery_constants.popularity_url.format(component)
    #         r = requests.get(popularity_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
    #         local_filename = popularity_url.split('/')[-1]
    #         with open(local_filename, "wb") as f:
    #             print("Downloading %s" % local_filename )
    #             logger.info("Downloading %s" % local_filename )
    #             total_length = r.headers.get('content-length')
    #
    #             if total_length is None:  # no content length header
    #                 f.write(r.content)
    #             else:
    #                 dl = 0
    #                 total_length = int(total_length)
    #                 for data in r.iter_content(chunk_size=4096*1024):
    #                     dl += len(data)
    #                     f.write(data)
    #                     done = int(50 * dl / total_length)
    #                     sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
    #                     sys.stdout.flush()
    #                 print("Download " + local_filename + " done\n")
    #                 logger("Download " + local_filename + " done\n")
    #     if "itunes" in component:
    #         song_url = discovery_constants.song_url.format(component)
    #         r = requests.get(song_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
    #         local_filename = song_url.split('/')[-1]
    #         with open(local_filename, "wb") as f:
    #             print("Downloading %s" % local_filename)
    #             logger.info("Downloading %s" % local_filename)
    #             total_length = r.headers.get('content-length')
    #
    #             if total_length is None:  # no content length header
    #                 f.write(r.content)
    #             else:
    #                 dl = 0
    #                 total_length = int(total_length)
    #                 for data in r.iter_content(chunk_size=4096*1024*10):
    #                     dl += len(data)
    #                     f.write(data)
    #                     done = int(50 * dl / total_length)
    #                     sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
    #                     sys.stdout.flush()
    #                 print("Download " + local_filename + " done\n")
    #                 logger.info("Download " + local_filename + " done\n")
    #         genre_url = discovery_constants.genre_url.format(component)
    #         r = requests.get(genre_url, stream=True, auth=(discovery_constants.username, discovery_constants.password))
    #         local_filename = genre_url.split('/')[-1]
    #         with open(local_filename, "wb") as f:
    #             print("Downloading %s" % local_filename)
    #             logger.info("Downloading %s" % local_filename)
    #             total_length = r.headers.get('content-length')
    #
    #             if total_length is None:  # no content length header
    #                 f.write(r.content)
    #             else:
    #                 dl = 0
    #                 total_length = int(total_length)
    #                 for data in r.iter_content(chunk_size=4096*1024):
    #                     dl += len(data)
    #                     f.write(data)
    #                     done = int(50 * dl / total_length)
    #                     sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
    #                     sys.stdout.flush()
    #                 print("Download " + local_filename + " done\n")
    return result

def unzipFile(filename) :
    cmd =  ['tar', '-xjf', filename]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in p.stdout:
        print(line)
    p.wait()
    print(p.returncode)

def loadTrendingData(popularity_track_meta_dict) :

    popularity_path = popularity_track_meta_dict["popularity"]
    itunes_path = popularity_track_meta_dict["itunes"]

    logger.info("loading genre data")
    genreDict = loadGenre(itunes_path)
    logger.info("loading collection data")
    # collection_dict = loadCollection(itunes_path)
    collection_dict = {}
    logger.info("loading artist data")
    artist_dict = loadArtist(itunes_path)
    logger.info("loading track artist data")
    trackid_artistid_dict = loadArtistSong(itunes_path)
    logger.info("loading track collection data")
    # trackid_collectionid_dict = loadCollectionSong(itunes_path)
    trackid_collectionid_dict = {}
    logger.info("loading popularity data")
    populiarity_dict,track_genre_dic = loadPopularity(popularity_path, genreDict)

    logger.info("loading tracking data(Trending only")
    return loadTrackMetaFromSong(itunes_path,populiarity_dict,track_genre_dic,collection_dict, artist_dict,trackid_artistid_dict,trackid_collectionid_dict)

def loadGenre(itunes_path, all_genre=False) :
    genreDict = {}
    counterGenre = 0
    counter = 0
    with open("./"+itunes_path+'genre', 'rb') as f:
        content = islice(f, 34, None)
        for s in content:
            counter += 1
            if counter % 100000 == 0:
                logger.info("{} records are read".format(counter))
            counterGenre += 1
            record_delimiter = chr(2) + '\n'
            s = str(s,'utf-8')
            s = re.sub(record_delimiter, '', s)
            columns = s.split(chr(1))
            # print(columns[1] + ':' + columns[3])
            if len(columns) == 4:
                if all_genre :
                    genreDict.update({columns[1]: columns[3]})
                else :
                    if columns[3] in discovery_constants.genres_condidates:
                        genreDict.update({columns[1]: columns[3]})
    logger.info("{} records are read".format(counter))
    return genreDict

def loadArtist(itunes_path) :
    artistDict = {}
    counter = 0
    with open("./"+itunes_path+"artist", "rb") as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
                line = str(line, 'utf-8')
                columns = line.split(chr(1))
                if len(columns) == 6 :
                    artist_id = columns[1]
                    artist_name = columns[2]
                    artist_url = columns[4]
                    artistDict.setdefault(artist_id,{}).update({"artist_id":artist_id, "artist_url": artist_url, "artist_name":artist_name})
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    logger.info("{} records are read".format(counter))
    return artistDict

def loadCollection(itunes_path):
    collectionDict = {}
    counter = 0
    with open("./" + itunes_path + "collection", "rb") as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
                line = str(line, 'utf-8')
                columns = line.split(chr(1))
                if len(columns) == 18:
                    collection_id = columns[1]
                    collection_name = columns[2]
                    search_terms = columns[4]
                    release_date = columns[9]
                    artwork_url = columns[8]
                    collectionDict.setdefault(collection_id, {}).update(
                        {"collection_id": collection_id, "artwork_url": artwork_url, "search_terms": search_terms,"collection_name":collection_name,"release_date":release_date})
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    logger.info("{} records are read".format(counter))
    return collectionDict


def loadPopularity(popularity_path,genreDict) :
    result = {}
    track_genre_dic = {}
    counter = 0
    with open("./"+popularity_path+'song_popularity_per_genre') as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
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
    logger.info("{} records are read".format(counter))
    return result, track_genre_dic
def loadArtistSong(itunes_path) :
    trackid_artistid_dict = {}
    counter = 0
    with open("./" + itunes_path + 'artist_song', 'r') as f:

        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
                columns = line.split(chr(1))
                if len(columns) == 3:
                    track_id = columns[2].strip(chr(2)+ '\n')
                    artist_id = columns[1]
                    if track_id not in trackid_artistid_dict.keys():
                        trackid_artistid_dict.setdefault(track_id, set()).add(artist_id)
                    else :
                        trackid_artistid_dict[track_id].add(artist_id)
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    print(len(trackid_artistid_dict))
    logger.info("{} records are read".format(counter))
    return trackid_artistid_dict

def loadCollectionSong(itunes_path) :
    trackid_collectionid_dict = {}
    counter = 0
    with open("./" + itunes_path + 'collection_song', 'r') as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
                columns = line.split(chr(1))
                if (len(columns) == 6):
                    collection_id = columns[1]
                    track_id = columns[2]
                    if track_id not in trackid_collectionid_dict.keys():
                        trackid_collectionid_dict.setdefault(track_id, set()).add(collection_id)
                    else:
                        trackid_collectionid_dict[track_id].add(collection_id)
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    logger.info("{} records are read".format(counter))
    return trackid_collectionid_dict

def loadCollectionGenre(itunes_path):
    collectionid_genreid_dict = {}
    counter = 0
    with open("./" + itunes_path + 'genre_collection', 'r') as f:
        for line in f:
            try:
                counter += 1
                if counter % 100000 == 0:
                    logger.info("{} records are read".format(counter))
                columns = line.split(chr(1))
                if (len(columns) == 4):
                    genre_id = columns[1]
                    collection_id = columns[2]
                    if collection_id not in collectionid_genreid_dict.keys():
                        collectionid_genreid_dict.setdefault(collection_id, set()).add(genre_id)
                    else:
                        collectionid_genreid_dict[collection_id].add(genre_id)
            except Exception as e:
                print(e)
                print(columns)
                print(line)
    logger.info("{} records are read".format(counter))
    return collectionid_genreid_dict


def loadTrackMetaFromSong(itunes_path, populiarity_dict,track_genre_dic,collection_dict, artist_dict,trackid_artistid_dict,trackid_collectionid_dict) :
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
                        # remove the track if any part of title, artist, album is missing
                        if not track_title or not artist or not album:
                            track_dic.pop(track_id, None)
                        else:
                            collection_id = ""
                            artist_id = ""
                            if track_id in trackid_collectionid_dict.keys():
                                collection_ids = trackid_collectionid_dict[track_id]
                                for temp_collection_id in collection_ids :
                                    if temp_collection_id in collection_dict.keys():
                                        if collection_dict[temp_collection_id]["collection_name"] == album:
                                            collection_id = temp_collection_id
                                            break
                            if track_id in trackid_artistid_dict.keys():
                                artist_ids = trackid_artistid_dict[track_id]
                                for temp_artist_id in artist_ids:
                                    if temp_artist_id in artist_dict.keys():
                                        if artist_dict[temp_artist_id]["artist_name"] == artist:
                                            artist_id = temp_artist_id
                                            break
                            track_dic[track_id].update({'title': track_title, 'artist':artist, 'collection':album, "albumId": collection_id, "artistId": artist_id})
            except Exception:
                print(columns)
    logger.info("{} records are read".format(counter))
    return populiarity_dict

def write_treading_to_json(populiarity_dict) :
    result = {}
    for genre in discovery_constants.genres_condidates:
        if genre in populiarity_dict.keys():
            result.update({genre:populiarity_dict[genre]})
    with open("./trending.json", "w") as f:
        json.dump(result,f)

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

def main():
    start_time = time.time()
    result = downloadTrending()
    popularity_path = result["popularity"]
    itunes_path = result["itunes"]
    print("--- {} seconds --- for downloading the lastest files".format(time.time() - start_time))
    logger.info("--- {} seconds --- for downloading the lastest files".format(time.time() - start_time))
    action = sys.argv[1]
    if action.lower() == "download" :
        with open("./download_itunes.sh", "w") as f:
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.genre_url.format(
                                                                 itunes_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.popularity_url.format(
                                                                 popularity_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.genre_collection_url.format(
                                                                 itunes_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password, discovery_constants.artist_url.format(itunes_path))+"\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.collection_song_url.format(
                                                                 itunes_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.artist_song_url.format(
                                                                 itunes_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.collection_url.format(
                                                                 itunes_path)) + "\n")
            f.write("wget --user {} --password {} {}".format(discovery_constants.username, discovery_constants.password,
                                                             discovery_constants.song_url.format(
                                                                 popularity_path)) + "\n")


            f.write("tar -xjf genre.tbz\n")
            f.write("tar -xjf song_popularity_per_genre.tbz\n")
            f.write("tar -xjf genre_collection.tbz\n")
            f.write("tar -xjf artist.tbz\n")
            f.write("tar -xjf collection_song.tbz\n")
            f.write("tar -xjf artist_song.tbz\n")
            f.write("tar -xjf collection.tbz\n")
            f.write("tar -xjf song.tbz\n")
        cmd = ['chmod', '777', "./download_itunes.sh"]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in p.stdout:
            print(line)
        p.wait()
        print(p.returncode)
    elif action.lower() == "gen":
        # print("starting unzipping song_popularity_per_genre.tbz")
        # logger.info("starting unzipping song_popularity_per_genre.tbz")
        # start_time = time.time()
        # print(unzipFile("./song_popularity_per_genre.tbz"))
        # print("--- {} seconds --- for unzipping song_popularity_per_genre.tbz".format(time.time() - start_time))
        # logger.info("--- {} seconds --- for unzipping song_popularity_per_genre.tbz".format(time.time() - start_time))
        #
        # print("starting unzipping genre.tbz")
        # logger.info("starting unzipping genre.tbz")
        # start_time = time.time()
        # print(unzipFile("./genre.tbz"))
        # print("--- {} seconds --- for unzipping genre.tbz".format(time.time() - start_time))
        # logger.info("--- {} seconds --- for unzipping genre.tbz".format(time.time() - start_time))
        #
        # print("starting unzipping song.tbz")
        # logger.info("starting unzipping song.tbz")
        # start_time = time.time()
        # print(unzipFile("./song.tbz"))
        # print("--- {} seconds --- for unzipping song.tbz".format(time.time() - start_time))
        # logger.info("--- {} seconds --- for unzipping song.tbz".format(time.time() - start_time))


        # # {genre:{trackid:{trackinfo}}}
        print("parsing data:")
        logger.info("parsing data")
        populiarity_dict = loadTrendingData(discovery_constants.result)

        print("pushing data to redis")
        logger.info("pushing data to redis")
        start_time = time.time()
        write_to_redis(populiarity_dict)
        print("--- {} seconds --- for pushing data to redis".format(time.time() - start_time))
        logger.info("--- {} seconds --- for pushing data to redis".format(time.time() - start_time))

        # print("writting data to json")
        # logger.info("writting data to json")
        # start_time = time.time()
        # write_treading_to_json(populiarity_dict)
        # print("--- {} seconds --- for writting data to json".format(time.time() - start_time))
        # logger.info("--- {} seconds --- writting data to json".format(time.time() - start_time))


if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    logger.info("---total %s seconds to finish all jobs---" % (time.time() - start_time))
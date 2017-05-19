#!/usr/bin/env python3

import houndify
import sys
import json
import time
"""
 This is houndify client running on python3.6 
 There are three client_id/client_key can be used, and each pair has 100call/day lmit
 The purpose of this code is showing how to load bunch of query strings then extract some part of 
 the result to a json file
"""
if __name__ == '__main__':

  # CLIENT_ID = sys.argv[1]
  # CLIENT_KEY = sys.argv[2]
  # query = sys.argv[3]

  # from liang
  # CLIENT_ID = 'Tg5WORkRprgKTan1rBmKMA=='
  # CLIENT_KEY = 'yblYAMKJraraiUX2_zici2sir2FNjhdeHlxRa4tokG-o9yX82nrhTTyoLxik-CTzs48Xx85FkjDQXZhgdZ_FYg=='

  # from weiyang
  # CLIENT_ID = '6zTuUnIn91mEHl6vjkBYMw=='
  # CLIENT_KEY = '727Q5ZYVJYjuoN3kGw8OAkUrJev4VOAqkdFffnlfzdGJq_257y06RFwdIcgqAhoFdnrvi_87SZmTeY2yuzkeOQ=='

  #from lijun
  CLIENT_ID = 'Jedvhplz-zTaVzns6fWPGQ=='
  CLIENT_KEY = 'DWPUZaZ4a0j49OQoF7-MzPlIXwnq_WhQVnGF-jqDHdE5VHEHhC2Hzun-8f798-lwF-dZKuXwt8ES4VjZn01xHg=='


  requestInfo = {
    ## Pretend we're at SoundHound HQ.  Set other fields as appropriate
    'Latitude': 37.388309, 
    'Longitude': -121.973968
  }
  client = houndify.TextHoundClient(CLIENT_ID, CLIENT_KEY, "test_user", requestInfo)

  with open(sys.argv[1]) as f:
    lines = f.read().splitlines()
  f.close()
  index = 0
  with open('./play_full_result_0519.json',  'w') as outfile:
    outfile.write("[\n")
    for line in lines:
      print("")
      print("string for query is:" + line)
      response = client.query(line)
      response = json.loads(json.dumps(response))
      print('{0}: {1}'.format(index,response))
      index += 1


      #extract result
      data = {}
      data['searchTerm'] = response['Disambiguation']['ChoiceData'][0]['Transcription']
      print(data['searchTerm'])
      data['result'] = response['AllResults'][0]['SpokenResponseLong']
      if data['result'] != 'Didn\'t get that!':
        data['command'] = response['AllResults'][0]['CommandKind']
        if data['command'] == 'MusicCommand':
          if 'Tracks' in response['AllResults'][0]['NativeData']:
            data['TrackReturned'] = len(response['AllResults'][0]['NativeData']['Tracks'])
          if 'Albums' in response['AllResults'][0]['NativeData']:
            data['AlbumReturned'] = len(
              response['AllResults'][0]['NativeData']['Albums'])
        data['Domain'] = response['DomainUsage'][0]
      data['ConfidenceScore'] = response['Disambiguation']['ChoiceData'][0]['ConfidenceScore']
      print(data)
      json.dump(data, outfile, indent=4)
      outfile.write(",\n")
      print('sleep 1 seconds')
      time.sleep(1)
    outfile.write("]")
  outfile.close()
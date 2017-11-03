genres_condidates = ['Blues', "Children's Music", 'Classical', 'Country','Electronic', 'Folk','Hip-Hop',
'Jazz',  'Pop', 'Reggae', 'Rock', 'Orchestral', 'Piano','Rock & Roll', 'R&B/Soul', 'Soundtrack']

username = "epfuser200232"
password = "04e689835662b6cb1ed900d25482e7b2"
url = "https://feeds.itunes.apple.com/feeds/epf/v4/current/current/"
song_url = "https://feeds.itunes.apple.com/feeds/epf/v4/current/current/{}song.tbz"
popularity_url = "https://feeds.itunes.apple.com/feeds/epf/v4/current/current/{}song_popularity_per_genre.tbz"
genre_url = "https://feeds.itunes.apple.com/feeds/epf/v4/current/current/{}genre.tbz"
redis_host = "10.1.10.63"
redis_port = 6379

redis_itunes_treding_prefix = "itunesTrendingTracksByGenre"
redis_itunes_track_prefix = "itunesTrendingTracks"
redis_itunes_checklist_prefix = "mediaDiscoveryChecklist"

max_tracks_per_genre = 1000

location = '143441'  # 'USA'
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
import matplotlib.pyplot as plt
import pickle

SIZE=1000
MAX=1000

listening_events_file = 'data/listening_events.tsv.bz2'
tracks_file = 'data/tracks.tsv.bz2'

def read_listening_events(file_path):
    for i, chunk in enumerate(tqdm(pd.read_csv(file_path, sep='\t', compression='bz2', chunksize=SIZE, on_bad_lines='warn'), total=MAX, desc="Reading chunks")):
        yield chunk
        if i + 1 >= MAX:
            break

tracks = pd.read_csv(tracks_file, sep='\t', compression='bz2', on_bad_lines='warn')
print("Tracks data loaded.")

def calculate_track_popularity(file_path):
    track_popularity = defaultdict(int)
    for chunk in read_listening_events(file_path):
        for track_id in chunk['track_id']:
            track_popularity[track_id] += 1
    return track_popularity

track_popularity = calculate_track_popularity(listening_events_file)
track_popularity_df = pd.DataFrame(list(track_popularity.items()), columns=['track_id', 'popularity'])

def identify_unpopular_tracks(track_popularity_df, percentile=0.3):
    threshold = track_popularity_df['popularity'].quantile(percentile)
    unpopular_tracks = track_popularity_df[track_popularity_df['popularity'] <= threshold]['track_id']
    return set(unpopular_tracks)

unpopular_tracks = identify_unpopular_tracks(track_popularity_df)
# Save unpopular_tracks
with open('unpopular_tracks.pkl', 'wb') as f:
    pickle.dump(unpopular_tracks, f)

def identify_niche_users(file_path, unpopular_tracks, top_percentile=0.3):
    user_unpopular_listens = defaultdict(int)
    user_total_listens = defaultdict(int)
    
    for chunk in read_listening_events(file_path):
        for _, row in chunk.iterrows():
            user_id = row['user_id']
            track_id = row['track_id']
            if track_id in unpopular_tracks:
                user_unpopular_listens[user_id] += 1
            user_total_listens[user_id] += 1
    
    user_unpopular_ratio = {user_id: user_unpopular_listens[user_id] / user_total_listens[user_id] 
                            for user_id in user_unpopular_listens if user_total_listens[user_id] > 0}
    
    threshold = pd.Series(user_unpopular_ratio).quantile(1 - top_percentile)
    niche_users = [user_id for user_id, ratio in user_unpopular_ratio.items() if ratio >= threshold]
    
    return set(niche_users)

niche_users = identify_niche_users(listening_events_file, unpopular_tracks)
# Save niche_users
with open('niche_users.pkl', 'wb') as f:
    pickle.dump(niche_users, f)

def update_artist_popularity(chunk, tracks_df, niche_users, artist_popularity, niche_users_artist_popularity):
    chunk = chunk.merge(tracks_df[['track_id', 'artist']], on='track_id', how='left')
    for _, row in chunk.iterrows():
        artist = row['artist']
        timestamp = pd.to_datetime(row['timestamp'])
        year_month = timestamp.to_period('6M') 
        artist_popularity[artist][year_month] += 1
        if row['user_id'] in niche_users:
              niche_users_artist_popularity[artist][year_month] += 1

artist_popularity = defaultdict(lambda: defaultdict(int)) 
niche_users_artist_popularity = defaultdict(lambda: defaultdict(int)) 
for chunk in read_listening_events(listening_events_file):
    update_artist_popularity(chunk, tracks, niche_users, artist_popularity, niche_users_artist_popularity)

# Convert defaultdict to dict for serialization
artist_popularity_over_time = {artist: dict(popularity) for artist, popularity in artist_popularity.items()}
niche_users_artist_popularity_over_time = {artist: dict(popularity) for artist, popularity in niche_users_artist_popularity.items()}

# Save artist_popularity_over_time
with open('artist_popularity_over_time.pkl', 'wb') as f:
    pickle.dump(artist_popularity_over_time, f)

# Save niche_users_artist_popularity_over_time
with open('niche_users_artist_popularity_over_time.pkl', 'wb') as f:
    pickle.dump(niche_users_artist_popularity_over_time, f)

def identify_top_artists_increasing_popularity(artist_popularity_over_time):
    artist_trends = {}
    
    for artist, popularity in artist_popularity_over_time.items():
        sorted_popularity = pd.Series(popularity).sort_index()
        trend = sorted_popularity.diff().mean()  # Calculate average increase per 6-month interval
        artist_trends[artist] = trend
    
    top_artists = sorted(artist_trends, key=artist_trends.get, reverse=True)[:10]
    return top_artists

top_artists = identify_top_artists_increasing_popularity(artist_popularity_over_time)
# Save top_artists
with open('top_artists.pkl', 'wb') as f:
    pickle.dump(top_artists, f)

# def plot_artist_popularity_vs_niche_users(artist_popularity_over_time, niche_users_artist_popularity_over_time, top_artists):
#     fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15, 10), sharex=True)

#     for artist in top_artists:
#         total_listens = pd.Series(artist_popularity_over_time[artist]).sort_index()
        
#         niche_listens = pd.Series(niche_users_artist_popularity_over_time[artist]).sort_index()

#         total_listens.index = total_listens.index.to_timestamp()
#         niche_listens.index = niche_listens.index.to_timestamp()

#         axes[0].plot(total_listens.index, total_listens.values, label=artist)
#         axes[1].plot(niche_listens.index, niche_listens.values, label=artist)
    
#     axes[0].set_title('Total Listens Over Time')
#     axes[1].set_title('Listens by Niche Users Over Time')
#     axes[1].set_xlabel('Time')
#     axes[0].set_ylabel('Total Listens')
#     axes[1].set_ylabel('Niche User Listens')
#     axes[0].legend(loc='upper left', bbox_to_anchor=(1, 1))
#     axes[1].legend(loc='upper left', bbox_to_anchor=(1, 1))
    
#     plt.tight_layout()
#     plt.show()

# plot_artist_popularity_vs_niche_users(artist_popularity_over_time, niche_users_artist_popularity_over_time, top_artists)

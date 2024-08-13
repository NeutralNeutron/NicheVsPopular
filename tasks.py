import pandas as pd
from collections import defaultdict
from tqdm import tqdm

import pickle

SIZE=1000
MAX=1000

UAM = 'data/user_artist_by_month.tsv'


UAM= pd.read_csv(UAM, sep='\t', on_bad_lines='warn')

def identify_poi_artists(data, percentile=0.3):    
    artist_popularity = data.groupby('artist')['count'].sum()
    thresholdunpop = artist_popularity.quantile(percentile)
    thresholdpop = artist_popularity.quantile(1-percentile)
    unpopular_artists = artist_popularity[artist_popularity <= thresholdunpop].index
    popular_artists = artist_popularity[artist_popularity >= thresholdpop].index
    return set(unpopular_artists),set(popular_artists)

unpopular_artists,popular_artists = identify_poi_artists(UAM, percentile=0.3)

with open('unpopular_artists.pkl', 'wb') as f:
    pickle.dump(unpopular_artists, f)
    
with open('popular_artists.pkl', 'wb') as f:
    pickle.dump(popular_artists, f)



def find_niche_users(data, niche_threshold=0.3):
    data['is_unpopular'] = data['artist'].isin(unpopular_artists)
    user_total_listens = data.groupby('user_id')['count'].sum()
    user_unpopular_listens = data[data['is_unpopular']].groupby('user_id')['count'].sum()
    user_niche_proportion = user_unpopular_listens / user_total_listens
    user_niche_proportion = user_niche_proportion.fillna(0)
    threshold_value = user_niche_proportion.quantile(1 - niche_threshold)
    niche_users = user_niche_proportion[user_niche_proportion >= threshold_value].index
    
    return set(niche_users)


niche_users = find_niche_users(UAM, niche_threshold=0.3)
with open('niche_users.pkl', 'wb') as f:
    pickle.dump(niche_users, f)


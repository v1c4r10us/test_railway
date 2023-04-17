import pandas as pd
import pickle
from datetime import datetime

#Streaming Datasets
df_a=pd.read_csv('datasets/amazon_prime_titles.csv')
df_d=pd.read_csv('datasets/disney_plus_titles.csv')
df_h=pd.read_csv('datasets/hulu_titles.csv')
df_n=pd.read_csv('datasets/netflix_titles.csv')
df_rate=pd.read_csv('ratings/rating_global.csv')

#Functions
def transform(input_dataframe, idx_char):
    df=input_dataframe
    df.insert(1,'id',idx_char+df[['show_id']]) #Generating Id
    df['rating'].fillna('G', inplace=True) #Replacing NA of rating
    df['date_added']=df.date_added.str.strip() ##Fixing netflix problem
    df['date_added']=pd.to_datetime(df['date_added'], format='%B %d, %Y') #Formating dates
    df[['duration_int','duration_type']]=df.duration.str.split(" ", expand=True) #Splitting duration
    df['type']=df.type.str.lower() #Lowercase for text fields
    df['title']=df.title.str.lower()
    df['director']=df.director.str.lower()
    df['cast']=df['cast'].astype('str') ##Fixing hulu problem
    df['cast']=df.cast.str.lower()
    df['country']=df.country.str.lower()
    df['rating']=df.rating.str.lower()
    df['listed_in']=df.listed_in.str.lower()
    df['description']=df.description.str.lower()
    df['duration_type']=df.duration_type.str.lower()
    return df

def get_resume_rating(platform):
    platforms={'amazon':amazon, 'disney':disney, 'hulu': hulu, 'netflix':netflix} #Carga de datasets origen
    dr=df_rate #Carga del dataset promedio de los 11M > 500K de registros en rating
    dr=dr[['movieId', 'year', 'rating']] #Solo campos útiles
    dr=dr[dr['movieId'].str.startswith(platform[0])] #Reduccion a la plataforma específica
    dp=platforms[platform] #df de la plataforma
    dr=dr.set_index('movieId').join(dp[['id', 'type']].set_index('id'))
    dr=dr[dr['type']=='movie']
    return dr

#API Functions
def get_max_duration(year, platform, duration_type):
    df=platform
    rows=df[(df['type']=='movie')&(df['release_year']==year)&(df['duration_type']==duration_type)].sort_values(by='duration_int')
    if rows.shape[0]>0:
        resp=rows.iloc[0]['title']
    else:
        resp=None
    return resp

def get_score_count(platform, scored, year):
    qty=0 #Qty of movies in platform with scored at year
    df=rates[platform] #Select platform dataframe
    qty=df[(df['rating']>=scored)&(df['year']==year)].shape[0] #Count roww of movies with scored at year
    return qty

def get_count_platform(platform):
    df=platform
    rows=df[df['type']=='movie']
    return rows.shape[0]

def get_actor(platform, year):
    df=platform
    df=df[df['release_year']==year]
    actors=[y for x in df['cast'] for y in x.split(', ')]
    actors_w_count={x: actors.count(x) for x in actors}
    del actors_w_count['nan']
    if len(actors_w_count)>0:
        m=max(actors_w_count, key=actors_w_count.get)
    else:
        m=None
    return m

def prod_per_country(tipo,pais,anio):
    rows_a=amazon[(amazon['type']==tipo) & (amazon['release_year']==anio) & (amazon['country']==pais)].shape[0]
    rows_d=disney[(disney['type']==tipo) & (disney['release_year']==anio) & (disney['country']==pais)].shape[0]
    rows_h=hulu[(hulu['type']==tipo) & (hulu['release_year']==anio) & (hulu['country']==pais)].shape[0]
    rows_n=netflix[(netflix['type']==tipo) & (netflix['release_year']==anio) & (netflix['country']==pais)].shape[0]
    return {'pais': pais, 'anio': anio, tipo:rows_a+rows_d+rows_h+rows_n}

def get_contents(rating):
    rows_a=amazon[amazon['rating']==rating].shape[0]
    rows_d=disney[disney['rating']==rating].shape[0]
    rows_h=hulu[hulu['rating']==rating].shape[0]
    rows_n=netflix[netflix['rating']==rating].shape[0]
    return {rating: rows_a+rows_d+rows_h+rows_n}

#Initializing datasets
amazon=transform(df_a, 'a')
disney=transform(df_d, 'd')
hulu=transform(df_h, 'h')
netflix=transform(df_n, 'n')

rate_a=get_resume_rating('amazon')
rate_d=get_resume_rating('disney')
rate_h=get_resume_rating('hulu')
rate_n=get_resume_rating('netflix')
rates={'amazon': rate_a, 'disney': rate_d, 'hulu': rate_h, 'netflix':rate_n}

all_recommendations={}
with open('all_recommendations.pkl', 'rb') as f:
    all_recommendations=pickle.load(f) #Bulk of recommendations

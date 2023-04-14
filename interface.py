import pandas as pd
from datetime import datetime

#Streaming Datasets
df_a=pd.read_csv('https://drive.google.com/uc?id=1WJfCKOAp2UhWfXEBSXHtiIe4ShBw48pB')
df_d=pd.read_csv('https://drive.google.com/uc?id=187comTc0dz1aqGLSXQ5gLzY70RALoRpJ')
df_h=pd.read_csv('https://drive.google.com/uc?id=1z6v7Sx4wBkjEBSKz5cp2lMID2EYYIQY4')
df_n=pd.read_csv('https://drive.google.com/uc?id=1eArA5pc0zGgn1w2ujBiKkf1hSEf1_Fjk')

#Rating datasets
#df1=pd.read_csv('https://drive.google.com/uc?id=1RpQwvS0W8AqIsV8bP51iJzDwLSch_o1d')
#df2=pd.read_csv('https://drive.google.com/uc?id=1KELzh5ibLPv6J5OvFiCjCs3xBhtI2ILf')
#df3=pd.read_csv('https://drive.google.com/uc?id=1H-x_2SEIFHqtdrdev8jwBCzu28DKGNjO')
#df4=pd.read_csv('https://drive.google.com/uc?id=1Q9Yaf2O8pUn2_LQLSFYqPKn-iJBsJgog')
#df5=pd.read_csv('https://drive.google.com/uc?id=1-giwHEZExxJyaYIFls8POZBQ7VAa9luS')
#df6=pd.read_csv('https://drive.google.com/uc?id=1KdL6ZOalBpGcvqHf92l8omyGe_wib_Gx')
#df7=pd.read_csv('https://drive.google.com/uc?id=1xEAsohYePVW7oPm7mtQLXaPqW-PhBXZc')
#df8=pd.read_csv('https://drive.google.com/uc?id=1-kfhN2jPDGZVXOnswgUQa2031m3RUUiD')
#df_rate=[df1, df2, df3, df4, df5, df6, df7, df8]

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

def get_max_duration(year, platform, duration_type):
    df=platform
    rows=df[(df['type']=='movie')&(df['release_year']==year)&(df['duration_type']==duration_type)].sort_values(by='duration_int')
    if rows.shape[0]>0:
        resp=rows.iloc[0]['title']
    else:
        resp=None
    return resp

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

#Transformed datasets
amazon=transform(df_a, 'a')
disney=transform(df_d, 'd')
hulu=transform(df_h, 'h')
netflix=transform(df_n, 'n')

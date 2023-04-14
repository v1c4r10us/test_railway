from fastapi import FastAPI
import interface as elt
import json

app=FastAPI()
platforms={'amazon':elt.amazon, 'disney':elt.disney, 'hulu':elt.hulu, 'netflix':elt.netflix}

@app.get('/')
def get_df():
    return {'server': 'Railway running...'}

@app.get('/gmd/{platform}')
def get_max_duration(year:int, platform:str, duration_type:str):
    return {'movie':elt.get_max_duration(year, platforms[platform], duration_type)}

@app.get('/gcp/{platform}')
def get_count_platform(platform:str):
    return {'total_movies':elt.get_count_platform(platforms[platform])}

@app.get('/ga/{platform}')
def get_actor(platform:str, year:int):
    return {'actor': elt.get_actor(platforms[platform], year)}

@app.get('/ppc')
def prod_per_country(tipo:str, pais:str, anio:int):
    return elt.prod_per_country(tipo, pais, anio)

@app.get('/gc')
def get_contents(rating:str):
    return elt.get_contents(rating)

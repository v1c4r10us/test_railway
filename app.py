from fastapi import FastAPI
import interface as elt
import json

app=FastAPI()

@app.get('/')
def get_df():
    return elt.get_df()


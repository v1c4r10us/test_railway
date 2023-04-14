from fastapi import FastAPI
import interface as elt
import json

app=FastAPI()

@app.get('/')
def get_df():
    return json.loads(elt.get_df())


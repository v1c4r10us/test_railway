from fastapi import FastAPI
import json

app=FastAPI()

@app.get('/')
def hello():
    return {'server': 'Hello Friend!'}

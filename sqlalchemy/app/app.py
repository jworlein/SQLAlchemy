import datetime as dt
import numpy as np
import pandas as pd
from flask import Flask, jsonify
import json
from sqlHelper import SQLHelper

#######################################################

app = Flask(__name__)

sqlHelper = SQLHelper()

@app.route("/")
def home():
    print("Client requested the home page from the server")
    return("<h1>Welcome to my home page!</h1>")

@app.route("/api/v1/precipitation")
def get_precipitation():
    data = sqlHelper.get_precipitation()
    return jsonify(json.loads(data.to_json(orient="records")))

@app.route("/api/v1/stations")
def get_all_stations():
    data = sqlHelper.get_all_stations()
    return jsonify(json.loads(data.to_json(orient="records")))

@app.route("/api/v1/tobs")
def get_tobs_for_most_active():
    data = sqlHelper.get_tobs_for_most_active()
    return jsonify(json.loads(data.to_json(orient="records")))

# date must be in format YYYY-MM-DD
@app.route("/api/v1/<start_date>/<end_date>")
def get_temp_data_for_date_range(start_date, end_date):
    data = sqlHelper.get_temp_data_for_date_range(start_date, end_date)
    return jsonify(json.loads(data.to_json(orient="records")))

# date must be in format YYYY-MM-DD
@app.route("/api/v1/<start_date>")
def get_temp_data_for_date(start_date):
    data = sqlHelper.get_temp_data_for_date(start_date)
    return jsonify(json.loads(data.to_json(orient="records")))

if __name__ == "__main__":
    app.run(debug=True)
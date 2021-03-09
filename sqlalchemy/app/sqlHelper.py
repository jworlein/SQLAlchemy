from sqlalchemy import create_engine
import pandas as pd
import numpy as np

class SQLHelper():

    def __init__(self):
        self.connection_string = "sqlite:///data\\hawaii.sqlite"
        self.engine = create_engine(self.connection_string)

    def get_precipitation(self):
        query = ""
        with open("queries/lastYearPrecipitation.sql", "r") as f:
            query = f.read()

        conn = self.engine.connect()
        df = pd.read_sql(query, con=conn)
        conn.close()

        return df

    def get_all_stations(self):
        query = """
                SELECT
                    s.station,
                    s.name, 
                    s.latitude, 
                    s.longitude, 
                    s.elevation,
                    count(*) as tot_obs
                FROM
                    station s
                    JOIN measurement m on s.station = m.station
                GROUP BY
                    s.station, s.name, s.latitude, s.longitude, s.elevation
                ORDER BY
                    count(*) desc
                """

        conn = self.engine.connect()
        df = pd.read_sql(query, con=conn)
        conn.close()

        return df


    def get_tobs_for_most_active(self):

        query = ""
        with open("queries/lastYearTobsMostActive.sql", "r") as f:
            query = f.read()

        conn = self.engine.connect()
        df = pd.read_sql(query, con=conn)
        conn.close()

        return df

    # date must be in format YYYY-MM-DD
    def get_temp_data_for_date_range(self, start_date, end_date):
        query = f"""
                SELECT
                    min(tobs) as min_tobs,
                    max(tobs) as max_tobs,
                    avg(tobs) as avg_tobs
                FROM
                    measurement
                WHERE
                    date >= '{start_date}'
                    AND date <= '{end_date}'
                """ 
        
        conn = self.engine.connect()
        df = pd.read_sql(query, con=conn)
        conn.close()

        return df

    # date must be in format YYYY-MM-DD
    def get_temp_data_for_date(self, start_date):
        query = f"""
                SELECT
                    min(tobs) as min_tobs,
                    max(tobs) as max_tobs,
                    avg(tobs) as avg_tobs
                FROM
                    measurement
                WHERE
                    date = '{start_date}'
                """ 
        
        conn = self.engine.connect()
        df = pd.read_sql(query, con=conn)
        conn.close()

        return df
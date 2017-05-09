#!/usr/bin/env python
"""
NOW REDUNDANT
----
Processes a large CSV in chunks - here we map lat/lon to a NYC neighbourhood.
This was later optimised through Geohashing.
"""

def batch_process(chunksize=2000,
    #data_path = "../../data/raw/2015_Green_Taxi_Trip_Data.csv",
    data_path = "../../data/processed/shuffled.csv",
    output_path = "../../data/processed/processed.csv",
    shape_file = "../../data/external/boroughs.geojson"
    ):
    """
    Processes raw taxi data by:
        - Calculating neighbourhood via lat/lon and geoJSON shape file
        - Process datetime into standard format
        - Calculate trip time as new column "Trip_time"
    """
    import pandas as pd
    import json
    from shapely.geometry import Point, shape
    import sqlalchemy as sa

    def neighbourhood_finder(coords):
        lat = coords[0]
        lon = coords[1]
        point = Point(lat,lon) 
        for feature in neighbourhoods['features']:
            polygon = shape(feature['geometry'])
            if polygon.contains(point):
                #return feature["properties"]["neighborhood"]
                return feature["properties"]["borough"]
            
    neighbourhoods = json.load(open(shape_file))
    counter = 0
    #df = pd.read_csv(data_path,nrows=1000)
    for df in pd.read_csv(data_path,chunksize=chunksize):
        engine = sa.create_engine("sqlite:///database_big.db")
        con = engine.connect()
#        df["Pickup_neighbourhood"] = df.apply(
#            lambda x: neighbourhood_finder(
#                x["Pickup_longitude"],
#                x["Pickup_latitude"],
#                js),
#            axis=1)
        df["Pickup_coords"] =  list(zip(df["Pickup_longitude"], df["Pickup_latitude"]))
        df["Dropoff_coords"] = list(zip(df["Dropoff_longitude"], df["Dropoff_latitude"]))

        df["Pickup_neighbourhood"] = df["Pickup_coords"].apply(neighbourhood_finder)
        df["Dropoff_neighbourhood"] = df["Dropoff_coords"].apply(neighbourhood_finder)
   
        del df["Pickup_coords"]
        del df["Dropoff_coords"]

   #     df["Dropoff_neighbourhood"] = df.apply(
   #         lambda x: neighbourhood_finder(
   #             x["Dropoff_longitude"],
   #             x["Dropoff_latitude"],
   #             js),
   #         axis=1)
    
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], format='%m/%d/%Y %I:%M:%S %p')
        df["pickup_datetime"] =  pd.to_datetime(df["pickup_datetime"], format='%m/%d/%Y %I:%M:%S %p')
    
        df["Trip_time"] = df["dropoff_datetime"] - df["pickup_datetime"]
        df["Trip_time"] = df["Trip_time"] / pd.Timedelta('1 minute')
        
        df.to_sql(name="taxi_data",if_exists="append",con=con)
        #df.to_csv("test.csv")
        con.close()
        counter += 1
        print(chunksize*counter)
    return df


if __name__ == "__main__":
    batch_process()

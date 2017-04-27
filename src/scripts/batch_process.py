#!/usr/bin/env python
"""

"""

def batch_process(chunksize=100,
    #data_path = "../../data/raw/2015_Green_Taxi_Trip_Data.csv",
    data_path = "../../data/processed/shuffled.csv",
    output_path = "../../data/processed/processed.csv",
    shape_file = "../../data/external/nyc.geojson"
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

    def neighbourhood_finder(lat,lon,neighbourhoods):
        point = Point(lat,lon) 
        for feature in neighbourhoods['features']:
            polygon = shape(feature['geometry'])
            if polygon.contains(point):
                return feature["properties"]["neighborhood"]
            
    js = json.load(open(shape_file))
    con = sa.create_engine("sqlite:///database.db")

    for df in pd.read_csv(data_path,chunksize=chunksize):
        df["Pickup_neighbourhood"] = df.apply(
            lambda x: neighbourhood_finder(
                x["Pickup_longitude"],
                x["Pickup_latitude"],
                js),
            axis=1)
    
        df["Dropoff_neighbourhood"] = df.apply(
            lambda x: neighbourhood_finder(
                x["Dropoff_longitude"],
                x["Dropoff_latitude"],
                js),
            axis=1)
    
        df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
        df["pickup_datetime"] =  pd.to_datetime(df["pickup_datetime"])
    
        df["Trip_time"] = df["dropoff_datetime"] - df["pickup_datetime"]
        df["Trip_time"] = df["Trip_time"] / pd.Timedelta('1 minute')
        
        df.to_sql(name="taxi_data",if_exists="append",con=con)
    #df.to_csv("test.csv")

    return df


if __name__ == "__main__":
    batch_process()

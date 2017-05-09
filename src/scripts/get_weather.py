#!/usr/bin/env python
"""
Grabs historical weather data for a given date range from Weather Underground and dumps to a CSV file
"""

import csv
import datetime
import os
import requests 
import numpy as np
import time
 
# API key for Wunderground.com
api_key = "be9640aea2dcc0f7"
api_key = "27850cba6ce6bd77"

# Weather station ID - find on the website
station_ids = ["KNYC"] 
 
for station_id in station_ids:
    print("Fetching data for station ID: %s" % station_id)
    # initialise csv file
    with open('%s.csv' % station_id, 'w') as outfile:
        writer = csv.writer(outfile, delimiter=",")
        headers = ['date','hour', "temp", "rain","snow","precip"] # edit these as required
        writer.writerow(headers)
        
        # enter the first and last day required here
        start_date = datetime.date(2016,1,1)
        end_date = datetime.date(2016,12,31)
        date = start_date
        
        while date <= end_date:
            # format the date as YYYYMMDD
            date_string = date.strftime('%Y%m%d')
            #    build the url
            url = ("http://api.wunderground.com/api/%s/history_%s/q/%s.json" %
    			  (api_key, date_string, station_id))
            # make the request and parse json
            data = requests.get(url).json()
            #   build row
            for history in data['history']['observations']:
                try:
                    if float(history["tempm"]) > -50.0:
                        tmp = float(history['tempm'])
                    row = [date,
                        history["date"]["hour"],
                        tmp,
                        history["rain"],
                        history["snow"],
                        history["precipm"]
                    ]
                    row = [str(x) for x in row]
                    writer.writerow(row)
                except:
                    pass
            # increment the day by one
            print(date)
            date += datetime.timedelta(days=1)
            time.sleep(5)
print("Done!")

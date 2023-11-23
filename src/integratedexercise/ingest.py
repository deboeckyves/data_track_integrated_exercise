import sys
from time import sleep

import argparse
import requests
import logging
import json
import boto3


def process_raw_data(s3_bucket: str, date: str):
    pass

def ingest_data(env, date):
    

    a_response = requests.get('https://geo.irceline.be/sos/api/v1/stations')
    station_ids = list(map(lambda x: x['properties']['id'], a_response.json()))
    print(station_ids)

    s3 = boto3.resource('s3')
    for station_id in station_ids: 
        station_response = requests.get(f'https://geo.irceline.be/sos/api/v1/stations/{station_id}')
        timeseries_keys = station_response.json().get('properties').get('timeseries').keys()
        #print("--------------")
        #print(station_response.json().get('properties').get('timeseries'))
        #print("---------------")
        #print(f' station {station_id} has following timeseries {timeseries_keys}')

        headers = {'Content-Type': 'application/json'}
        body = {"timespan": "PT24h/2023-11-23TZ", "timeseries": list(timeseries_keys)}
        timeseries = requests.post(f'https://geo.irceline.be/sos/api/v1/timeseries/getData', data=json.dumps(body), headers=headers )
        #print(f' station {station_id} has following timeseries {timeseries.json()}')
        for timeserie_key in timeseries_keys:
            print('$$$$$$$$$$$$$$$$$$')
            print(station_response.json()['properties']['timeseries'][timeserie_key])
            print(timeseries.json()[timeserie_key]['values'])
            print('$$$$$$$$$$$$$$$$$$')
            station_response.json()['properties']['timeseries'][timeserie_key]['values'] = timeseries.json()[timeserie_key]['values']
            print(station_response.json()['properties']['timeseries'][timeserie_key])
            print('--------------------------')
        #object = s3.Object('data-track-integrated-exercise', f'yves-data/{date}/{station_id}.json')
        #object.put(Body=timeseries.json())



    # Method 1: Object.put()


    

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    parser = argparse.ArgumentParser(description="Building greeter")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="The environment in which we execute the code", required=True
    )
    args = parser.parse_args()
    logging.info(f"Using args: {args}")
    ingest_data(args.env, args.date)

if __name__ == "__main__":
    main()
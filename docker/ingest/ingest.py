import sys
from time import sleep

import argparse
import requests
import logging
import json
import boto3

s3 = boto3.resource('s3')

def process_raw_data_for_station(station: dict, date: str):
    station_id = station['properties']['id']
    station_response = requests.get(f'https://geo.irceline.be/sos/api/v1/stations/{station_id}').json()
    timeseries_keys = station_response.get('properties').get('timeseries').keys()
    headers = {'Content-Type': 'application/json'}
    body = {"timespan": f"PT24h/{date}TZ", "timeseries": list(timeseries_keys)}
    timeseries = requests.post(f'https://geo.irceline.be/sos/api/v1/timeseries/getData', data=json.dumps(body), headers=headers )    

    for timeserie_key in timeseries_keys:
        station_response['properties']['timeseries'][timeserie_key]['values'] = timeseries.json()[timeserie_key]['values']

    station['timeseries'] = station_response['properties']['timeseries']
    return station

def process_raw_data_for_station_v2(station: dict, date: str):
    station_id = station['properties']['id']
    station_response = requests.get(f'https://geo.irceline.be/sos/api/v1/stations/{station_id}').json()
    timeseries_keys = station_response.get('properties').get('timeseries').keys()
    headers = {'Content-Type': 'application/json'}
    body = {"timespan": f"PT24h/{date}TZ", "timeseries": list(timeseries_keys)}
    timeseries = requests.post(f'https://geo.irceline.be/sos/api/v1/timeseries/getData', data=json.dumps(body), headers=headers )

    station_records = []
    print(station)
    print("------------------------------------------")
    print(timeseries_keys)
    print('-------------------------------------------')
    print(timeseries.json())
    for timeserie_key in timeseries_keys:
        station_response['properties']['timeseries'][timeserie_key]['values'] = timeseries.json()[timeserie_key]['values']
        for value in timeseries.json()[timeserie_key]['values']:
            result_day_station_metric = value
            result_day_station_metric['station_id'] = station_id
            result_day_station_metric['station_label'] = station['properties']['label']
            result_day_station_metric['station_geometry_coordinates_x'] = station['geometry']['coordinates'][0]
            result_day_station_metric['station_geometry_coordinates_y'] = station['geometry']['coordinates'][1]
            result_day_station_metric['station_geometry_coordinates_z'] = station['geometry']['coordinates'][2]
            result_day_station_metric['station_geometry_type'] = station['geometry']['type']
            result_day_station_metric['station_type'] = station['type']
            result_day_station_metric['timeseries_id'] = timeserie_key
            result_day_station_metric['service_id'] = station_response['properties']['timeseries'][timeserie_key]['service']['id']
            result_day_station_metric['service_label'] = station_response['properties']['timeseries'][timeserie_key]['service']['label']
            result_day_station_metric['offering_id'] = station_response['properties']['timeseries'][timeserie_key]['offering']['id']
            result_day_station_metric['offering_label'] = station_response['properties']['timeseries'][timeserie_key]['offering']['label']
            result_day_station_metric['feature_id'] = station_response['properties']['timeseries'][timeserie_key]['feature']['id']
            result_day_station_metric['feature_label'] = station_response['properties']['timeseries'][timeserie_key]['feature']['label']
            result_day_station_metric['procedure_id'] = station_response['properties']['timeseries'][timeserie_key]['procedure']['id']
            result_day_station_metric['procedure_label'] = station_response['properties']['timeseries'][timeserie_key]['procedure']['label']
            result_day_station_metric['phenomenon_id'] = station_response['properties']['timeseries'][timeserie_key]['phenomenon']['id']
            result_day_station_metric['phenomenon_label'] = station_response['properties']['timeseries'][timeserie_key]['phenomenon']['label']
            result_day_station_metric['category_id'] = station_response['properties']['timeseries'][timeserie_key]['category']['id']
            result_day_station_metric['category_label'] = station_response['properties']['timeseries'][timeserie_key]['category']['label']
            station_records.append(result_day_station_metric)

    return station_records

def write_json_to_s3(s3_bucket: str, key: str, target_json: dict):
    object = s3.Object(s3_bucket, key)
    object.put(Body=(bytes(json.dumps(target_json).encode('UTF-8'))))

def ingest_data(env, date):
    all_stations_response = requests.get('https://geo.irceline.be/sos/api/v1/stations')
    for station in all_stations_response.json(): 
        # station = process_raw_data_for_station(station, date)
        station_id = station['properties']['id']
        station_records = process_raw_data_for_station_v2(station, date)
        #write_json_to_s3('data-track-integrated-exercise', f'yves-data/{date}/{station_id}.json', station)
        write_json_to_s3('data-track-integrated-exercise', f'yves-data-v2/{date}/{station_id}.json', station_records)

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
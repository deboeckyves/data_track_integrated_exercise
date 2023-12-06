import sys

import argparse
import requests
import logging
import json
import boto3

S3_FOLDER = "yves-data-v2"
S3_BUCKET = 'data-track-integrated-exercise'

IRCELINE_BASE_URL = 'https://geo.irceline.be/sos/api/v1'

s3 = boto3.resource('s3')

def process_raw_data_for_station(station: dict, date: str):
    station_id = station['properties']['id']
    station_timeseries_metadata = get_station_timeseries_metadata(station_id)
    timeseries_keys = extract_timeseries_keys_from_metadata(station_timeseries_metadata)
    timeseries = get_timeseries_data(date, timeseries_keys)
    station_records = flatten_and_combine_station_data(station, station_timeseries_metadata, timeseries)

    return station_records


def flatten_and_combine_station_data(station, station_timeseries_metadata, timeseries):
    station_records = []
    for timeserie_key in timeseries.json().keys():
        station_timeseries_metadata['properties']['timeseries'][timeserie_key]['values'] = timeseries.json()[timeserie_key]['values']
        for value in timeseries.json()[timeserie_key]['values']:
            station_record = value
            station_record['station_id'] = station['properties']['id']
            station_record['station_label'] = station['properties']['label']
            station_record['station_geometry_coordinates_x'] = station['geometry']['coordinates'][0]
            station_record['station_geometry_coordinates_y'] = station['geometry']['coordinates'][1]
            station_record['station_geometry_coordinates_z'] = station['geometry']['coordinates'][2]
            station_record['station_geometry_type'] = station['geometry']['type']
            station_record['station_type'] = station['type']
            station_record['timeseries_id'] = timeserie_key
            station_record['service_id'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['service'][
                'id']
            station_record['service_label'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['service'][
                'label']
            station_record['offering_id'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['offering'][
                'id']
            station_record['offering_label'] = \
                station_timeseries_metadata['properties']['timeseries'][timeserie_key]['offering']['label']
            station_record['feature_id'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['feature'][
                'id']
            station_record['feature_label'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['feature'][
                'label']
            station_record['procedure_id'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['procedure'][
                'id']
            station_record['procedure_label'] = \
                station_timeseries_metadata['properties']['timeseries'][timeserie_key]['procedure']['label']
            station_record['phenomenon_id'] = \
                station_timeseries_metadata['properties']['timeseries'][timeserie_key]['phenomenon']['id']
            station_record['phenomenon_label'] = \
                station_timeseries_metadata['properties']['timeseries'][timeserie_key]['phenomenon']['label']
            station_record['category_id'] = station_timeseries_metadata['properties']['timeseries'][timeserie_key]['category'][
                'id']
            station_record['category_label'] = \
                station_timeseries_metadata['properties']['timeseries'][timeserie_key]['category']['label']
            station_records.append(station_record)
    return station_records


def get_timeseries_data(date, timeseries_keys):
    headers = {'Content-Type': 'application/json'}
    body = {"timespan": f"PT24h/{date}TZ", "timeseries": list(timeseries_keys)}
    timeseries = requests.post(f'{IRCELINE_BASE_URL}/timeseries/getData', data=json.dumps(body), headers=headers)
    return timeseries


def extract_timeseries_keys_from_metadata(station_timeseries_metadata):
    return station_timeseries_metadata.get('properties').get('timeseries').keys()


def get_station_timeseries_metadata(station_id):
    return requests.get(f'{IRCELINE_BASE_URL}/stations/{station_id}').json()


def write_json_to_s3(s3_bucket: str, key: str, target_json: dict):
    object = s3.Object(s3_bucket, key)
    object.put(Body=(bytes(json.dumps(target_json).encode('UTF-8'))))


def ingest_data(date):
    all_stations_response = requests.get(f'{IRCELINE_BASE_URL}/stations')
    for station in all_stations_response.json():
        station_id = station['properties']['id']
        station_records = process_raw_data_for_station(station, date)
        write_json_to_s3(S3_BUCKET, f'{S3_FOLDER}/{date}/{station_id}.json', station_records)


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
    ingest_data(args.date)


if __name__ == "__main__":
    main()

import argparse
import logging
import sys
import boto3
import pyspark.sql.functions as F
import requests
# from geopy.geocoders import Nominatim

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

s3 = boto3.resource('s3')

spark = SparkSession.builder.config(
    "spark.jars.packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.1",
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()



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

    bucket = "data-track-integrated-exercise"
    folder = f"yves-data-v2/{args.date}"
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(bucket)
    files_in_s3 = [f.key.split(folder + "/")[1] for f in s3_bucket.objects.filter(Prefix=folder).all()]
    counter = 0

    for file in files_in_s3:
        data_station_df = spark.read.option("multiline", "true").json(
            f"s3a://data-track-integrated-exercise/yves-data-v2/{args.date}/{file}")

        data_station_df.printSchema()
        if not data_station_df:
            continue

        try:
            df = transform(data_station_df)
            df.printSchema()
            break
            write_parquet_to_s3(df, args.date, file)
        except Exception as err:
            print(f"Exception for file {file}: {err}")
            counter += 1

    print(f"data transformed for date {args.date}, encountered {counter} errors")


def write_parquet_to_s3(df, date, file):
    df.write.mode("overwrite").partitionBy("phenomenon_id").parquet(
        f"s3a://data-track-integrated-exercise/yves-data-v2/clean/aggregate_station_by_day/{date}/{file}")


def transform(df):
    # timezone: we assume it's CET for now since our only producer is located in belgium. If we were to expand to stations in other timezones
    # we would the timezone information as a parameter. One solution would be to enforce a "timezone" column in the schema saved to s3.
    df = add_datetime_column(df)
    df = add_avg_column(df)
    df = add_city_column(df)
    return df


def add_city_column(df):
    df = df.withColumn("station_city", get_city(df["station_geometry_coordinates_x"], df["station_geometry_coordinates_y"]))
    return df


def add_avg_column(df):
    df_average = df.groupBy("phenomenon_id").agg(F.avg("value").alias("avg_day"))
    df1 = df.alias('df1')
    df2 = df_average.alias('df2')
    df = df1.join(df2, df1.phenomenon_id == df2.phenomenon_id, "left").select('df1.*', 'df2.avg_day')
    return df


def add_datetime_column(df):
    df = df.withColumn("datetime",
                       F.to_utc_timestamp(F.from_unixtime(F.col("timestamp") / 1000, 'yyyy-MM-dd HH:mm:ss'),
                                          'CET'))
    return df


@F.udf(StringType())
def get_city(x, y):
    # geolocator = Nominatim(user_agent="my_app")
    # location = geolocator.reverse(f"{x}, {y}", exactly_one=True)
    # address = location.raw['address']
    url = f'https://nominatim.openstreetmap.org/reverse?lat={y}&lon={x}&format=json&accept-language=en'
    try:
        result = requests.get(url=url)
        result_json = result.json()
        address = result_json['address']
        if 'city' in address:
            city = address['city']
        elif 'city_district' in address:
            city = address['city_district']
        elif 'town' in address:
            city = address['town']
        else:
            city = 'unknown'
        return city
    except:
        return None

    return city


if __name__ == "__main__":
    main()

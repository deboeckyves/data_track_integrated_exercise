import argparse
import logging
import sys
import boto3
import pyspark.sql.functions as F
import requests
import time
from pyspark.sql import SparkSession


S3_PATH_PREFIX = 's3a'

S3_SOURCE_FOLDER_NAME = 'yves-data-v2'
S3_TARGET_FOLDER_NAME = f'{S3_SOURCE_FOLDER_NAME}/clean/aggregate_station_by_day'
S3_BUCKET_NAME = "data-track-integrated-exercise"
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


def time_usage_factory(func_name):
    def time_usage(func):
        def wrapper(*args, **kwargs):
            beg_ts = time.time()
            retval = func(*args, **kwargs)
            end_ts = time.time()
            logging.info(f"{func_name} elapsed time: {end_ts - beg_ts}")
            return retval

        return wrapper

    return time_usage


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

    folder = f"{S3_SOURCE_FOLDER_NAME}/{args.date}"
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket(S3_BUCKET_NAME)
    files_in_s3 = [f.key.split(folder + "/")[1] for f in s3_bucket.objects.filter(Prefix=folder).all()]
    counter = 0

    for file in files_in_s3:
        data_station_df = read_json_from_s3(args, file)

        if not data_station_df:
            continue

        try:
            df = transform(data_station_df)
            write_parquet_to_s3(df, args.date, file)
        except Exception as err:
            logging.error(f"Exception for file {file}: {err}")
            counter += 1

    logging.info(f"data transformed for date {args.date}, encountered {counter} errors")


@time_usage_factory("read_json_from_s3")
def read_json_from_s3(args, file):
    return spark.read.option("multiline", "true").json(
        f"{S3_PATH_PREFIX}://{S3_BUCKET_NAME}/{S3_SOURCE_FOLDER_NAME}/{args.date}/{file}")


@time_usage_factory("write_parquet_to_s3")
def write_parquet_to_s3(df, date, file):
    df.write.mode("overwrite").partitionBy("phenomenon_id").parquet(
        f"{S3_PATH_PREFIX}://{S3_BUCKET_NAME}/{S3_TARGET_FOLDER_NAME}/{date}/{file.split('.')[0]}")


@time_usage_factory("transform")
def transform(df):
    # timezone: we assume it's CET for now since our only producer is located in belgium. If we were to expand to stations in other timezones
    # we would the timezone information as a parameter. One solution would be to enforce a "timezone" column in the schema saved to s3.
    df = add_datetime_column(df).cache()
    df = add_avg_column(df)
    df = add_city_column(df)
    return df


def add_city_column(df):
    x = df.first().station_geometry_coordinates_x
    y = df.first().station_geometry_coordinates_y
    city = get_city(x, y)
    df = df.withColumn("station_city", F.lit(city))
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


# @F.udf(StringType())
# it is possible to implement this as an udf but this will have a very big impact on performance if not properly cached
# since the value is the same for each record of the same station, we can just call this function once and add a constant column
# because there is no built-in pyspark udf caching functionality, i decided to go for the latter
def get_city(x, y):
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

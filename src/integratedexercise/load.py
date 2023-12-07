import json
import argparse
import logging
import sys
import boto3
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession

from pyspark.sql import SparkSession


s3 = boto3.resource('s3')

#scala version = 2.12.18
#spark version = 3.5.0

spark = SparkSession.builder.config(
    "spark.jars.packages",
    ",".join(
        [
            "org.apache.hadoop:hadoop-aws:3.3.1",
            "net.snowflake:spark-snowflake_2.12:2.5.4-spark_2.4",
            "net.snowflake:snowflake-jdbc:3.14.3"
        ]
    ),
).config(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
).getOrCreate()

from botocore.exceptions import ClientError

def get_secret(secret_name: str):

    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])

def get_snowflake_creds_from_sm(secret_name: str):
    creds = get_secret(secret_name)
    return {
        "sfURL": f"{creds['URL']}",
        "sfPassword": creds["PASSWORD"],
        "sfUser": creds["USER_NAME"],
        "sfDatabase": creds["DATABASE"],
        "sfWarehouse": creds["WAREHOUSE"],
        'sfSchema': creds['SCHEMA'],
        "sfRole": creds["ROLE"]
    }

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
    folder = f"yves-data-v2/clean/aggregate_station_by_day/{args.date}"

    s3_bucket = s3.Bucket(bucket)

    files = set()
    files_in_s3 = [f.key.split(folder + "/")[1].split('/')[0] for f in s3_bucket.objects.filter(Prefix=folder).all()]
    files.update(files_in_s3)

    df_ppm_all_stations = spark.read.parquet(
        f"s3a://data-track-integrated-exercise/yves-data-v2/clean/aggregate_station_by_day/{args.date}/1030/phenomenon_id=5")

    for file in files:
        try:
            df = spark.read.parquet(
                f"s3a://data-track-integrated-exercise/yves-data-v2/clean/aggregate_station_by_day/{args.date}/{file}/phenomenon_id=5")
            df_ppm_all_stations = df_ppm_all_stations.union(df)
        except Exception as err:
            print(f"Exception fetching ppm data for date {args.date} and file {file}, possibly because this station does not have ppm data: {err}")

    # df_ppm_all_stations.show()
    # df_ppm_all_stations.show(n=5, truncate=False, vertical=True)

    sfOptions = get_snowflake_creds_from_sm("snowflake/integrated-exercise/yves-login")
    table_name = f"PPM_ALL_STATIONS_{args.date.replace('-', '_')}"
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, f"""CREATE TABLE IF NOT EXISTS {table_name} (
        category_id VARCHAR,
        category_label VARCHAR,
        feature_id VARCHAR,
        feature_label VARCHAR,
        offering_id VARCHAR,
        offering_label VARCHAR,
        phenomenon_label VARCHAR,
        procedure_id VARCHAR,
        procedure_label VARCHAR,
        service_id VARCHAR,
        service_label VARCHAR,
        station_geometry_coordinates_x DOUBLE,
        station_geometry_coordinates_y DOUBLE,
        station_geometry_coordinates_z VARCHAR,
        station_geometry_type VARCHAR,
        station_id INTEGER,
        station_label VARCHAR,
        station_type VARCHAR,
        timeseries_id VARCHAR,
        timestamp INTEGER,
        value DOUBLE,
        datetime TIMESTAMP,
        avg_day DOUBLE,
        city VARCHAR)""")

    df_ppm_all_stations.write.format("snowflake").options(**sfOptions).option("dbtable", f"{table_name}").mode("overwrite").save()

    static_table_name = "PPM_LAST_DAY"
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, f"""CREATE TABLE IF NOT EXISTS {static_table_name} (
        category_id VARCHAR,
        category_label VARCHAR,
        feature_id VARCHAR,
        feature_label VARCHAR,
        offering_id VARCHAR,
        offering_label VARCHAR,
        phenomenon_label VARCHAR,
        procedure_id VARCHAR,
        procedure_label VARCHAR,
        service_id VARCHAR,
        service_label VARCHAR,
        station_geometry_coordinates_x DOUBLE,
        station_geometry_coordinates_y DOUBLE,
        station_geometry_coordinates_z VARCHAR,
        station_geometry_type VARCHAR,
        station_id INTEGER,
        station_label VARCHAR,
        station_type VARCHAR,
        timeseries_id VARCHAR,
        timestamp INTEGER,
        value DOUBLE,
        datetime TIMESTAMP,
        avg_day DOUBLE,
        city VARCHAR)""")

    df_ppm_all_stations.write.format("snowflake").options(**sfOptions).option("dbtable", f"{static_table_name}").mode("overwrite").save()

    static_table_name = "PPM_ALL"
    spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, f"""CREATE TABLE IF NOT EXISTS {static_table_name} (
        category_id VARCHAR,
        category_label VARCHAR,
        feature_id VARCHAR,
        feature_label VARCHAR,
        offering_id VARCHAR,
        offering_label VARCHAR,
        phenomenon_label VARCHAR,
        procedure_id VARCHAR,
        procedure_label VARCHAR,
        service_id VARCHAR,
        service_label VARCHAR,
        station_geometry_coordinates_x DOUBLE,
        station_geometry_coordinates_y DOUBLE,
        station_geometry_coordinates_z VARCHAR,
        station_geometry_type VARCHAR,
        station_id INTEGER,
        station_label VARCHAR,
        station_type VARCHAR,
        timeseries_id VARCHAR,
        timestamp INTEGER,
        value DOUBLE,
        datetime TIMESTAMP,
        avg_day DOUBLE,
        city VARCHAR)""")

    df_ppm_all_stations.write.format("snowflake").options(**sfOptions).option("dbtable", f"{static_table_name}").mode("append").save()

if __name__ == "__main__":
    main()



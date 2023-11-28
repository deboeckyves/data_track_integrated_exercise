import argparse
import logging
import sys
import boto3
import pyspark.sql.functions as F

from pyspark.sql import SparkSession

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

def write_parquet_to_s3(s3_bucket: str, key: str, target_df):
    target_df.write.parquet("s3a://sparkbyexamples/parquet/people.parquet")

def read_json_from_s3(s3_bucket: str, key: str, target_json: dict):
    multiline_df = spark.read.option("multiline", "true").json("s3a://sparkbyexamples/json/multiline-zipcode.json")
    multiline_df.show()

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

    df = spark.read.option("multiline", "true").json("s3a://data-track-integrated-exercise/yves-data/2023-11-23/1030.json")
    df.show()
    df.printSchema()

    df.select("geometry", "properties", F.explode_outer("timeseries")).show()

    df.show()

    #df.write.parquet("s3a://data-track-integrated-exercise/yves-data/clean/aggregate_station_by_day/2023-11-23/1030.json")




if __name__ == "__main__":
    main()

import json
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
            "net.snowflake:spark-snowflake_2.13:2.13.0-spark_3.4",
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

    # files = set()
    # files_in_s3 = [f.key.split(folder + "/")[1].split('/')[0] for f in s3_bucket.objects.filter(Prefix=folder).all()]
    # files.update(files_in_s3)
    #
    # counter = 0
    # print(files)
    # df_ppm_all_stations = None
    # for i, file in enumerate(files):
    #     if i > 2:
    #         break
    #     if i == 0:
    #         df_ppm_all_stations = spark.read.parquet(
    #             f"s3a://data-track-integrated-exercise/yves-data-v2/clean/aggregate_station_by_day/{args.date}/{file}/phenomenon_id=5")
    #     else:
    #         df = spark.read.parquet(
    #             f"s3a://data-track-integrated-exercise/yves-data-v2/clean/aggregate_station_by_day/{args.date}/{file}/phenomenon_id=5")
    #         df_ppm_all_stations.union(df)
    #
    #
    # df_ppm_all_stations.show()
    # df_ppm_all_stations.show(n=5, truncate=False, vertical=True)

    # read full table
    df = spark.read.format("snowflake") \
            .options(**get_snowflake_creds_from_sm("snowflake/integrated-exercise/yves-login")) \
        .option("dbtable", "AXXES_YVES.customer") \
        .load()

    df.show()

if __name__ == "__main__":
    main()



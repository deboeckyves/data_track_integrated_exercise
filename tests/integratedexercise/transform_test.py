import datetime
import pytest
import logging

from src.integratedexercise.transform import add_city_column, add_datetime_column, add_avg_column, transform
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StringType, IntegerType, StructType, LongType, DoubleType, TimestampType


def test_transform_add_avg_column(spark):
    df_input_fields = [
        StructField("phenomenon_id", StringType(), True),
        StructField("value", DoubleType(), True),
    ]

    df_output_fields = [
        StructField("phenomenon_id", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("avg_day", DoubleType(), True),

    ]
    df_input = spark.createDataFrame([
        ('1', 15.0),
        ('1', 25.0),
        ('2', 3.14),
        ('2', 2.72),
    ], schema=StructType(df_input_fields))

    df_output = spark.createDataFrame([
        ('1', 15.0, 20.0),
        ('1', 25.0, 20.0),
        ('2', 3.14, 2.93),
        ('2', 2.72, 2.93),
    ], schema=StructType(df_output_fields))

    assert_frames_functionally_equivalent(add_avg_column(df_input), df_output)


def test_transform_add_datetime_columnn(spark):
    df_input_fields = [
        StructField("id", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ]

    df_output_fields = [
        StructField("id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("datetime", TimestampType(), True),

    ]
    df_input = spark.createDataFrame([
        (1, 1700611200000),
        (2, 1700614800000),
        (3, 1700618400000),
        (4, 1700622000000),
    ], schema=StructType(df_input_fields))

    df_output = spark.createDataFrame([
        (1, 1700611200000, datetime.datetime.strptime("2023-11-22 00:00:00", '%Y-%m-%d %H:%M:%S')),
        (2, 1700614800000, datetime.datetime.strptime("2023-11-22 01:00:00", '%Y-%m-%d %H:%M:%S')),
        (3, 1700618400000, datetime.datetime.strptime("2023-11-22 02:00:00", '%Y-%m-%d %H:%M:%S')),
        (4, 1700622000000, datetime.datetime.strptime("2023-11-22 03:00:00", '%Y-%m-%d %H:%M:%S')),
    ], schema=StructType(df_output_fields))

    df_expected = add_datetime_column(df_input)

    assert_frames_functionally_equivalent(df_expected, df_output)


def test_transform_add_city_column(spark):
    df_input_fields = [
        StructField("station_geometry_coordinates_x", DoubleType(), True),
        StructField("station_geometry_coordinates_y", DoubleType(), True),

    ]

    df_output_fields = [

        StructField("station_geometry_coordinates_x", DoubleType(), True),
        StructField("station_geometry_coordinates_y", DoubleType(), True),
        StructField("station_city", StringType(), True)
    ]
    df_input = spark.createDataFrame([
        (5.547464183, 50.624991569),
        (5.547464183, 50.624991569),
        (3.121155599, 50.95317728),
        (3.121155599, 50.95317728),
    ], schema=StructType(df_input_fields))

    df_expected_output = spark.createDataFrame([
        (5.547464183, 50.624991569, 'Liège'),
        (5.547464183, 50.624991569, 'Liège'),
        (3.121155599, 50.95317728, 'Roeselare'),
        (3.121155599, 50.95317728, 'Roeselare'),
    ], schema=StructType(df_output_fields))

    df_actual_output = add_city_column(df_input)
    print("na transform")

    assert_frames_functionally_equivalent(df_actual_output, df_expected_output)


def test_transform_produces_correct_schema(spark):
    df_input_fields = [
        StructField("category_id", StringType(), True),
        StructField("category_label", StringType(), True),
        StructField("feature_id", StringType(), True),
        StructField("feature_label", StringType(), True),
        StructField("offering_id", StringType(), True),
        StructField("offering_label", StringType(), True),
        StructField("phenomenon_id", StringType(), True),
        StructField("phenomenon_label", StringType(), True),
        StructField("procedure_id", StringType(), True),
        StructField("procedure_label", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("service_label", StringType(), True),
        StructField("station_geometry_coordinates_x", DoubleType(), True),
        StructField("station_geometry_coordinates_y", DoubleType(), True),
        StructField("station_geometry_coordinates_z", StringType(), True),
        StructField("station_geometry_type", StringType(), True),
        StructField("station_id", LongType(), True),
        StructField("station_label", StringType(), True),
        StructField("station_type", StringType(), True),
        StructField("timeseries_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", DoubleType(), True),
    ]

    df_output_fields = [
        StructField("category_id", StringType(), True),
        StructField("category_label", StringType(), True),
        StructField("feature_id", StringType(), True),
        StructField("feature_label", StringType(), True),
        StructField("offering_id", StringType(), True),
        StructField("offering_label", StringType(), True),
        StructField("phenomenon_id", StringType(), True),
        StructField("phenomenon_label", StringType(), True),
        StructField("procedure_id", StringType(), True),
        StructField("procedure_label", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("service_label", StringType(), True),
        StructField("station_geometry_coordinates_x", DoubleType(), True),
        StructField("station_geometry_coordinates_y", DoubleType(), True),
        StructField("station_geometry_coordinates_z", StringType(), True),
        StructField("station_geometry_type", StringType(), True),
        StructField("station_id", LongType(), True),
        StructField("station_label", StringType(), True),
        StructField("station_type", StringType(), True),
        StructField("timeseries_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("datetime", TimestampType(), True),
        StructField("avg_day", DoubleType(), True),
        StructField("station_city", StringType(), True)
    ]
    df_input = spark.createDataFrame([
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0),
    ], schema=StructType(df_input_fields))

    df_expected = spark.createDataFrame([
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0, datetime.datetime.strptime("2023-11-22 00:00:00", '%Y-%m-%d %H:%M:%S'),
         15.0, "Liège"),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0, datetime.datetime.strptime("2023-11-22 00:00:00", '%Y-%m-%d %H:%M:%S'),
         15.0, "Liège"),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0, datetime.datetime.strptime("2023-11-22 00:00:00", '%Y-%m-%d %H:%M:%S'),
         15.0, "Liège"),
        ('5', 'Particulate Matter < 10 µm', '1170', '43H201 - Liège', '6880', '6880 -  - procedure', '25', 'Particulate Matter < 10 µm',
         '6880', '6880 -  - procedure', '1', 'IRCEL - CELINE: timeseries-api (SOS 2.0)', 5.547464183, 50.624991569, 50.5, "Point", 1170,
         "43H201 - Liège", "Feature", "6880", 1700611200000, 15.0, datetime.datetime.strptime("2023-11-22 00:00:00", '%Y-%m-%d %H:%M:%S'),
         15.0, "Liège"),
    ], schema=StructType(df_output_fields))

    df_actual = transform(df_input)
    assert_frames_functionally_equivalent(df_actual, df_expected)


def assert_frames_functionally_equivalent(
        df1: DataFrame, df2: DataFrame, check_nullability=True
):
    """
    Validate if two non-nested dataframes have identical schemas, and data,
    ignoring the ordering of both.
    """
    # This is what we call an “early-out”: here it is computationally cheaper
    # to validate that two things are not equal, rather than finding out that
    # they are equal.
    try:
        if check_nullability:
            assert set(df1.schema.fields) == set(df2.schema.fields)
        else:
            assert set(df1.dtypes) == set(df2.dtypes)
    except AssertionError:
        logging.warning(df1.schema)
        logging.warning(df2.schema)
        raise

    df1.show()
    df2.show()
    sorted_rows = df2.select(df1.columns).orderBy(df1.columns).collect()
    assert df1.orderBy(*df1.columns).collect() == sorted_rows, "data not equal"

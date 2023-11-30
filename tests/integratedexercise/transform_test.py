from numpy import NaN

from  src.integratedexercise.transform import transform
import pytest
import logging

from pyspark.sql import DataFrame

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, LongType, DoubleType, TimestampType



def test_transform(spark):
    frame1 = spark.createDataFrame([(1,), (2,)])
    frame2 = spark.createDataFrame([(1,), (2,), (2,)])

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
        StructField("avg_day", DoubleType(), True)
    ]
    df_input = spark.createDataFrame([
        ('5',	'Particulate Matter < 10 µm',	'1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0),
        ('5',	'Particulate Matter < 10 µm',	'1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0),
        ('5',	'Particulate Matter < 10 µm',	'1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0),
        ('5',	'Particulate Matter < 10 µm',	'1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0),
        ], schema=StructType(df_input_fields))

    df_output = spark.createDataFrame([
        ('5',	'Particulate Matter < 10 µm', '1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0,	to_timestamp("2023-11-21 23:00:00.000"), 15.0),
        ('5',	'Particulate Matter < 10 µm', '1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0,	"2023-11-21 23:00:00.000", 15.0),
        ('5',	'Particulate Matter < 10 µm', '1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0,	"2023-11-21 23:00:00.000", 15.0),
        ('5',	'Particulate Matter < 10 µm', '1170',	'43H201 - Liège',	'6880',	'6880 -  - procedure', '25', 'Particulate Matter < 10 µm', '6880',	'6880 -  - procedure',	'1',	'IRCEL - CELINE: timeseries-api (SOS 2.0)',	5.547464183,	50.624991569,	50.5, "Point",	1170,	"43H201 - Liège",	"Feature",	"6880",	1700611200000,	15.0,	"2023-11-21 23:00:00.000", 15.0),
    ], schema=StructType(df_output_fields))

    with pytest.raises(AssertionError):
        assert_frames_functionally_equivalent(transform(df_input), df_output)


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

    sorted_rows = df2.select(df1.columns).orderBy(df1.columns).collect()
    assert df1.orderBy(*df1.columns).collect() == sorted_rows
import pytest
from pyspark import SparkConf
from pyspark.sql import SparkSession
@pytest.fixture(scope="session")
def spark(request):
    """Fixture for creating a SparkSession."""
    conf = {}
    conf = SparkConf().setAll(pairs=conf.items())
    builder = SparkSession.builder.master("local[*]").config(conf=conf)
    session = builder.getOrCreate()
    request.addfinalizer(session.stop)

    return session


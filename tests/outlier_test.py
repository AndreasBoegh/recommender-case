import pandas as pd
from utils import outlier_thresholds
from pyspark.sql import SparkSession


def test_outlier_thresholds():
    # Create a sample DataFrame
    spark = SparkSession.builder.getOrCreate()
    data = {"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
    df = spark.createDataFrame(pd.DataFrame(data))

    # Call the function
    low_limit, up_limit = outlier_thresholds(df, "col1", probabilities=[0.1, 0.9])

    # Check the expected values
    assert low_limit == -11
    assert up_limit == 21

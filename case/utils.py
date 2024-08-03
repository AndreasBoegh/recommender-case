from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def outlier_thresholds(df: DataFrame, variable):
    quantiles = df.approxQuantile(variable, [0.01, 0.99], 0.0)
    quartile1, quartile3 = quantiles
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit


# Function to replace outliers
def replace_with_thresholds(df: DataFrame, variable):
    low_limit, up_limit = outlier_thresholds(df, variable)
    df = df.withColumn(
        variable, F.expr(f"IF({variable} < {low_limit}, {low_limit}, {variable})")
    )
    df = df.withColumn(
        variable, F.expr(f"IF({variable} > {up_limit}, {up_limit}, {variable})")
    )
    return df

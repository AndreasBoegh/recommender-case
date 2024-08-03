from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Python Spark preprocessing step").getOrCreate()
from .utils import replace_with_thresholds


def main():
    # Read the data (assume this data would live on the data platform and not in CSV)
    df = spark.read.csv(
        "./data/Year 2010-2011.csv", header=True, inferSchema=True
    ).withColumn("InvoiceDate", F.to_date(F.col("InvoiceDate"), "M/d/yyyy H:mm"))

    # Clean up by removing rows with missing values and negative quantities
    df = (
        df.dropna()
        .filter(F.col("Invoice").contains("C") == False)
        .filter(F.col("Quantity") > 0)
    )

    # Remove outliers from quantity and price
    df = replace_with_thresholds(df, "Quantity")
    df = replace_with_thresholds(df, "Price")

    df = df.withColumn("TotalPrice", F.col("Quantity") * F.col("Price"))

    # Get the max invoice date and add 2 days to it
    max_invoice_date = df.select(
        F.max("InvoiceDate") + F.expr("INTERVAL 2 DAYS")
    ).collect()[0][0]

    # Calculate RFM values
    df = (
        df.groupBy("Customer ID")
        .agg(
            (F.datediff(F.max("InvoiceDate"), F.min("InvoiceDate"))).alias("Recency"),
            (F.datediff(F.lit(max_invoice_date), F.min(F.col("InvoiceDate")))).alias(
                "T"
            ),
            F.countDistinct("Invoice").alias("Frequency"),
            (F.sum("TotalPrice") / F.countDistinct("Invoice")).alias("Monetary"),
        )
        .filter(F.col("Frequency") > 1)
        .withColumn("Frequency", F.col("Frequency").cast("int"))
    )

    # Save the result for caching
    df.write.mode("overwrite").parquet("./data/preprocessed_data")

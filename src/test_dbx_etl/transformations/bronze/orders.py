from pyspark import pipelines as dp
from pyspark.sql.functions import to_date


@dp.table(name="orders_bronze")
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volume/orders/")
        .withColumnRenamed("product_id", "item_id")
        .withColumn("order_date", to_date("order_date"))
    )

from pyspark import pipelines as dp


@dp.table(
    nama="customers_bronze",
    description="Bronze table for customers data",
    tags=["bronze", "customers"],
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volumne/customers.csv")
    )

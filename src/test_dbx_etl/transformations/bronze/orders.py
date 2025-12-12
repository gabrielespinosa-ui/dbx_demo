from pyspark import pipelines as dp


@dp.table(
    nama="orders_bronze",
    description="Bronze table for orders data",
    tags=["bronze", "orders"],
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volumne/orders.csv")
    )

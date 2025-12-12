from pyspark import pipelines as dp


@dp.table(
    nama="items_bronze",
    description="Bronze table for items data",
    tags=["bronze", "items"],
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volumne/items.csv")
    )

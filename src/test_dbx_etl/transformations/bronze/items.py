from pyspark import pipelines as dp


@dp.table(name="items_bronze")
def items_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volume/items/")
        .withColumnRenamed("product_id", "item_id")
    )

from pyspark import pipelines as dp
from pyspark.sql.functions import to_date


@dp.table(name="customers_bronze")
def customers_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/dbx_test/default/test_volume/customers/")
        .withColumn("updated_at", to_date("updated_at"))
    )

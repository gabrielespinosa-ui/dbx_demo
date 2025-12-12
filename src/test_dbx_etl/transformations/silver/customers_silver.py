from pyspark import pipelines as dp
from pyspark.sql.functions import expr, col

dp.create_streaming_table("customers_silver")
dp.create_auto_cdc_flow(
    source="customers_bronze",
    target="customers_silver",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("op='d'"),
    stored_as_scd_type="1",
    except_column_list=["op"],
)

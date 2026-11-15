# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "440b0c40-f81b-482c-8816-c3842d00eddf",
# META       "default_lakehouse_name": "ContosoCleanLakehouse",
# META       "default_lakehouse_workspace_id": "5b6d80b6-785f-42fe-bec3-2fd4653e79a2",
# META       "known_lakehouses": [
# META         {
# META           "id": "440b0c40-f81b-482c-8816-c3842d00eddf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from functools import reduce

TABLE_NAMES = [
    "customer",
    "store",
    "product",
    "date",
    "currencyexchange",
    "sales",
]

tables = {
    table_name: spark.table(f"dbo.{table_name}")
    for table_name in TABLE_NAMES
}

customer = tables["customer"]
store = tables["store"]
product = tables["product"]
date_table = tables["date"]
currencyexchange = tables["currencyexchange"]
sales = tables["sales"]

customer_row_count = customer.count()
customer_distinct_key_count = (
    customer
    .select("CustomerKey")
    .distinct()
    .count()
)
customer_duplicate_key_count = (
    customer_row_count
    - customer_distinct_key_count
)

store_row_count = store.count()
store_distinct_key_count = (
    store
    .select("StoreKey")
    .distinct()
    .count()
)
store_duplicate_key_count = (
    store_row_count
    - store_distinct_key_count
)

product_row_count = product.count()
product_distinct_key_count = (
    product
    .select("ProductKey")
    .distinct()
    .count()
)
product_duplicate_key_count = (
    product_row_count
    - product_distinct_key_count
)

date_row_count = date_table.count()
date_distinct_date_count = (
    date_table
    .select(F.to_date("Date").alias("Date"))
    .distinct()
    .count()
)
date_duplicate_date_count = (
    date_row_count
    - date_distinct_date_count
)

currencyexchange_row_count = currencyexchange.count()
sales_row_count = sales.count()

sales_distinct_order_count = (
    sales
    .select("OrderKey")
    .distinct()
    .count()
)

sales_distinct_customer_count = (
    sales
    .select("CustomerKey")
    .distinct()
    .count()
)

sales_distinct_store_count = (
    sales
    .select("StoreKey")
    .distinct()
    .count()
)

sales_distinct_product_count = (
    sales
    .select("ProductKey")
    .distinct()
    .count()
)

missing_customer_relationship_rows = (
    sales.alias("sales")
    .join(
        customer.select("CustomerKey").alias("customer"),
        F.col("sales.CustomerKey")
        == F.col("customer.CustomerKey"),
        "left_anti",
    )
    .filter(F.col("sales.CustomerKey").isNotNull())
    .count()
)

missing_store_relationship_rows = (
    sales.alias("sales")
    .join(
        store.select("StoreKey").alias("store"),
        F.col("sales.StoreKey")
        == F.col("store.StoreKey"),
        "left_anti",
    )
    .filter(F.col("sales.StoreKey").isNotNull())
    .count()
)

missing_product_relationship_rows = (
    sales.alias("sales")
    .join(
        product.select("ProductKey").alias("product"),
        F.col("sales.ProductKey")
        == F.col("product.ProductKey"),
        "left_anti",
    )
    .filter(F.col("sales.ProductKey").isNotNull())
    .count()
)

valid_dates = (
    date_table
    .select(F.to_date("Date").alias("ValidDate"))
    .distinct()
)

missing_order_date_relationship_rows = (
    sales
    .select(
        F.to_date("OrderDate").alias("OrderDate")
    )
    .filter(F.col("OrderDate").isNotNull())
    .join(
        valid_dates,
        F.col("OrderDate") == F.col("ValidDate"),
        "left_anti",
    )
    .count()
)

missing_delivery_date_relationship_rows = (
    sales
    .select(
        F.to_date("DeliveryDate").alias("DeliveryDate")
    )
    .filter(F.col("DeliveryDate").isNotNull())
    .join(
        valid_dates,
        F.col("DeliveryDate") == F.col("ValidDate"),
        "left_anti",
    )
    .count()
)

missing_coordinate_rows = (
    customer
    .filter(
        F.col("Latitude").isNull()
        | F.col("Longitude").isNull()
    )
    .count()
)

invalid_latitude_rows = (
    customer
    .filter(
        F.col("Latitude").isNotNull()
        & (
            (F.col("Latitude") < -90)
            | (F.col("Latitude") > 90)
        )
    )
    .count()
)

invalid_longitude_rows = (
    customer
    .filter(
        F.col("Longitude").isNotNull()
        & (
            (F.col("Longitude") < -180)
            | (F.col("Longitude") > 180)
        )
    )
    .count()
)

zero_coordinate_rows = (
    customer
    .filter(
        (F.col("Latitude") == 0)
        & (F.col("Longitude") == 0)
    )
    .count()
)

address_columns = [
    "StreetAddress",
    "City",
    "State",
    "ZipCode",
    "Country",
]

conflicting_address_count = (
    customer
    .select(
        *address_columns,
        "Latitude",
        "Longitude",
    )
    .distinct()
    .groupBy(*address_columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_order_date_rows = (
    sales
    .filter(F.col("OrderDate").isNull())
    .count()
)

invalid_delivery_date_rows = (
    sales
    .filter(F.col("DeliveryDate").isNull())
    .count()
)

delivery_before_order_rows = (
    sales
    .filter(
        F.col("OrderDate").isNotNull()
        & F.col("DeliveryDate").isNotNull()
        & (
            F.col("DeliveryDate")
            < F.col("OrderDate")
        )
    )
    .count()
)

nonpositive_quantity_rows = (
    sales
    .filter(
        F.col("Quantity").isNull()
        | (F.col("Quantity") <= 0)
    )
    .count()
)

negative_unit_price_rows = (
    sales
    .filter(F.col("UnitPrice") < 0)
    .count()
)

negative_net_price_rows = (
    sales
    .filter(F.col("NetPrice") < 0)
    .count()
)

negative_unit_cost_rows = (
    sales
    .filter(F.col("UnitCost") < 0)
    .count()
)

invalid_exchange_rate_rows = (
    sales
    .filter(
        F.col("ExchangeRate").isNull()
        | (F.col("ExchangeRate") <= 0)
    )
    .count()
)

sales_totals = (
    sales
    .agg(
        F.min("OrderDate").alias(
            "Minimum_Order_Date"
        ),
        F.max("OrderDate").alias(
            "Maximum_Order_Date"
        ),
        F.min("DeliveryDate").alias(
            "Minimum_Delivery_Date"
        ),
        F.max("DeliveryDate").alias(
            "Maximum_Delivery_Date"
        ),
        F.sum(
            F.col("Quantity").cast("double")
        ).alias("Total_Quantity"),
        F.sum(
            F.col("Quantity").cast("double")
            * F.col("NetPrice").cast("double")
            * F.col("ExchangeRate").cast("double")
        ).alias("Total_Net_Sales"),
        F.sum(
            F.col("Quantity").cast("double")
            * F.col("UnitCost").cast("double")
            * F.col("ExchangeRate").cast("double")
        ).alias("Total_Cost"),
    )
    .first()
)

total_quantity = sales_totals["Total_Quantity"]
total_net_sales = sales_totals["Total_Net_Sales"]
total_cost = sales_totals["Total_Cost"]
gross_margin = total_net_sales - total_cost

blocking_metrics = {
    "Customer_Duplicate_Key_Count":
        customer_duplicate_key_count,
    "Store_Duplicate_Key_Count":
        store_duplicate_key_count,
    "Product_Duplicate_Key_Count":
        product_duplicate_key_count,
    "Date_Duplicate_Date_Count":
        date_duplicate_date_count,
    "Missing_Customer_Relationship_Rows":
        missing_customer_relationship_rows,
    "Missing_Store_Relationship_Rows":
        missing_store_relationship_rows,
    "Missing_Product_Relationship_Rows":
        missing_product_relationship_rows,
    "Missing_OrderDate_Relationship_Rows":
        missing_order_date_relationship_rows,
    "Missing_DeliveryDate_Relationship_Rows":
        missing_delivery_date_relationship_rows,
    "Missing_Coordinate_Rows":
        missing_coordinate_rows,
    "Invalid_Latitude_Rows":
        invalid_latitude_rows,
    "Invalid_Longitude_Rows":
        invalid_longitude_rows,
    "Zero_Coordinate_Rows":
        zero_coordinate_rows,
    "Conflicting_Address_Count":
        conflicting_address_count,
    "Invalid_OrderDate_Rows":
        invalid_order_date_rows,
    "Invalid_DeliveryDate_Rows":
        invalid_delivery_date_rows,
    "Delivery_Before_Order_Rows":
        delivery_before_order_rows,
    "Nonpositive_Quantity_Rows":
        nonpositive_quantity_rows,
    "Negative_UnitPrice_Rows":
        negative_unit_price_rows,
    "Negative_NetPrice_Rows":
        negative_net_price_rows,
    "Negative_UnitCost_Rows":
        negative_unit_cost_rows,
    "Invalid_ExchangeRate_Rows":
        invalid_exchange_rate_rows,
}

blocking_issue_count = sum(
    blocking_metrics.values()
)

validation_status = (
    "PASS"
    if blocking_issue_count == 0
    else "FAIL"
)

metrics = {
    "Customer_Row_Count":
        customer_row_count,
    "Customer_Distinct_Key_Count":
        customer_distinct_key_count,
    "Customer_Duplicate_Key_Count":
        customer_duplicate_key_count,
    "Store_Row_Count":
        store_row_count,
    "Store_Distinct_Key_Count":
        store_distinct_key_count,
    "Store_Duplicate_Key_Count":
        store_duplicate_key_count,
    "Product_Row_Count":
        product_row_count,
    "Product_Distinct_Key_Count":
        product_distinct_key_count,
    "Product_Duplicate_Key_Count":
        product_duplicate_key_count,
    "Date_Row_Count":
        date_row_count,
    "Date_Distinct_Date_Count":
        date_distinct_date_count,
    "Date_Duplicate_Date_Count":
        date_duplicate_date_count,
    "CurrencyExchange_Row_Count":
        currencyexchange_row_count,
    "Sales_Row_Count":
        sales_row_count,
    "Sales_Distinct_Order_Count":
        sales_distinct_order_count,
    "Sales_Distinct_Customer_Count":
        sales_distinct_customer_count,
    "Sales_Distinct_Store_Count":
        sales_distinct_store_count,
    "Sales_Distinct_Product_Count":
        sales_distinct_product_count,
    **blocking_metrics,
    "Total_Quantity":
        total_quantity,
    "Total_Net_Sales":
        total_net_sales,
    "Total_Cost":
        total_cost,
    "Gross_Margin":
        gross_margin,
    "Blocking_Issue_Count":
        blocking_issue_count,
}

validation_rows = [
    (metric_name, float(metric_value))
    for metric_name, metric_value
    in metrics.items()
]

validation_result = spark.createDataFrame(
    validation_rows,
    ["Metric", "Value"],
)

display(
    validation_result.orderBy("Metric")
)

print("=" * 70)
print("FABRIC PYTHON CONTROL TOTALS")
print("=" * 70)
print(f"Sales rows: {sales_row_count:,}")
print(
    "Distinct orders: "
    f"{sales_distinct_order_count:,}"
)
print(f"Total quantity: {total_quantity:,.2f}")
print(f"Total net sales: {total_net_sales:,.4f}")
print(f"Total cost: {total_cost:,.4f}")
print(f"Gross margin: {gross_margin:,.4f}")
print(
    "Minimum order date: "
    f"{sales_totals['Minimum_Order_Date']}"
)
print(
    "Maximum order date: "
    f"{sales_totals['Maximum_Order_Date']}"
)
print(
    "Minimum delivery date: "
    f"{sales_totals['Minimum_Delivery_Date']}"
)
print(
    "Maximum delivery date: "
    f"{sales_totals['Maximum_Delivery_Date']}"
)
print(
    f"Blocking issue count: "
    f"{blocking_issue_count:,}"
)
print(
    f"FABRIC PYTHON VALIDATION: "
    f"{validation_status}"
)

if validation_status != "PASS":
    raise ValueError(
        "Fabric Python validation failed."
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


EXPECTED_ROWS = {
    "currencyexchange": 100450,
    "customer": 104990,
    "date": 4018,
    "product": 2517,
    "sales": 2349091,
    "store": 74,
}

KEY_COLUMNS = {
    "currencyexchange": [],
    "customer": ["CustomerKey"],
    "date": ["DateKey"],
    "product": ["ProductKey"],
    "sales": ["OrderKey", "LineNumber"],
    "store": ["StoreKey"],
}


def validate_table(
    table_name: str,
    expected_rows: int,
    key_columns: list[str],
) -> dict:
    dataframe = spark.table(table_name)

    actual_rows = dataframe.count()

    duplicate_keys = 0
    null_keys = 0

    if key_columns:
        distinct_keys = (
            dataframe
            .select(*key_columns)
            .distinct()
            .count()
        )

        duplicate_keys = actual_rows - distinct_keys

        null_condition = reduce(
            lambda left, right: left | right,
            [
                F.col(column_name).isNull()
                for column_name in key_columns
            ],
        )

        null_keys = (
            dataframe
            .filter(null_condition)
            .count()
        )

    status = (
        "PASS"
        if actual_rows == expected_rows
        and duplicate_keys == 0
        and null_keys == 0
        else "FAIL"
    )

    return {
        "TableName": table_name,
        "ActualRows": actual_rows,
        "ExpectedRows": expected_rows,
        "DuplicateKeys": duplicate_keys,
        "NullKeys": null_keys,
        "ValidationStatus": status,
    }


validation_results = [
    validate_table(
        table_name=table_name,
        expected_rows=expected_rows,
        key_columns=KEY_COLUMNS[table_name],
    )
    for table_name, expected_rows
    in EXPECTED_ROWS.items()
]

validation_dataframe = (
    spark.createDataFrame(validation_results)
    .select(
        "TableName",
        "ActualRows",
        "ExpectedRows",
        "DuplicateKeys",
        "NullKeys",
        "ValidationStatus",
    )
    .orderBy("TableName")
)

failed_tables = [
    result
    for result in validation_results
    if result["ValidationStatus"] != "PASS"
]

print("CONTOSO FABRIC PYTHON VALIDATION")
print("=" * 40)
print(f"Tables tested: {len(validation_results)}")
print(
    "Total rows: "
    f"{sum(result['ActualRows'] for result in validation_results):,}"
)
print(f"Tables requiring review: {len(failed_tables)}")
print(
    "FINAL STATUS: "
    + ("PASS" if not failed_tables else "REVIEW REQUIRED")
)

display(validation_dataframe)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Day 29 - End-to-End E-Commerce ETL Project

## 1. Project Overview

This project implements an end-to-end e-commerce data engineering pipeline using Apache Spark and Scala.

The pipeline follows:

Raw Data -> Clean -> Enrich -> Aggregate -> Partitioned Output

It demonstrates both low-level Spark RDD processing and higher-level Spark SQL/DataFrame processing in a single production-style workflow.

The project is designed to demonstrate:

- RDD and Pair RDD processing
- DataFrame and Dataset processing
- Data quality validation
- Duplicate removal
- Broadcast joins
- Window functions
- Aggregations
- Persistence
- Partitioned Parquet output
- Shuffle and stage analysis
- Spark physical execution plans
- Catalyst optimization
- Avoiding unnecessary UDFs
- Local Spark execution with Java 17

---

## 2. Architecture

```text
                    +-----------------------+
                    |    Raw Orders CSV     |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |    RDD Ingestion      |
                    | textFile + parsing    |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |     RDD Cleaning      |
                    | validation + parsing  |
                    | duplicate removal     |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |      Pair RDD         |
                    | product revenue       |
                    |      reduceByKey      |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |    Clean Dataset      |
                    |     DataFrame         |
                    +-----------+-----------+
                                |
              +----------------+----------------+
              |                                 |
              v                                 v
     +------------------+             +------------------+
     | Customer CSV     |             | Product CSV      |
     | Reference Data   |             | Reference Data   |
     +--------+---------+             +--------+---------+
              |                                 |
              +----------------+----------------+
                               |
                               v
                    +-----------------------+
                    |    Broadcast Joins    |
                    | Customer + Product    |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |      Persistence      |
                    |    MEMORY_AND_DISK    |
                    +-----------+-----------+
                                |
                                v
                    +-----------------------+
                    |    Window Functions   |
                    | order number           |
                    | cumulative revenue    |
                    +-----------+-----------+
                                |
                 +--------------+--------------+
                 |              |              |
                 v              v              v
        +-------------+ +-------------+ +-------------+
        |  Category   | |  Customer   | |    Daily    |
        | Aggregation | |   Ranking   | |    Sales    |
        +------+------+ +------+------+ +------+------+
               |               |               |
               +---------------+---------------+
                               |
                               v
                    +-----------------------+
                    |  Partitioned Parquet  |
                    |      Output Layer     |
                    +-----------------------+
## 3. Technology Stack
Technology	Version / Usage
Scala	2.12.18
Apache Spark	3.5.1
Spark Core	RDD / Pair RDD
Spark SQL	DataFrame / Dataset
SBT	Build tool
Java	17.0.20
Storage Format	Parquet
Compression	Snappy
Execution	local[*]
## 4. Project Structure
Day29-End-to-End-E-Commerce-Project/
|
+-- data/
|   +-- raw/
|   |   +-- orders.csv
|   |
|   +-- reference/
|       +-- customers.csv
|       +-- products.csv
|
+-- src/
|   +-- main/
|       +-- scala/
|           +-- com/
|               +-- day29ecommerce/
|                   +-- EcommerceETLApp.scala
|
+-- output/
|   +-- category_summary/
|   +-- customer_ranking/
|   +-- daily_sales/
|   +-- enriched_orders/
|
+-- build.sbt
+-- .gitignore
+-- README.md

Generated build and runtime artifacts such as target/, project/, and output/ are excluded from Git using .gitignore.

## 5. Input Data
Orders

The raw orders dataset contains:

order ID
customer ID
product ID
order date
quantity
unit price
order status

The dataset intentionally contains data-quality problems to demonstrate real-world cleaning.

Intentional Data Quality Issues
Duplicate

ORD1015 appears twice.

The pipeline removes duplicate order IDs using a Pair RDD.

Invalid Quantity

ORD1017 contains:

quantity = -1

The record is rejected because quantity must be greater than zero.

Cancelled Order

ORD1004 has:

status = CANCELLED

The record remains part of the cleaned dataset but is excluded from completed-order analytics.

## 6. Pipeline Stages
Stage 1 - Raw Ingestion

The raw CSV is loaded using Spark Core:

spark.sparkContext.textFile(rawPath)

The header is removed before processing the records.

This demonstrates direct RDD-based ingestion.

Stage 2 - RDD Cleaning

Each raw record is parsed using:

flatMap

Validation checks include:

correct number of fields
non-empty identifiers
valid integer quantity
valid numeric price
positive quantity
non-empty status

Invalid records are discarded.

The cleaned records are converted into:

CleanOrder

case class objects.

Stage 3 - Duplicate Removal

A Pair RDD is created:

.map(order => (order.orderId, order))

Duplicates are removed using:

.reduceByKey((first, _) => first)

reduceByKey was intentionally selected instead of groupByKey.

Why reduceByKey?

reduceByKey performs map-side aggregation before sending data across the network.

This reduces shuffle volume compared with collecting all values for a key using groupByKey.

## 7. Pair RDD Revenue Calculation

Completed orders are converted into:

(product_id, revenue)

Then:

.reduceByKey(_ + _)

calculates product-level revenue.

This demonstrates a classic Pair RDD aggregation pattern.

Example:

PROD001 -> $3600.00
PROD002 -> $4000.00
PROD003 -> $1350.00
PROD004 -> $3600.00
PROD005 -> $6600.00
PROD006 -> $300.00
## 8. Dataset and DataFrame Processing

The cleaned RDD is converted into a typed Dataset:

cleanedOrders.toDS()

Then Spark SQL functions are used to derive:

order_date
revenue

Revenue is calculated using Spark's built-in expression:

quantity * unitPrice

No custom UDF is required.

## 9. Why No UDF?

This project intentionally avoids UDFs.

All required transformations can be implemented using Spark SQL functions such as:

to_date
sum
avg
countDistinct
round
row_number
dense_rank

Built-in Spark expressions are preferred because Catalyst can understand and optimize them.

A UDF would become appropriate only when business logic cannot reasonably be expressed using Spark's built-in functions.

## 10. Reference Data

Two small reference datasets are loaded as DataFrames:

Customers
customer_id
customer_name
city
segment
Products
product_id
product_name
category
brand
## 11. Broadcast Joins

The order dataset is enriched using two joins:

Orders -> Customers
Orders -> Products

Both reference datasets are small, so broadcast joins are used:

broadcast(customersDF)
broadcast(productsDF)

This allows Spark to distribute the small lookup tables to executors instead of performing a large shuffle join.

The physical plan confirms:

BroadcastHashJoin

for both joins.

Optimization Choice

Broadcast joins are appropriate because:

customer table is small
product table is small
fact/order data is comparatively larger

For large reference tables, broadcasting could cause executor memory pressure and should not be used blindly.

## 12. Persistence

The enriched dataset is reused by multiple downstream operations:

window calculations
category aggregation
customer aggregation
daily aggregation
output writes

Therefore it is persisted using:

.persist(StorageLevel.MEMORY_AND_DISK)

This prevents Spark from repeatedly recomputing the enrichment pipeline.

Why MEMORY_AND_DISK?

MEMORY_AND_DISK provides a safer persistence strategy when the dataset may not completely fit in memory.

Spark keeps cached partitions in memory where possible and spills remaining partitions to disk.

## 13. Window Functions

Two types of window processing are demonstrated.

Customer Order Number

The customer-specific window:

PARTITION BY customerId
ORDER BY order_date, orderId

is used with:

row_number()

This generates the sequential order number for each customer.

Example:

CUST001
ORD1001 -> 1
ORD1003 -> 2
ORD1009 -> 3
ORD1015 -> 4
Cumulative Revenue

The same customer window is used with:

sum("revenue")

over:

unboundedPreceding -> currentRow

This produces cumulative customer revenue.

## 14. Global Customer Revenue Ranking

Customer totals are calculated first.

Then:

dense_rank()

ranks customers by total revenue.

The ranking window intentionally has no partition because the requirement is a global ranking across all customers.

Spark therefore reports:

WindowExec: No Partition Defined for Window operation

This is a performance warning rather than an error.

For a global ranking, all rows must participate in the same ordered ranking operation.

The resulting ranking is:

1 -> CUST001 -> $4900
2 -> CUST004 -> $4000
3 -> CUST002 -> $3300
4 -> CUST003 -> $2800
4 -> CUST005 -> $2800
5 -> CUST006 -> $1650

The tie between CUST003 and CUST005 demonstrates the behavior of dense_rank().

## 15. Category Aggregation

The category summary calculates:

distinct orders
units sold
total revenue
average order revenue

Results:

Category	Orders	Units	Revenue
Electronics	8	11	$14,200.00
Accessories	8	18	$5,250.00

Total completed revenue:

$19,450.00
## 16. Daily Sales

Daily aggregation produces:

Date	Orders	Units	Revenue
2026-10-01	2	3	$3,200
2026-10-02	1	3	$450
2026-10-03	2	3	$3,100
2026-10-04	2	6	$2,200
2026-10-05	2	2	$2,650
2026-10-06	2	3	$1,500
2026-10-07	3	6	$5,150
2026-10-08	2	3	$1,200
## 17. Partitioned Output

The enriched order dataset is written as Parquet partitioned by:

order_date

Output structure:

output/enriched_orders/
|
+-- order_date=2026-10-01/
+-- order_date=2026-10-02/
+-- order_date=2026-10-03/
+-- order_date=2026-10-04/
+-- order_date=2026-10-05/
+-- order_date=2026-10-06/
+-- order_date=2026-10-07/
+-- order_date=2026-10-08/
Why partition by date?

Most analytical workloads commonly filter by date.

Partitioning allows Spark to perform partition pruning so irrelevant dates can be skipped.

Example:

WHERE order_date = '2026-10-07'

can avoid reading unrelated date partitions.

## 18. Output Datasets

The pipeline generates four logical outputs.

enriched_orders

Fully enriched completed orders containing customer and product attributes.

Format:

Parquet

Partitioned by:

order_date
category_summary

Category-level sales metrics.

customer_ranking

Customer revenue ranking.

daily_sales

Daily sales metrics.

All outputs use Parquet with Spark's default Snappy compression.

## 19. Shuffle Analysis

A shuffle occurs when Spark must redistribute data between partitions.

This project intentionally demonstrates several shuffle-producing operations.

reduceByKey

Used for:

duplicate removal
product revenue aggregation

Data is redistributed by key.

However, reduceByKey performs map-side combining, reducing network traffic.

groupBy / Aggregation

Operations such as:

groupBy("category")

require records with the same grouping key to reach the same partition.

The physical plan shows:

Exchange hashpartitioning(...)

This is a shuffle boundary.

countDistinct

The category aggregation contains:

countDistinct("orderId")

Distinct aggregation requires additional aggregation stages and contributes to shuffle work.

The physical plan demonstrates partial and final aggregation.

Window Functions

The customer window:

PARTITION BY customerId
ORDER BY order_date

requires data to be partitioned and sorted by the window specification.

This can introduce:

Exchange
Sort
Window

operations.

Global Ranking

The global customer ranking has no partition specification.

Spark must order all customer records globally.

This produces a global exchange and sort.

Partitioned Write

Writing:

.partitionBy("order_date")

can require repartitioning data by the output partition column.

This is necessary to physically organize the data into date partitions.

## 20. Stage Analysis

Spark divides execution into stages separated by shuffle boundaries.

A simplified execution flow is:

Stage
 |
 +-- Read raw data
 |
 +-- Parse and validate
 |
 +-- Map transformations
 |
 +-- Shuffle
 |
 +-- Reduce / aggregate
 |
 +-- Shuffle
 |
 +-- Window / sort
 |
 +-- Aggregate
 |
 +-- Write output
Narrow Transformations

Examples:

map
filter
flatMap
withColumn
select

These generally do not require data redistribution.

Wide Transformations

Examples:

reduceByKey
groupBy
countDistinct
orderBy
window operations
partitionBy

These can introduce shuffle boundaries.

## 21. Physical Plan Analysis

The application calls:

categorySummary.explain(true)

This displays:

Parsed Logical Plan
Analyzed Logical Plan
Optimized Logical Plan
Physical Plan

The optimized plan demonstrates Spark's Catalyst optimizer.

The physical plan confirms:

BroadcastHashJoin
HashAggregate
Exchange
Sort
InMemoryTableScan

This provides evidence that the intended optimization strategies were actually applied.

## 22. Catalyst Optimization

Spark Catalyst optimizes the SQL/DataFrame portion of the pipeline.

Examples visible in the physical plan include:

Predicate Pushdown

The completed-order filter:

status = COMPLETED

is pushed toward the source transformation.

Column Pruning

Only required columns are carried into downstream aggregation.

Broadcast Join Selection

The explicit broadcast hints result in:

BroadcastHashJoin
Reuse of Persisted Data

The physical plan contains:

InMemoryRelation

and:

InMemoryTableScan

showing that the persisted enriched dataset is being reused.

## 23. Spark Configuration

The project uses:

.config("spark.sql.shuffle.partitions", "4")

The default Spark shuffle partition count is much larger than necessary for this small local dataset.

Four partitions are intentionally used to keep the learning workload lightweight.

Production Consideration

The value should be tuned according to:

input data size
cluster size
executor cores
shuffle volume
workload characteristics

It should not automatically be fixed at 4 in a production cluster.

## 24. Optimization Decisions
Decision	Reason
reduceByKey	Map-side combine reduces shuffle volume
Broadcast joins	Small reference tables
Built-in SQL functions	Catalyst optimization
No UDF	No custom logic required
MEMORY_AND_DISK	Dataset is reused and safer than memory-only caching
Date partitioning	Efficient date filtering and partition pruning
Parquet	Columnar storage
Snappy	Good compression/performance balance
4 shuffle partitions	Appropriate for local learning dataset
Column selection	Reduces unnecessary data processing
## 25. Data Quality Results

Input:

19 raw records

After cleaning and duplicate removal:

17 unique valid records

Completed records used for analytics:

16

Removed from completed analytics:

1 cancelled order

Invalid/duplicate records removed during cleaning:

2

The two removed records are:

Duplicate ORD1015
Invalid ORD1017
## 26. Business Results
Total Revenue
$19,450.00
Top Customer
CUST001
Revenue: $4,900.00
Top Category
Electronics
Revenue: $14,200.00
Highest Revenue Product
PROD005
Revenue: $6,600.00
Highest Revenue Day
2026-10-07
Revenue: $5,150.00
## 27. Running the Project
Compile
JAVA_TOOL_OPTIONS=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED sbt clean

Then:

JAVA_TOOL_OPTIONS=--add-exports=java.base/sun.nio.ch=ALL-UNNAMED sbt compile
Run

Because Spark 3.5.1 is being executed with Java 17 in this WSL environment, the following Java module options are required:

JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" sbt run
## 28. Spark UI

During execution Spark starts the local Spark UI on:

http://localhost:4040

The Spark UI can be used to inspect:

Jobs
Stages
Tasks
Storage
Environment
SQL execution plans
Shuffle read/write
Cached data
## 29. Verification

After a successful run:

find output -maxdepth 3 -type d | sort

Check generated files:

find output -maxdepth 3 -type f | sort

Check output size:

du -sh output

Expected output datasets:

output/category_summary
output/customer_ranking
output/daily_sales
output/enriched_orders
## 30. Key Spark Concepts Demonstrated

This project demonstrates the following Spark concepts in one end-to-end application:

SparkContext
SparkSession
RDD
Pair RDD
Dataset
DataFrame
map
flatMap
filter
reduceByKey
joins
broadcast joins
persistence
MEMORY_AND_DISK
aggregations
countDistinct
window functions
row_number
dense_rank
cumulative aggregation
shuffle
stages
partitioning
partition pruning
Parquet
Snappy compression
Catalyst optimization
physical execution plans
data quality validation
## 31. Production Considerations

For a production deployment, the following improvements would be considered:

Use a schema registry or explicit schemas instead of CSV inference.
Store raw data in object storage such as S3, ADLS, or GCS.
Use a data lakehouse format such as Delta Lake or Apache Iceberg.
Add structured logging and monitoring.
Add automated data-quality checks.
Add unit and integration tests.
Configure retry and failure handling.
Tune executor memory and cores.
Tune shuffle partitions based on workload.
Monitor data skew.
Use incremental processing for large datasets.
Add orchestration through Airflow, Databricks Workflows, or another scheduler.
Add CI/CD validation.
Maintain data lineage and observability.
## 32. Conclusion

This project demonstrates a complete Spark-based e-commerce ETL pipeline from raw ingestion to analytical outputs.

The project intentionally combines:

RDD
  |
Pair RDD
  |
Dataset / DataFrame
  |
Broadcast Join
  |
Persistence
  |
Window Functions
  |
Aggregations
  |
Partitioned Parquet

The implementation also analyzes Spark's execution behavior by examining shuffle boundaries, stages, physical plans, caching, joins, and partitioning decisions.

The result is a compact but production-oriented example of how Spark can be used to build an end-to-end batch data engineering pipeline.

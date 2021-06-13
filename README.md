# Azure CosmosDB : Data Modeling and Migration

Azure Cosmos DB is a schema-free databases and this feature makes Cosmos DB highly preferable to store and query unstructured and semi-structured data, but it requires deep understanding about application data model to get the most of the service in terms of performance and scalability and lowest cost.

Before desgining the data modeling we need to understand:

* How is data going to be stored?
* Data access pattern. How is application going to retrieve and query data?
* Is application read-heavy, or write-heavy?

## Denormalization vs Normalization

When we start modeling data in Azure Cosmos DB we need to treat entities as self-contained items represented as JSON documents.

For comparison, let's first see how we might model data in a relational database. The following example shows how an Order might be stored in a relational database.

![OrderERDinRDBMS](https://github.com/shan-sharma/pyspark-azuresql-cosmos/blob/main/images-in-readme/OrderDetailERD.jpg)


When working with relational databases, the strategy is to normalize all your data. Normalizing your data typically involves taking an entity, such as an order, and breaking it down into discrete components. In the example above, an order can have multiple items detail records (within order), as well their associated product records. 

The guiding premise when normalizing data is to avoid storing redundant data on each record and rather refer to data. In this example, to read an order, with all their order items details and product information, It is required to use JOINS to effectively compose back (or denormalize) your data at run time.

Now let's go through a sample order which has 2 items in OrderDetails and product description of those items from Product table.

![NormalizedOrderData](https://github.com/shan-sharma/pyspark-azuresql-cosmos/blob/main/images-in-readme/RelationalTable.JPG)

Modifying an Order with their items details and shipment/ delivery information will require write operations across many individual tables.

Now let's take a look at how we would model the same data as a self-contained entity in Azure Cosmos DB.

![OrderEmbeddCosmos](https://github.com/shan-sharma/pyspark-azuresql-cosmos/blob/main/images-in-readme/CosmosDBOrderDetailEmbed.JPG)

Using the approach above we have denormalized the ordert record, by embedding all the information related to this order, such as their order item details and products, into a single JSON document. In addition, because we're not confined to a fixed schema we have the flexibility to do things like having order items details of different shapes entirely.

Retrieving a complete order record from the database is now a single read operation against a single container and for a single item. Updating a order record, with their order item details and products, is also a single write operation against a single item.

By denormalizing data, the application may need to issue fewer queries and updates to complete common operations.

## Embedding

When to embed
In general, use embedded data models when:

* There are contained relationships between entities.
* There are one-to-one or one-to-few relationships between entities.
  * Embedding order details with order would be a good practise as there would be limited number of items in an order.
  * Embedding order with customer would NOT be a good practise as there is no limit on number of orders a customer can place.
* There is embedded data that changes infrequently.
* There is embedded data that will not grow without bound.
* There is embedded data that is queried frequently together.


# pyspark-azuresql-cosmos
## Quick Demo
Let's have a quick demo of what we discussed recently.

Throughout this article we rely on Azure Databricks Runtime 8.3 (includes Apache Spark 3.1.1, Scala 2.12).

We would require connectors for Both Azure SQL and CosmosDB in order to connect these database from Azure Databricks. Following connectors needs to be installed through Maven. Connectors can be imported using the coordinates below:


| Connector | Maven Coordinate |
| --------- | ------------------ |
|Azure SQLServer | `com.microsoft.azure:spark-mssql-connector_2.12_3.0:1.0.0-alpha` |
|Azure Cosmos DB | `com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.1.0` |


Open cluster --> Liberaries --> Install New --> Maven (Liberary Source) --> Search Package (Maven Central) --> Coordinates


## Get Started

The Apache Spark Connector for SQL Server and Azure SQL is based on the Spark DataSourceV1 API and SQL Server Bulk API and uses the same interface as the built-in JDBC Spark-SQL connector. This allows you to easily integrate the connector and migrate your existing Spark jobs by simply updating the format parameter with `com.microsoft.sqlserver.jdbc.spark`.


### Read from Azure SQL Table
```
#Connection Parameters
server_name = "jdbc:sqlserver://<your-server-name>.database.windows.net" #Add <your-server-name>
database_name = "AdventureWorks2017"
url = server_name + ";" + "databaseName=" + database_name + ";"

username = "<your-user-name>" # Add <your-user-name>
password = "<your-password>" # Add <your-password>
```
You can either provide table name or custom query to fetch data from SQL DB. 

```
#table_name = "dbo.Product"

table_name = """
(
SELECT  so.SalesOrderId AS id 
       ,so.CustomerId   AS CustomerId 
       ,so.OrderDate    AS OrderDate 
       ,ShipDate        AS ShipDate 
       ,(
SELECT  sod.SalesOrderDetailId AS SalesOrderDetailId 
       ,p.ProductNumber        AS Sku 
       ,P.Name                 AS Name 
       ,sod.UnitPrice          AS Price 
       ,sod.OrderQty           AS Quantity
FROM Sales.SalesOrderDetail sod
INNER JOIN Production.Product p
ON p.ProductID = sod.ProductID
WHERE so.SalesOrderId = sod.SalesOrderId for json auto) AS OrderDetails 
FROM Sales.SalesOrderHeader so
) AS query"""
```

```python
jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()
```

### Data wrangling and embedd OrderDetails with Order

Define schema for OrderDetails nested JSON column

```from pyspark.sql.types import *

orderDetailsSchema = ArrayType(StructType([
    StructField("SalesOrderDetailId",StringType()),
    StructField("Sku",StringType()),
    StructField("Name",StringType()),
    StructField("Price",DoubleType()),
    StructField("Quantity",IntegerType())
]))
```
Embedd schema (orderDetailsSchema) to OrderDetails array column

```from pyspark.sql import functions as F

writeToCosmosDF = jdbcDF.select(
  F.col("id").cast("string"),
  F.col("CustomerId").cast("string"),
  F.col("OrderDate").cast("timestamp"),
  F.from_json(F.col("OrderDetails"), orderDetailsSchema).alias("OrderDetails")
)
```
### Loading dataframe to CosmosDB

```
#Cosmos DB Connection Parameters

cosmosEndpoint = "https://<your-cosmos-server-name>.documents.azure.com:443/" #Add <your-cosmos-server-name>
cosmosMasterKey = "" #Add PrimaryKey for authorization
cosmosDatabaseName = "AnalyticsStore"
cosmosContainerName = "OrderDetailEmbedd"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

```

Configure Catalog API
```
# Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cosmosEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cosmosMasterKey)

# create a cosmos database using catalog api
spark.sql("CREATE DATABASE IF NOT EXISTS cosmosCatalog.{};".format(cosmosDatabaseName))

# create a cosmos container using catalog api
spark.sql("CREATE TABLE IF NOT EXISTS cosmosCatalog.{}.{} using cosmos.oltp TBLPROPERTIES(partitionKeyPath = '/id', manualThroughput = '1100')".format(cosmosDatabaseName, cosmosContainerName))
```

Write spark dataframe to Cosmos DB container
```
#Write to CosmosDB 
(writeToCosmosDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**writeConfig)
  .save())
  ```

# Azure CosmosDB : Data Modeling and Migration
# pyspark-azuresql-cosmos

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

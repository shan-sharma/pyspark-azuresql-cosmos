# pyspark-azuresql-cosmos

There are two versions of the connector available through Maven, a 2.4.5 compatible version and a 3.0.0 compatible version. Both versions can be found here and can be imported using the coordinates below:


| Connector | Maven Coordinate |
| --------- | ------------------ |
|MS SQLServer - Spark 2.4.5 compatible connnector | `com.microsoft.azure:spark-mssql-connector:1.0.1` |
|MS Azure Cosmos DB - Spark 2.4.5 compatible connnector | `com.microsoft.azure:azure-cosmosdb-spark_2.4.0_2.11:3.6.13` |

Connected needs to be added to cluster (Azure Databricks):

Open cluster --> Liberaries --> Install New --> Maven (Liberary Source) --> Coordinates


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
	       ,so.CustomerId AS CustomerId
	       ,so.OrderDate AS OrderDate
	       ,(
	SELECT  sod.SalesOrderDetailId AS SalesOrderDetailId
	       ,sod.Sku AS Sku
	       ,sod.Name AS Name
	       ,sod.Price AS Price
	       ,sod.Quantity AS Quantity
	FROM SalesOrderDetail sod
	WHERE so.SalesOrderId = sod.SalesOrderId for json auto) AS OrderDetails 
	FROM SalesOrder so
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
### Loading dataframe to Cosmos DB

```
#Cosmos DB Connection Parameters

URI = "https://<your-cosmos-server-name>.documents.azure.com:443/" Add #<your-cosmos-server-name>
PrimaryKey = "" #Add PrimaryKey for authorization
CosmosDatabase = "<your-cosmos-db-name." # Add <your-cosmos-db-name>
CosmosCollection = "<your-container-name>" #Add <your-container-name>
writeConfig = {
  "Endpoint": URI,
  "Masterkey": PrimaryKey,
  "Database": CosmosDatabase,
  "Collection": CosmosCollection,
  "writingBatchSize":"1000",
  "Upsert": "true"
}
```
Write spark dataframe to Cosmos DB container
```
#Cosmos DB 
(writeToCosmosDF.write
  .mode("overwrite")
  .format("com.microsoft.azure.cosmosdb.spark")
  .options(**writeConfig)
  .save())
  ```

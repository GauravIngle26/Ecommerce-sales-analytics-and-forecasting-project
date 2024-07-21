# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## # **_- Walmart sales forecast data transformation in Azure Databricks using pyspark:_**
# MAGIC
# MAGIC In this part of the project the data transformation process in performed using pyspark. Handling of NaN / null values along with rows and column repetitions is performed. In the end one comprehensive dataframe named as "dataset" is stored in the container of Azure data lake storage.

# COMMAND ----------

# Configure the storage account
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "baa10826-c406-453a-9ce5-7d4cf3340de8",
  "fs.azure.account.oauth2.client.secret": "dPA8Q~q0pttTvUuBUJwfYNo96x3zQafdmM7PBcfR",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/25a72ede-1750-4016-b257-1edaee97756d/oauth2/token"
}

# # Provide a mount for the storage container
# dbutils.fs.mount(
#   source="abfss://walmart-sales-data-container@walmartsalesdata.dfs.core.windows.net",
#   mount_point="/mnt/walamrt-data-transform-mount",
#   extra_configs=configs
# )



# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/walamrt-data-transform-mount"

# COMMAND ----------

#read the features and stores data as a Spark DataFrame.

features = spark.read.format("csv").option("header", "true").load("/mnt/walamrt-data-transform-mount/Raw data/features.csv", header=True)

stores = spark.read.format("csv").option("header", "true").load("/mnt/walamrt-data-transform-mount/Raw data/stores.csv", header=True)

# read train and test data set.

train = spark.read.format("csv").option("header", "true").load("/mnt/walamrt-data-transform-mount/Raw data/train.csv", header=True)

test = spark.read.format("csv").option("header", "true").load("/mnt/walamrt-data-transform-mount/Raw data/test.csv", header=True)


# COMMAND ----------

#Analyse the data 
#Display the schema and first few rows of the data.

features.printSchema()
features.show(5)

stores.printSchema()
stores.show(5) 

train.printSchema() 
train.show(5)

test.printSchema()
test.show(5)



# COMMAND ----------

#Transform the datatypes of the dataframes to appropriate datatypes:
from pyspark.sql.functions import col

#Transform the data types for features dataframe: (changing the IsHoliday name to IsHoliday_a for joining with features dataframe)
features = features.withColumn("Store", col("Store").cast("integer"))\
                    .withColumn("Date", col("Date").cast("date"))\
                    .withColumn("Temperature", col("Temperature").cast("float"))\
                    .withColumn("Fuel_Price", col("Fuel_Price").cast("float"))\
                    .withColumn("MarkDown1", col("MarkDown1").cast("float"))\
                    .withColumn("MarkDown2", col("MarkDown2").cast("float"))\
                    .withColumn("MarkDown3", col("MarkDown3").cast("float"))\
                    .withColumn("MarkDown4", col("MarkDown4").cast("float"))\
                    .withColumn("MarkDown5", col("MarkDown5").cast("float"))\
                    .withColumn("CPI", col("CPI").cast("float"))\
                    .withColumn("Unemployment", col("Unemployment").cast("float"))\
                    .withColumn("IsHoliday_a", col("IsHoliday").cast("boolean"))\
                    .drop("IsHoliday")        

#Transform the data types for stores dataframe:
stores = stores.withColumn("Store", col("Store").cast("integer"))\
                .withColumn("Size", col("Size").cast("integer"))

#Transform the data types for train dataframe: (changing the IsHoliday name to IsHoliday_b for joining with features dataframe)
train = train.withColumn("Store", col("Store").cast("integer"))\
             .withColumn("Dept", col("Dept").cast("integer"))\
             .withColumn("Date", col("Date").cast("date"))\
             .withColumn("IsHoliday_b", col("IsHoliday").cast("boolean"))\
             .withColumn("Weekly_Sales", col("Weekly_Sales").cast("float"))\
             .drop("IsHoliday") 

#Transform the data types for test dataframe:
test = test.withColumn("Store", col("Store").cast("integer"))\
             .withColumn("Dept", col("Dept").cast("integer"))\
             .withColumn("Date", col("Date").cast("date"))\
             .withColumn("IsHoliday", col("IsHoliday").cast("boolean"))


#Analyze the schema by printing the schema of the dataframes.
features.printSchema()
stores.printSchema()
train.printSchema()
test.printSchema()



# COMMAND ----------

#Create one comprehensive data set dataframe by joining the features, stores, and train dataframes. (Ignoring the test dataframe because of repetative data)

# Join train dataset with features
train_features = train.join(features, on=["Store", "Date"], how="left")

# Join the resulting DataFrame with stores
merged_df = train_features.join(stores, on=["Store"], how="left")

# Display the schema of the merged DataFrame to verify
merged_df.printSchema()

# Display the first few rows of the merged DataFrame to verify
merged_df.show(5)


# COMMAND ----------

#clean the merge dataset (Remove the replace the Null / NA values to 0 from columns)
dataset = merged_df.fillna(0)

#check and remove duplicate rows If any:
# Check for duplicate rows
total_count = dataset.count()
distinct_count = dataset.distinct().count()

if total_count == distinct_count:
    print("No duplicate rows found.")
else:
    print(f"Duplicate rows found: {total_count - distinct_count}")

# Optional: Show duplicate rows if any
if total_count != distinct_count:
    duplicate_rows_df = dataset.groupBy(dataset.columns).count().filter("count > 1")
    duplicate_rows_df.show()   

#In the columns of the dataset dataframe, remove the IsHoliday_a and chnage IsHoliday_b name to IsHoliday.
dataset = dataset.drop("IsHoliday_a")
dataset = dataset.withColumnRenamed("IsHoliday_b", "IsHoliday")

dataset.printSchema()
dataset.show(5)


# COMMAND ----------

# In this project the aim is to predict the sales. hence, Let's analyse the weekly sales of each store and each departments.

#count the sales by department:
distinct_departments_count = dataset.select("Dept").distinct().count()
print(f"Distinct number of departments: {distinct_departments_count}")

#count the stores:
distinct_stores_count = dataset.select("Store").distinct().count()
print(f"Distinct number of stores: {distinct_stores_count}")

#Import the required functions
from pyspark.sql.functions import mean
import pandas as pd

# Create a pivot table showing mean Weekly_Sales for stores and departments
pivot_table = dataset.groupBy("Store").pivot("Dept").agg(mean("Weekly_Sales").alias("Avg_Weekly_Sales"))

# Convert the pivot table to Pandas DataFrame for better visualization
pivot_table_pd = pivot_table.toPandas()

# Display the pivot table using Pandas
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.expand_frame_repr', False)  # Don't wrap the DataFrame in the output

print(pivot_table_pd)


# COMMAND ----------

#From the above analysis it is observed that the average weekly sales for some department and stores are in negative and NaN values, which is illogical. Hence we need to remove these rows.

# Filter the DataFrame for Weekly_Sales <= 0
negative_or_zero_sales_df = dataset.filter(dataset.Weekly_Sales <= 0)

# Count the number of rows with Weekly_Sales <= 0
negative_or_zero_sales_count = negative_or_zero_sales_df.count()

# Display the count for total rows having weekly_sales <= 0:
print(f"Number of rows with Weekly_Sales <= 0: {negative_or_zero_sales_count}")

# Display the count for total rows of dataset
print(f"Total number of data rows: {dataset.count()}")

# Display the percentage of the data rows out of total to be discarded:
print(f"The invalid data to be discarded amounts to {negative_or_zero_sales_count / dataset.count() * 100:.2f}%")

# Optionally, show the rows with Weekly_Sales <= 0 for further analysis
negative_or_zero_sales_df.show()



# COMMAND ----------

#Remove the invalid data rows from the dataset:
# Filter the DataFrame to remove rows with Weekly_Sales <= 0
dataset = dataset.filter(dataset.Weekly_Sales > 0)

# Count the number of rows and columns in the filtered DataFrame
row_count = dataset.count()
column_count = len(dataset.columns)

# Display the shape of the DataFrame
print(f"Shape of the DataFrame: ({row_count}, {column_count})")

# Optionally, show the first few rows of the filtered DataFrame for verification
dataset.show()

# COMMAND ----------

#Now that the process of cleaning and transforming the dataset is complete, we can persist the cleaned data to Azure data lake gen 2 in the form of csv:
# Specify the path to store the DataFrame in Azure Data Lake Storage Gen2
output_path = "/mnt/walamrt-data-transform-mount/Cleaned Data/"

# Write the DataFrame to the Data Lake as CSV files with headers
dataset.write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(output_path)

# Print a confirmation message
print("Data successfully written to Azure Data Lake Storage Gen2 with headers.")



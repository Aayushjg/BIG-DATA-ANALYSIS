!pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count

# Step 1: Start Spark Session
spark = SparkSession.builder.appName("NYC_Taxi_Trip_Analysis").getOrCreate()

# Step 2: Load the dataset
df = spark.read.csv("yellow_trip_dataset.csv", header=True, inferSchema=True)

# Step 3: Preview schema and data
df.printSchema()
df.show(5)

# Step 4: Drop rows with missing values
df_clean = df.dropna(subset=[
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount", "total_amount"
])

# Step 5: Total trip count
print("✅ Total Trips:", df_clean.count())

# Step 6: Passenger count distribution
print("✅ Trip Count by Passenger Count:")
df_clean.groupBy("passenger_count").count().orderBy("passenger_count").show()

# Step 7: Average trip distance by passenger count
print("✅ Average Trip Distance by Passenger Count:")
df_clean.groupBy("passenger_count").agg(avg("trip_distance").alias("avg_distance")).show()

# Step 8: Peak Pickup Hours
print("✅ Peak Pickup Hours:")
df_with_hour = df_clean.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
df_with_hour.groupBy("pickup_hour").agg(count("*").alias("trip_count")).orderBy("trip_count", ascending=False).show()

# Step 9: Stop Spark
spark.stop()

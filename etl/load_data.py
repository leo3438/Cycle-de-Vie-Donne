import sys
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, lit, current_timestamp, substring

# --- CONFIGURATION ---
# Adaptez ces variables selon votre environnement
DB_USER = "root"
DB_PASS = "root" 
DB_URL = "jdbc:mysql://localhost:3306/off_datamart?useSSL=false&allowPublicKeyRetrieval=true"
# Chemins locaux (à adapter si exécution hors du conteneur Proxmox)
JAR_PATH = "/root/projet-off/jars/mysql-connector-j-8.3.0.jar"
DATA_PATH = "/root/projet-off/data/sample.jsonl"
REPORT_PATH = "/root/projet-off/logs/run_report.json"

# 1. INIT SPARK
spark = SparkSession.builder \
    .appName("OFF_ETL_Final_Metric") \
    .config("spark.driver.extraClassPath", JAR_PATH) \
    .config("spark.jars", JAR_PATH) \
    .getOrCreate()

# 2. SCHÉMA EXPLICITE
off_schema = StructType([
    StructField("code", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("brands", StringType(), True),
    StructField("nutriscore_grade", StringType(), True),
    StructField("nutriments", StructType([
        StructField("energy-kcal_100g", FloatType(), True),
        StructField("sugars_100g", FloatType(), True),
        StructField("salt_100g", FloatType(), True),
        StructField("proteins_100g", FloatType(), True)
    ]))
])

# 3. LECTURE (Bronze)
df_raw = spark.read.schema(off_schema).json(DATA_PATH)
count_raw = df_raw.count()

# 4. TRANSFORMATION & QUALITÉ (Silver)
df_silver = df_raw.select(
    col("code"),
    col("product_name"),
    col("brands").alias("brand_name"),
    substring(col("nutriscore_grade"), 1, 50).alias("nutriscore_grade"),
    col("nutriments.energy-kcal_100g").alias("energy_kcal_100g"),
    col("nutriments.sugars_100g").alias("sugars_100g"),
    col("nutriments.salt_100g").alias("salt_100g"),
    col("nutriments.proteins_100g").alias("proteins_100g")
).dropna(subset=["code"]) \
 .filter((col("sugars_100g") <= 100) & (col("sugars_100g") >= 0)) \
 .filter((col("salt_100g") <= 100) & (col("salt_100g") >= 0))

count_silver = df_silver.count()
count_rejected = count_raw - count_silver

# 5. CHARGEMENT (Gold)
if count_silver > 0:
    # Dim Product
    df_silver.select("code", "product_name", "brand_name", "nutriscore_grade") \
        .withColumn("effective_from", current_timestamp()) \
        .withColumn("effective_to", lit("2099-12-31 00:00:00").cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .write.format("jdbc").option("url", DB_URL).option("dbtable", "dim_product") \
        .option("user", DB_USER).option("password", DB_PASS).option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append").save()

    # Fact Nutrition
    df_silver.select(
        col("code").alias("product_code"),
        "energy_kcal_100g", "sugars_100g", "salt_100g", "proteins_100g"
    ).withColumn("imported_at", current_timestamp()) \
     .write.format("jdbc").option("url", DB_URL).option("dbtable", "fact_nutrition") \
     .option("user", DB_USER).option("password", DB_PASS).option("driver", "com.mysql.cj.jdbc.Driver") \
     .mode("append").save()

# 6. RAPPORT JSON
metrics = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "rows_read": count_raw,
    "rows_processed": count_silver,
    "rows_rejected": count_rejected,
    "quality_rules": ["sugars <= 100", "salt <= 100", "code not null"]
}

with open(REPORT_PATH, 'w') as f:
    json.dump(metrics, f, indent=4)

spark.stop()
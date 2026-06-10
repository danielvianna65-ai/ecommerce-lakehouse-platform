# =====================================================
# IMPORTS
# =====================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import sys

# =====================================================
# Config
# =====================================================
table = "clientes_enrichment"

# =====================================================
# Paths
# =====================================================
source_path = f"/data/reference/{table}.csv"
landing_path = f"/data/01_landing/ecommerce/{table}"

# =====================================================
# Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("landing_clientes_enrichment")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .getOrCreate()
)

# =====================================================
# BOOTSTRAP CHECK (executa apenas na primeira carga)
# =====================================================
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark._jsc.hadoopConfiguration()
)

path = spark._jvm.org.apache.hadoop.fs.Path(landing_path)

if fs.exists(path):
    print(
        f"[LANDING][{table}] Dataset já existe em {landing_path}. Encerrando."
    )
    spark.stop()
    sys.exit(0)

print(
    f"[LANDING][{table}] Primeira execução. Iniciando carga."
)

# =====================================================
# READ
# =====================================================
print(f"[LANDING][{table}] Source path: {source_path}")
print(f"[LANDING][{table}] Target path: {landing_path}")

reference_extract_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .csv(source_path)
)

# =====================================================
# PARTITION BY DT
# =====================================================
partitioned_reference_df = reference_extract_df.withColumn("dt", current_date())

# =====================================================
# WRITE
# =====================================================
print(f"[LANDING][{table}] Gravando")
(
    partitioned_reference_df.write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet(landing_path)
)

spark.stop()
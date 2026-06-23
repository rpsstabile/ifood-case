# Databricks notebook source
TABLE_NAME = "main.ifood.process_control"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
(
    source_system STRING,
    file_name STRING,
    file_path STRING,
    processed_timestamp TIMESTAMP,
    layer STRING
)
USING DELTA
""")

# COMMAND ----------

# ============================================================
# SILVER LAYER - GREEN TAXI
# ============================================================
#
# Responsabilidades:
#
# 1. Leitura incremental da Bronze
# 2. Controle de arquivos processados
# 3. Padronização de schema
# 4. Data Quality
# 5. Quarantine
# 6. Deduplicação
# 7. Enriquecimento
# 8. Escrita Delta
#
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime
from functools import reduce
from pyspark.sql import functions as F
import re


# ============================================================
# CONFIGURAÇÕES
# ============================================================

SOURCE_SYSTEM = "green_taxi"

BRONZE_PATH = (
    "/Volumes/main/ifood/ifood_case/bronze/green_taxi"
)

SILVER_PATH = (
    "/Volumes/main/ifood/ifood_case/silver/green_taxi"
)

QUARANTINE_PATH = (
    "/Volumes/main/ifood/ifood_case/quarantine/green_taxi"
)

PROCESS_CONTROL_TABLE = (
    "main.ifood.process_control"
)

# ============================================================
# FUNÇÃO PARA LISTAR ARQUIVOS PARQUET
# ============================================================

def list_parquet_files(path):

    files = []

    for item in dbutils.fs.ls(path):

        if item.isDir():

            files.extend(
                list_parquet_files(item.path)
            )

        elif item.path.endswith(".parquet"):

            files.append(item.path)

    return files


# ============================================================
# DESCOBRE ARQUIVOS DA BRONZE
# ============================================================

bronze_files = list_parquet_files(BRONZE_PATH)

print("=" * 80)
print("ARQUIVOS ENCONTRADOS NA BRONZE")
print("=" * 80)

for file in bronze_files:
    print(file)

print(f"\nTotal: {len(bronze_files)}")


# ============================================================
# RECUPERA ARQUIVOS JÁ PROCESSADOS
# ============================================================

processed_files = set(
    row["file_path"]
    for row in (
        spark.table(PROCESS_CONTROL_TABLE)
        .filter(F.col("source_system") == SOURCE_SYSTEM)
        .filter(F.col("layer") == "silver")
        .select("file_path")
        .collect()
    )
)

processed_files = set(processed_files)


# ============================================================
# IDENTIFICA NOVOS ARQUIVOS
# ============================================================

new_files = [
    file
    for file in bronze_files
    if file not in processed_files
]

print("\n" + "=" * 80)
print("ARQUIVOS NOVOS PARA PROCESSAMENTO")
print("=" * 80)

for file in new_files:
    print(file)

print(f"\nTotal: {len(new_files)}")


# ============================================================
# ENCERRA CASO NÃO EXISTAM NOVOS ARQUIVOS
# ============================================================

if len(new_files) == 0:

    print("\nNenhum novo arquivo encontrado.")

    dbutils.notebook.exit(
        "NO_NEW_FILES_TO_PROCESS"
    )


# ============================================================
# LEITURA INCREMENTAL DA BRONZE
# ============================================================
dfs = []

for file in new_files:

    print(f"Lendo arquivo: {file}")

    # Extrai ano e mês da pasta
    # Exemplo:
    # /bronze/green_taxi/2023-04/green_tripdata_2023-04.parquet

    match = re.search(r"(\d{4})-(\d{2})", file)

    expected_year = int(match.group(1))
    expected_month = int(match.group(2))

    df = spark.read.parquet(file)

    df = (
        df
        .withColumn(
            "VendorID",
            F.col("VendorID").cast("int")
        )
        .withColumn(
            "passenger_count",
            F.col("passenger_count").cast("double")
        )
        .withColumn(
            "total_amount",
            F.col("total_amount").cast("double")
        )

        # Metadados da partição de origem
        .withColumn(
            "expected_year",
            F.lit(expected_year)
        )
        .withColumn(
            "expected_month",
            F.lit(expected_month)
        )
    )

    dfs.append(df)

df_bronze = reduce(
    lambda d1, d2:
        d1.unionByName(
            d2,
            allowMissingColumns=True
        ),
    dfs
)

bronze_count = df_bronze.count()

print("\n" + "=" * 80)
print("REGISTROS LIDOS DA BRONZE")
print("=" * 80)

print(f"Total registros: {bronze_count:,}")


# ============================================================
# SELEÇÃO DAS COLUNAS NECESSÁRIAS
# ============================================================

df = df_bronze.select(
    "VendorID",
    "passenger_count",
    "total_amount",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "expected_year",
    "expected_month"
)


# ============================================================
# PADRONIZAÇÃO DE TIPOS
# ============================================================

df = (
    df
    .withColumn(
        "VendorID",
        F.col("VendorID").cast("int")
    )
    .withColumn(
        "passenger_count",
        F.col("passenger_count").cast("int")
    )
    .withColumn(
        "total_amount",
        F.round(F.col("total_amount"), 2)
    )
)


# ============================================================
# DATA QUALITY
# ============================================================

valid_condition = (
    F.col("VendorID").isNotNull()
    & F.col("passenger_count").isNotNull()
    & F.col("total_amount").isNotNull()
    & F.col("lpep_pickup_datetime").isNotNull()
    & F.col("lpep_dropoff_datetime").isNotNull()

    & (
        F.col("lpep_pickup_datetime")
        <
        F.col("lpep_dropoff_datetime")
    )

    & (F.col("passenger_count") >= 0)

    & (F.col("total_amount") >= 0)

    # =====================================================
    # Validação dinâmica da partição
    # =====================================================
    & (
        F.year("lpep_pickup_datetime")
        == F.col("expected_year")
    )

    & (
        F.month("lpep_pickup_datetime")
        == F.col("expected_month")
    )
)

df = (
    df
    .withColumn(
        "error_reason",
        F.when(
            F.year("lpep_pickup_datetime")
            != F.col("expected_year"),
            "INVALID_PICKUP_YEAR"
        )
        .when(
            F.month("lpep_pickup_datetime")
            != F.col("expected_month"),
            "INVALID_PICKUP_MONTH"
        )
    )
)

# ============================================================
# QUARANTINE
# ============================================================

df_quarantine = df.filter(~valid_condition)

quarantine_count = df_quarantine.count()

(
    df_quarantine
    .write
    .format("delta")
    .mode("append")
    .save(QUARANTINE_PATH)
)

# ============================================================
# DADOS VÁLIDOS
# ============================================================

df_valid = df.filter(valid_condition)


# ============================================================
# DEDUPLICAÇÃO
# ============================================================

before_dedup = df_valid.count()

df_valid = df_valid.dropDuplicates(
    [
        "VendorID",
        "passenger_count",
        "total_amount",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime"
    ]
)

after_dedup = df_valid.count()

duplicates_removed = (
    before_dedup - after_dedup
)


# ============================================================
# ENRIQUECIMENTO
# ============================================================

df_silver = (
    df_valid

    .withColumnRenamed(
        "lpep_pickup_datetime",
        "pickup_datetime"
    )

    .withColumnRenamed(
        "lpep_dropoff_datetime",
        "dropoff_datetime"
    )

    .withColumn(
        "pickup_year",
        F.year("pickup_datetime")
    )

    .withColumn(
        "pickup_month",
        F.month("pickup_datetime")
    )

    .withColumn(
        "pickup_day",
        F.dayofmonth("pickup_datetime")
    )

    .withColumn(
        "pickup_hour",
        F.hour("pickup_datetime")
    )
)


# ============================================================
# ESCRITA DA SILVER
# ============================================================

(
    df_silver
    .write
    .format("delta")
    .mode("append")
    .partitionBy(
        "pickup_year",
        "pickup_month"
    )
    .save(SILVER_PATH)
)


# ============================================================
# REGISTRA PROCESSAMENTO
# ============================================================

control_rows = []

for file_path in new_files:

    control_rows.append(
        Row(
            source_system=SOURCE_SYSTEM,
            file_name=file_path.split("/")[-1],
            file_path=file_path,
            processed_timestamp=datetime.now(),
            layer="silver"
        )
    )

control_df = spark.createDataFrame(
    control_rows
)

(
    control_df
    .write
    .mode("append")
    .format("delta")
    .saveAsTable(
        PROCESS_CONTROL_TABLE
    )
)


# ============================================================
# MÉTRICAS
# ============================================================

silver_count = df_silver.count()

print("\n" + "=" * 80)
print("MÉTRICAS DA EXECUÇÃO")
print("=" * 80)

print(
    f"Registros Bronze       : {bronze_count:,}"
)

print(
    f"Registros Quarantine   : {quarantine_count:,}"
)

print(
    f"Duplicidades removidas : {duplicates_removed:,}"
)

print(
    f"Registros Silver       : {silver_count:,}"
)

print(
    f"Arquivos processados   : {len(new_files)}"
)

print("=" * 80)


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS main.ifood.green_taxi_silver
USING DELTA
AS
SELECT *
FROM delta.`/Volumes/main/ifood/ifood_case/silver/green_taxi`
""")
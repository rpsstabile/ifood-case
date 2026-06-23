# Databricks notebook source
# ============================================================
# GOLD LAYER - MONTHLY REVENUE
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ============================================================
# CONFIGURAÇÕES
# ============================================================

SILVER_PATH = (
    "/Volumes/main/ifood/ifood_case/silver/yellow_taxi"
)

GOLD_PATH = (
    "/Volumes/main/ifood/ifood_case/gold/monthly_revenue"
)

# ============================================================
# LEITURA DA SILVER
# ============================================================

df_silver = spark.table(
    "main.ifood.yellow_taxi_silver"
)

# ============================================================
# MESES EXISTENTES NA SILVER
# ============================================================

silver_months = (
    df_silver
    .select(
        "pickup_year",
        "pickup_month"
    )
    .distinct()
)

# ============================================================
# CRIA GOLD CASO NÃO EXISTA
# ============================================================

gold_exists = DeltaTable.isDeltaTable(
    spark,
    GOLD_PATH
)

if not gold_exists:

    print("Criando tabela Gold...")

    empty_df = spark.createDataFrame(
        [],
        """
        pickup_year INT,
        pickup_month INT,
        total_trips BIGINT,
        avg_total_amount DOUBLE,
        sum_total_amount DOUBLE,
        processed_timestamp TIMESTAMP
        """
    )

    (
        empty_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

# ============================================================
# LEITURA DA GOLD E IDENTIFICAÇÃO DE MESES NÃO PROCESSADOS
# ============================================================

if gold_exists:
    df_gold_existing = (
        spark.read
        .format("delta")
        .load(GOLD_PATH)
    )
    
    gold_months = (
        df_gold_existing
        .select(
            "pickup_year",
            "pickup_month"
        )
        .distinct()
    )
    
    months_to_process = (
        silver_months
        .join(
            gold_months,
            on=[
                "pickup_year",
                "pickup_month"
            ],
            how="left_anti"
        )
    )
else:
    # Se a tabela não existe, todos os meses da silver devem ser processados
    months_to_process = silver_months

months_count = months_to_process.count()

print("=" * 80)
print("MESES A PROCESSAR")
print("=" * 80)

months_to_process.show()

if months_count == 0:

    print("Nenhum novo mês para processar")

    dbutils.notebook.exit(
        "NO_NEW_MONTHS"
    )

# ============================================================
# FILTRA APENAS MESES NOVOS
# ============================================================

df_incremental = (
    df_silver
    .join(
        months_to_process,
        on=[
            "pickup_year",
            "pickup_month"
        ],
        how="inner"
    )
)

print("=" * 80)
print("REGISTROS PARA PROCESSAMENTO")
print("=" * 80)

print(df_incremental.count())

# ============================================================
# AGREGAÇÃO DE NEGÓCIO
# ============================================================

df_gold = (
    df_incremental
    .groupBy(
        "pickup_year",
        "pickup_month"
    )
    .agg(
        F.count("*")
            .alias("total_trips"),

        F.round(
            F.avg("total_amount"),
            2
        ).alias(
            "avg_total_amount"
        ),

        F.round(
            F.sum("total_amount"),
            2
        ).alias(
            "sum_total_amount"
        )
    )

    .withColumn(
        "processed_timestamp",
        F.current_timestamp()
    )
)

print("Qtd registros Gold:")
print(df_gold.count())

# ============================================================
# ESCRITA GOLD
# ============================================================

(
    df_gold
    .write
    .format("delta")
    .mode("append")
    .save(GOLD_PATH)
)

# ============================================================
# VALIDAÇÃO
# ============================================================

print("=" * 80)
print("RESULTADO PROCESSADO")
print("=" * 80)

print("=" * 80)
print(f"Meses processados: {months_count}")
print("=" * 80)

# COMMAND ----------

TABLE_NAME = "main.ifood.monthly_revenue"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
USING DELTA
AS
SELECT *
FROM delta.`/Volumes/main/ifood/ifood_case/gold/monthly_revenue`
""")
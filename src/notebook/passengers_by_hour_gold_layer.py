# Databricks notebook source
# ============================================================
# GOLD LAYER - PASSENGERS BY HOUR
# ============================================================
#
# Objetivo:
#
# Responder:
#
# "Qual a média de passageiros por cada hora do dia
# que pegaram táxi no mês de maio considerando todos
# os táxis da frota?"
#
# Fontes:
# - main.ifood.yellow_taxi_silver
# - main.ifood.green_taxi_silver
#
# Incremental:
# - Processa apenas meses ainda não existentes na Gold
#
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# CONFIGURAÇÕES
# ============================================================

YELLOW_SILVER_TABLE = (
    "main.ifood.yellow_taxi_silver"
)

GREEN_SILVER_TABLE = (
    "main.ifood.green_taxi_silver"
)

GOLD_PATH = (
    "/Volumes/main/ifood/ifood_case/gold/passengers_by_hour"
)


# ============================================================
# CRIA TABELA GOLD
# ============================================================


# ============================================================
# LEITURA SILVER
# ============================================================

df_yellow = spark.table(
    YELLOW_SILVER_TABLE
)

df_green = spark.table(
    GREEN_SILVER_TABLE
)

# ============================================================
# UNIÃO DAS FROTAS
# ============================================================

df_fleet = (
    df_yellow
    .select(
        "pickup_year",
        "pickup_month",
        "pickup_hour",
        "passenger_count"
    )
    .unionByName(
        df_green.select(
            "pickup_year",
            "pickup_month",
            "pickup_hour",
            "passenger_count"
        )
    )
)

# ============================================================
# MESES EXISTENTES NAS SILVERS
# ============================================================

silver_months = (
    df_fleet
    .select(
        "pickup_year",
        "pickup_month"
    )
    .distinct()
)

# ============================================================
# MESES JÁ PROCESSADOS NA GOLD
# ============================================================

# Verifica se o path contém uma tabela Delta válida
try:
    # Tenta verificar se o Delta log existe
    dbutils.fs.ls(f"{GOLD_PATH}/_delta_log")
    
    # Se chegou aqui, o Delta log existe, podemos ler
    gold_months = (
        spark.read.format("delta").load(GOLD_PATH)
        .select(
            "pickup_year",
            "pickup_month"
        )
        .distinct()
    )
except Exception:
    # Se o path não existe ou não é Delta ainda (primeira execução), cria DataFrame vazio
    gold_months = spark.createDataFrame([], "pickup_year INT, pickup_month INT")

# ============================================================
# IDENTIFICA MESES PENDENTES
# ============================================================

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

print("=" * 80)
print("MESES A PROCESSAR")
print("=" * 80)

months_to_process.orderBy(
    "pickup_year",
    "pickup_month"
).show()

# ============================================================
# ENCERRA CASO NÃO HAJA NOVOS MESES
# ============================================================

if months_to_process.count() == 0:

    print(
        "Nenhum novo mês encontrado."
    )

    dbutils.notebook.exit(
        "NO_NEW_MONTHS"
    )

# ============================================================
# FILTRA SOMENTE DADOS NOVOS
# ============================================================

df_incremental = (
    df_fleet
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

print(
    f"{df_incremental.count():,}"
)

# ============================================================
# AGREGAÇÃO DE NEGÓCIO
# ============================================================

df_gold = (
    df_incremental
    .groupBy(
        "pickup_year",
        "pickup_month",
        "pickup_hour"
    )
    .agg(
        F.count("*")
            .alias("total_trips"),

        F.sum(
            "passenger_count"
        ).cast("bigint")
         .alias(
             "total_passengers"
         ),

        F.round(
            F.avg(
                "passenger_count"
            ),
            2
        ).alias(
            "avg_passenger_count"
        )
    )

    .withColumn(
        "processed_timestamp",
        F.current_timestamp()
    )
)

# ============================================================
# VALIDAÇÃO PRÉ-GRAVAÇÃO
# ============================================================

print("=" * 80)
print("REGISTROS GERADOS")
print("=" * 80)

print(df_gold.count())

display(
    df_gold.orderBy(
        "pickup_year",
        "pickup_month",
        "pickup_hour"
    )
)

# ============================================================
# ESCRITA GOLD
# ============================================================

(
    df_gold
    .write
    .format("delta")
    .mode("append")
    .save(
        GOLD_PATH
    )
)

# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

print("=" * 80)
print("RESULTADO FINAL")
print("=" * 80)

display(
    spark.read.format("delta").load(GOLD_PATH)
    .orderBy(
        "pickup_year",
        "pickup_month",
        "pickup_hour"
    )
)

print("=" * 80)
print(
    f"Meses processados: {months_to_process.count()}"
)
print("=" * 80)

# COMMAND ----------

# ============================================================
# CRIA TABELA GOLD SE NÃO EXISTIR
# ============================================================
TABLE_NAME = "main.ifood.passengers_by_hour"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
USING DELTA
AS
SELECT *
FROM delta.`/Volumes/main/ifood/ifood_case/gold/passengers_by_hour`
""")
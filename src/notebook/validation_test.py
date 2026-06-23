# Databricks notebook source
from pyspark.sql import functions as F

print("=" * 80)
print("PIPELINE VALIDATION TESTS")
print("=" * 80)

tests = []

# ============================================================
# TESTE 1
# MONTHLY REVENUE POSSUI REGISTROS
# ============================================================

monthly_count = (
    spark.table(
        "main.ifood.monthly_revenue"
    ).count()
)

result = monthly_count > 0

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"Monthly Revenue possui registros "
    f"({monthly_count:,})"
)

# ============================================================
# TESTE 2
# PASSENGERS BY HOUR POSSUI REGISTROS
# ============================================================

passengers_count = (
    spark.table(
        "main.ifood.passengers_by_hour"
    ).count()
)

result = passengers_count > 0

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"Passengers By Hour possui registros "
    f"({passengers_count:,})"
)

# ============================================================
# TESTE 3
# NÃO EXISTE MÉDIA NEGATIVA DE RECEITA
# ============================================================

negative_revenue = (
    spark.table(
        "main.ifood.monthly_revenue"
    )
    .filter(
        F.col("avg_total_amount") < 0
    )
    .count()
)

result = negative_revenue == 0

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"Average Revenue Negativa "
    f"({negative_revenue} registros)"
)

# ============================================================
# TESTE 4
# NÃO EXISTE MÉDIA NEGATIVA DE PASSAGEIROS
# ============================================================

negative_passengers = (
    spark.table(
        "main.ifood.passengers_by_hour"
    )
    .filter(
        F.col("avg_passenger_count") < 0
    )
    .count()
)

result = negative_passengers == 0

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"Average Passenger Count Negativo "
    f"({negative_passengers} registros)"
)

# ============================================================
# TESTE 5
# MONTHLY REVENUE TEM TODOS OS MESES
# ============================================================

months = (
    spark.table(
        "main.ifood.monthly_revenue"
    )
    .select(
        "pickup_year",
        "pickup_month"
    )
    .distinct()
    .count()
)

result = months == 5

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"Quantidade de meses esperada "
    f"({months})"
)

# ============================================================
# TESTE 6
# PASSENGERS BY HOUR POSSUI 24 HORAS PARA MAIO
# ============================================================

hours_may = (
    spark.table(
        "main.ifood.passengers_by_hour"
    )
    .filter(
        (F.col("pickup_year") == 2023)
        &
        (F.col("pickup_month") == 5)
    )
    .select(
        "pickup_hour"
    )
    .distinct()
    .count()
)

result = hours_may == 24

tests.append(result)

print(
    f"[{'PASS' if result else 'FAIL'}] "
    f"24 horas disponíveis para Maio/2023 "
    f"({hours_may})"
)

# ============================================================
# RESUMO
# ============================================================

passed = sum(tests)
failed = len(tests) - passed

print("\n" + "=" * 80)
print("TEST SUMMARY")
print("=" * 80)

print(f"Total Tests : {len(tests)}")
print(f"Passed      : {passed}")
print(f"Failed      : {failed}")

if failed == 0:

    print("\nSUCCESS - ALL TESTS PASSED")

else:

    print("\nFAILED - SOME TESTS FAILED")

    raise Exception(
        f"{failed} tests failed"
    )
# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS main;
# MAGIC CREATE SCHEMA IF NOT EXISTS main.ifood;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS main.ifood.ifood_case;

# COMMAND ----------

# Volume base
volume_root = "/Volumes/main/ifood/ifood_case"

folders = [
    f"{volume_root}/bronze/yellow_taxi/2023-01",
    f"{volume_root}/bronze/yellow_taxi/2023-02",
    f"{volume_root}/bronze/yellow_taxi/2023-03",
    f"{volume_root}/bronze/yellow_taxi/2023-04",
    f"{volume_root}/bronze/yellow_taxi/2023-05",

    f"{volume_root}/bronze/green_taxi/2023-01",
    f"{volume_root}/bronze/green_taxi/2023-02",
    f"{volume_root}/bronze/green_taxi/2023-03",
    f"{volume_root}/bronze/green_taxi/2023-04",
    f"{volume_root}/bronze/green_taxi/2023-05",

    f"{volume_root}/bronze/forhire_taxi/2023-01",
    f"{volume_root}/bronze/forhire_taxi/2023-02",
    f"{volume_root}/bronze/forhire_taxi/2023-03",
    f"{volume_root}/bronze/forhire_taxi/2023-04",
    f"{volume_root}/bronze/forhire_taxi/2023-05",

    f"{volume_root}/bronze/highvolume_forhire_taxi/2023-01",
    f"{volume_root}/bronze/highvolume_forhire_taxi/2023-02",
    f"{volume_root}/bronze/highvolume_forhire_taxi/2023-03",
    f"{volume_root}/bronze/highvolume_forhire_taxi/2023-04",
    f"{volume_root}/bronze/highvolume_forhire_taxi/2023-05",

    f"{volume_root}/silver/yellow_taxi",
    f"{volume_root}/silver/green_taxi",
    f"{volume_root}/silver/forhire_taxi",
    f"{volume_root}/silver/highvolume_forhire_taxi",

    f"{volume_root}/quarantine/yellow_taxi",
    f"{volume_root}/quarantine/green_taxi",
    f"{volume_root}/quarantine/green_taxi",
    f"{volume_root}/quarantine/forhire_taxi",
    f"{volume_root}/quarantine/highvolume_forhire_taxi",


    f"{volume_root}/gold/monthly_revenue",
    f"{volume_root}/gold/passengers_by_hour"
]

created = []
already_exists = []

for folder in folders:
    try:
        dbutils.fs.ls(folder)
        already_exists.append(folder)

    except Exception:
        dbutils.fs.mkdirs(folder)
        created.append(folder)

print("=" * 80)
print("DIRETÓRIOS CRIADOS")
print("=" * 80)

for folder in created:
    print(folder)

print(f"\nTotal criados: {len(created)}")

print("\n" + "=" * 80)
print("DIRETÓRIOS JÁ EXISTENTES")
print("=" * 80)

for folder in already_exists:
    print(folder)

print(f"\nTotal já existentes: {len(already_exists)}")

print("\n" + "=" * 80)
print("RESUMO FINAL")
print("=" * 80)
print(f"Total de diretórios configurados: {len(folders)}")
print(f"Criados nesta execução: {len(created)}")
print(f"Já existentes: {len(already_exists)}")

# COMMAND ----------

def list_recursive(path, level=0):
    try:
        items = dbutils.fs.ls(path)

        for item in items:
            print("  " * level + f"└── {item.name}")

            if item.isDir():
                list_recursive(item.path, level + 1)

    except Exception as e:
        print(f"Erro ao listar {path}: {e}")

print("=" * 80)
print("ESTRUTURA DO DATA LAKE")
print("=" * 80)

list_recursive("/Volumes/main/ifood/ifood_case/")
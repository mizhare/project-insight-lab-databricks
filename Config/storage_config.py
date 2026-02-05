from databricks.sdk.runtime import *

# 1. Definição de storages (input e output)
storage_input = "landingbeca2026jan"
storage_account = "storageinsightlab"

# Containers
container_bronze = "bronze"
container_silver = "silver"
container_gold = "gold"

# Paths
cnpj_path = f"abfss://cnpj@{storage_input}.dfs.core.windows.net"
balanca_comercial_path = f"abfss://balancacomercial@{storage_input}.dfs.core.windows.net"

# 2. Definição dos paths
bronze_path = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net/"
silver_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/"
gold_path = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net/"
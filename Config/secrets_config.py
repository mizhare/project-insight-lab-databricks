from databricks.sdk.runtime import *

# 1. Listar todos os scopes
dbutils.secrets.listScopes()

# 2. Definição do secret scope
SECRET_SCOPE = "databricks-scope"

# 3. Atribuição de keys storages
bronze_storage_key = dbutils.secrets.get(scope="databricks-scope", key="insightlab-storage-key")
input_storage_key = dbutils.secrets.get(scope="databricks-scope", key="input-storage-key")
